from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from phe import paillier
from pyspark import SparkConf, SparkContext
import json
import copy
from functools import partial

fee_filepath= "data/fee.csv"
HEfee_filepath= "data/HEfee.csv"
pubkey_filepath='data/public_key.json'
privatekey_filepath='data/private_key.json'


def saveFee(x):
    with open(HEfee_filepath, 'a') as f:
        print("save file")
        f.write(str(x.ciphertext())+","+str(x.exponent)+'\n')



def JsonSerialisation(res_rdd,public_key,private_key):

    #rdd
    with open(HEfee_filepath, 'w') as f:
        f.write("ciphertext"+","+"exponent"+"\n")
    res_rdd.foreach(saveFee)
    #pub key
    enc_with_one_pub_key = {}
    enc_with_one_pub_key['public_key'] = {'n': public_key.n}
    with open(pubkey_filepath, 'w') as f:
        f.write(json.dumps(enc_with_one_pub_key))
    #private key
    enc_with_one_pri_key={}
    enc_with_one_pri_key['private_key'] = {'p': private_key.p,'q': private_key.q}
    with open(privatekey_filepath, 'w') as f:
        f.write(json.dumps(enc_with_one_pri_key))


if __name__ == '__main__':

    sparksn = SparkSession.builder.master("local[*]").appName("fee_calc").getOrCreate()
    print("app start")

    #generate public key and private key
    public_key, private_key = paillier.generate_paillier_keypair()

    # 创建SparkConf和SparkContext
    sc=sparksn.sparkContext
    fee_rdd =sc.textFile(fee_filepath)
    header = fee_rdd.first()
    # res_rdd = fee_rdd.filter(lambda hline: hline != header).map(lambda cline: cline.split(',')[1]).reduce(
    #     lambda a, b: int(a) + int(b))
    res_rdd = fee_rdd.filter(lambda hline: hline != header)\
        .map(lambda cline: cline.split(',')[1].strip())\
        .map(lambda x:public_key.encrypt(float(x)))
        # .reduce(lambda a,b:a+b)

    #save rdd results, public key and private key
    JsonSerialisation(res_rdd,public_key,private_key)

