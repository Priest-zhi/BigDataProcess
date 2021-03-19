from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from phe import paillier
from pyspark import SparkConf, SparkContext
import json
from functools import reduce

def decrypted():
    pass

def GetRDDlist(pub_key, rddfile):
    hedatalist=[]
    with open(rddfile,'r') as f:
        head = ['ciphertext', 'exponent']
        #ignore fist line
        f.readline()
        line = f.readline()
        while line:
            line_list = line[:-1].split(',')
            tmp_dict = dict(zip(head, line_list))
            hedatalist.append(paillier.EncryptedNumber(pub_key,int(tmp_dict['ciphertext']),int(tmp_dict['exponent'])))
            line = f.readline()
    return hedatalist

if __name__ == '__main__':
    # sparksn = SparkSession.builder.master("local[*]").appName("fee_calc").getOrCreate()
    # print("app start")
    # # create SparkConf and SparkContext
    # sc=sparksn.sparkContext


    #get public key
    with open('data/public_key.json', 'r') as f:
        received_dict = json.loads(f.read())
        pk = received_dict['public_key']
        public_key_rec = paillier.PaillierPublicKey(n=int(pk['n']))

    #get private key
    with open('data/private_key.json', 'r') as f:
        received_dict = json.loads(f.read())
        pk = received_dict['private_key']
        private_key_rec = paillier.PaillierPrivateKey(public_key_rec,p=int(pk['p']),q=int(pk['q']))

    #get rdd
    HeDataList = GetRDDlist(public_key_rec,"data/HEfee.csv")
    sum=reduce(lambda a,b:a+b,HeDataList)
    print(private_key_rec.decrypt(sum))


