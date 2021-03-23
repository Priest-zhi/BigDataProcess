from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from phe import paillier
from pyspark import SparkConf, SparkContext
import json
import copy
from functools import partial
from hdfs3 import HDFileSystem
import configparser
import os

# fee_filepath= "hdfs://node1:9000/proj/bigdataprocess/data/fee.csv"
# HEfee_filepath= "hdfs://node1:9000/proj/bigdataprocess/data/HEfee.csv"
# pubkey_filepath='hdfs://node1:9000/proj/bigdataprocess/data/public_key.json'
# privatekey_filepath='hdfs://node1:9000/proj/bigdataprocess/data/private_key.json'

isDebug=""
fee_filepath=""
HEfee_filepath=""
pubkey_filepath=""
privatekey_filepath=""

def PreProcess():
    #read conf
    # get current code file addr
    currcodedir = os.path.dirname(os.path.realpath(__file__))
    MainConffile = currcodedir + os.sep + "project.config"
    cf = configparser.ConfigParser()
    cf.read(MainConffile)
    global isDebug, fee_filepath, HEfee_filepath, pubkey_filepath, privatekey_filepath
    isDebug = cf.get("backend", "Debug") == "True"
    if isDebug:
        fee_filepath=cf.get("backend", "filepath_local")+ os.sep +"data/fee.csv"
        HEfee_filepath= cf.get("backend", "filepath_local")+ os.sep +"data/HEfee.csv"
        pubkey_filepath=cf.get("backend", "filepath_local")+ os.sep +'data/public_key.json'
        privatekey_filepath=cf.get("backend", "filepath_local")+ os.sep +'data/private_key.json'
    else:
        fee_filepath=cf.get("backend", "filepath_hdfs")+ os.sep +"data/fee.csv"
        HEfee_filepath= cf.get("backend", "filepath_hdfs")+ os.sep +"data/HEfee.csv"
        pubkey_filepath=cf.get("backend", "filepath_hdfs")+ os.sep +'data/public_key.json'
        privatekey_filepath=cf.get("backend", "filepath_hdfs")+ os.sep +'data/private_key.json'

def saveFee(x):
    with open(HEfee_filepath, 'a') as f:
        print("save file")
        f.write(str(x.ciphertext())+","+str(x.exponent)+'\n')



def JsonSerialisation(res_rdd,public_key,private_key):
    #rdd
    if isDebug:
        with open(HEfee_filepath, 'w') as f:
            f.write("ciphertext"+","+"exponent"+"\n")
    else:
        hdfs = HDFileSystem(host='node1:9000', port=8020)
        with hdfs.open(HEfee_filepath,'w') as f:
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
    PreProcess()
    sparksn = SparkSession.builder.appName("fee_calc").getOrCreate()
    #sparksn = SparkSession.builder.master("local[*]").appName("fee_calc").getOrCreate()
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

    print("Congratulations, all done!")