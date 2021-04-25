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
import time



isLocal=""
fee_filepath=""
HEfee_filepath=""
pubkey_filepath=""
privatekey_filepath=""
HEfilename=""
columnname=""
processline=0

def PreProcess():
    #read conf
    # get current code file addr
    currcodedir = os.path.dirname(os.path.realpath(__file__))
    MainConffile = currcodedir + os.sep + "project.config"
    cf = configparser.ConfigParser()
    cf.read(MainConffile)
    global isLocal, fee_filepath, HEfee_filepath, pubkey_filepath, privatekey_filepath,HEfilename,columnname
    HEfilename = cf.get("spark", "filename")
    columnname = cf.get("spark", "columnname")
    isLocal = cf.get("spark", "isLocal") == "True"
    if isLocal:
        #sparkContext using local file needs "file:///"
        fee_filepath="file://"+cf.get("spark", "filepath_local")+ os.sep +HEfilename
    else:
        fee_filepath="hdfs://"+cf.get("spark", "filepath_hdfs")+ os.sep +HEfilename
        # HEfee_filepath="hdfs://"+ cf.get("spark", "filepath_hdfs")+ os.sep +"HEfee.csv"
        # pubkey_filepath="hdfs://"+cf.get("spark", "filepath_hdfs")+ os.sep +"public_key.json"
        # privatekey_filepath="hdfs://"+cf.get("spark", "filepath_hdfs")+ os.sep +'private_key.json'
    # python create local file dont needs "file:///"
    HEfee_filepath = cf.get("spark", "filepath_local") + os.sep + "HEfee.csv"
    pubkey_filepath = cf.get("spark", "filepath_local") + os.sep + 'public_key.json'
    privatekey_filepath = cf.get("spark", "filepath_local") + os.sep + 'private_key.json'

def saveFee(listx):
    global processline
    processline+=1
    if processline % 1000 == 0:
        print("in func savefile, linenum: {}".format(processline))
    with open(HEfee_filepath, 'a') as f:
        for i, val in enumerate(listx):
            if (i + 1) == len(listx):
                f.write(str(val.ciphertext()) + "\t" + str(val.exponent) + '\n')
            else:
                f.write(str(val.ciphertext()) + "\t" + str(val.exponent) + ',')



def JsonSerialisation(res_rdd,public_key,private_key):
    #rdd
    with open(HEfee_filepath, 'w') as f:
        f.write(columnname + "\n")
    # if isLocal:
    #     with open(HEfee_filepath, 'w') as f:
    #         f.write(columnname+"\n")
    # else:
    #     # not test
    #     hdfs = HDFileSystem(host='node1:9000', port=8020)
    #     with hdfs.open(HEfee_filepath,'w') as f:
    #         f.write(columnname+"\n")

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

def main():
    PreProcess()
    sparksn = SparkSession.builder.appName("fee_calc").getOrCreate()
    print("app start")

    #generate public key and private key
    public_key, private_key = paillier.generate_paillier_keypair()

    # 创建SparkConf和SparkContext
    sc=sparksn.sparkContext
    fee_rdd =sc.textFile(fee_filepath)
    header = fee_rdd.first()
    res_rdd = fee_rdd.filter(lambda hline: hline != header)\
        .map(lambda cline: cline.strip().split(','))\
        .map(lambda listline: [public_key.encrypt(float(ele)) for ele in listline])

    #save rdd results, public key and private key
    JsonSerialisation(res_rdd,public_key,private_key)

def saveResult(costtime):
    with open("./myresult", 'a') as f:
        fstr="filename: {}, costtime: {}".format(HEfilename,costtime)
        f.write(fstr)

if __name__ == '__main__':
    time_start = time.time()
    main()
    time_end = time.time()
    print("Congratulations, all done!")
    print('totally cost', time_end - time_start)
    saveResult(time_end - time_start)