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
    HEfilename = cf.get("standalone", "filename")
    columnname = cf.get("standalone", "columnname")
    fee_filepath=cf.get("standalone", "filepath_local")+ os.sep +HEfilename
    HEfee_filepath = cf.get("standalone", "filepath_local") + os.sep + "HEfee.csv"
    pubkey_filepath = cf.get("standalone", "filepath_local") + os.sep + 'public_key.json'
    privatekey_filepath = cf.get("standalone", "filepath_local") + os.sep + 'private_key.json'

def saveFee(listx):

    with open(HEfee_filepath, 'a') as f:
        for line in listx:
            for i, val in enumerate(line):
                if (i + 1) == len(line):
                    f.write(str(val.ciphertext()) + ";" + str(val.exponent) + '\n')
                else:
                    f.write(str(val.ciphertext()) + ";" + str(val.exponent) + ',')



def JsonSerialisation(public_key,private_key):
    #rdd
    with open(HEfee_filepath, 'w') as f:
        f.write(columnname + "\n")

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

    print("app start")

    #generate public key and private key
    public_key, private_key = paillier.generate_paillier_keypair()

    #save rdd results, public key and private key
    JsonSerialisation(public_key,private_key)

    line1k = []
    with open(fee_filepath, 'r') as f:
        head = columnname.strip().split(',')
        # ignore fist line
        f.readline()
        line = f.readline()
        while line:
            line_list = line[:-1].split(',')
            line1k.append([public_key.encrypt(float(ele)) for ele in line_list])

            global processline
            processline += 1
            if processline % 1000 == 0:
                saveFee(line1k)
                line1k=[]
            line = f.readline()

        saveFee(line1k)


def saveResult(costtime):
    with open("./myresult", 'a') as f:
        fstr="filename: {}, costtime: {} \n".format(HEfilename,costtime)
        f.write(fstr)

if __name__ == '__main__':
    time_start = time.time()
    main()
    time_end = time.time()
    print("Congratulations, all done!")
    print('totally cost', time_end - time_start)
    saveResult(time_end - time_start)