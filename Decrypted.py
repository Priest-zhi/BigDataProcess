from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from phe import paillier
from pyspark import SparkConf, SparkContext
import json
from functools import reduce
import configparser
import os

isLocal=""
columnname=""

def PreProcess():
    #read conf
    # get current code file addr
    currcodedir = os.path.dirname(os.path.realpath(__file__))
    MainConffile = currcodedir + os.sep + "project.config"
    cf = configparser.ConfigParser()
    cf.read(MainConffile)
    global columnname
    columnname = cf.get("spark", "columnname")

def GetRDDlist(pub_key, rddfile):
    hedatadict= {}
    with open(rddfile,'r') as f:
        head = columnname.strip().split(',')
        #ignore fist line
        f.readline()
        line = f.readline()
        while line:
            line_list = line[:-1].split(',')
            tmp_dict = dict(zip(head, line_list))
            for header in head:
                tmpdatalist=tmp_dict[header].split("\t")
                if header not in hedatadict:
                    hedatadict[header]=[paillier.EncryptedNumber(pub_key,int(tmpdatalist[0]),int(tmpdatalist[1]))]
                else:
                    hedatadict[header].append(paillier.EncryptedNumber(pub_key,int(tmpdatalist[0]),int(tmpdatalist[1])))

            line = f.readline()
    return hedatadict

if __name__ == '__main__':
    PreProcess()
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
    HeDataDict = GetRDDlist(public_key_rec,"data/HEfee.csv")
    for keylist in HeDataDict:
        HeDataList=HeDataDict[keylist]
        sum=reduce(lambda a,b:a+b,HeDataList)
        printstr=(keylist+':{}').format(private_key_rec.decrypt(sum))
        print(printstr)


