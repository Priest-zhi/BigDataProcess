import sys
from phe import paillier
import configparser
import os
import json

isLocal=""
fee_filepath=""
HEfee_filepath=""
pubkey_filepath=""
privatekey_filepath=""

def preProcess():
    currcodedir = os.path.dirname(os.path.realpath(__file__))
    MainConffile = currcodedir + os.sep + "project.config"
    cf = configparser.ConfigParser()
    cf.read(MainConffile)
    global isLocal, fee_filepath, HEfee_filepath, pubkey_filepath, privatekey_filepath
    isLocal = cf.get("MR", "isLocal") == "True"
    if isLocal:
        fee_filepath=cf.get("MR", "filepath_local")+ os.sep +"fee.csv"
        HEfee_filepath= cf.get("MR", "filepath_local")+ os.sep +"HEfee.csv"
        pubkey_filepath=cf.get("MR", "filepath_local")+ os.sep +'public_key.json'
        privatekey_filepath=cf.get("MR", "filepath_local")+ os.sep +'private_key.json'
    else:
        fee_filepath=cf.get("MR", "filepath_hdfs")+ os.sep +"fee.csv"
        HEfee_filepath=cf.get("MR", "filepath_hdfs")+ os.sep +"HEfee.csv"
        pubkey_filepath=cf.get("MR", "filepath_hdfs")+ os.sep +"public_key.json"
        privatekey_filepath=cf.get("MR", "filepath_hdfs")+ os.sep +'private_key.json'

def JsonSerialisation(public_key,private_key):

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


def mapper():
    preProcess()
    #generate public key and private key
    public_key, private_key = paillier.generate_paillier_keypair()
    JsonSerialisation(public_key, private_key)

    def processLine(set):
        for line in set:
            if line.startswith("name,fee"):
                continue
            fee_line = line.split(',')[1].strip()
            fee_encry = public_key.encrypt(float(fee_line))
            print(str(fee_encry.ciphertext())+","+str(fee_encry.exponent))

    if isLocal:
        with open(fee_filepath, 'r') as f:
            processLine(f)
    else:
        processLine(sys.stdin)


    print("All done!")

if __name__ == '__main__':
    mapper()