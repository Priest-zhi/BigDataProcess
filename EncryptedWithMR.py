import sys
from phe import paillier
import configparser
import os

isLocal=""
fee_filepath=""

def preProcess():
    currcodedir = os.path.dirname(os.path.realpath(__file__))
    MainConffile = currcodedir + os.sep + "project.config"
    cf = configparser.ConfigParser()
    cf.read(MainConffile)
    global isLocal,fee_filepath
    isLocal = cf.get("MR", "isLocal") == "True"
    if isLocal:
        fee_filepath=cf.get("MR", "filepath_local")+ os.sep +"fee.csv"

def mapper():
    preProcess()
    #generate public key and private key
    public_key, private_key = paillier.generate_paillier_keypair()

    def processLine(set):
        for line in set:
            if line.startswith("name,fee"):
                continue
            fee_line = line.split(',')[1].strip()
            fee_encry = public_key.encrypt(float(fee_line))
            print(fee_encry)

    if isLocal:
        with open(fee_filepath, 'r') as f:
            processLine(f)
    else:
        processLine(sys.stdin)

if __name__ == '__main__':
    mapper()