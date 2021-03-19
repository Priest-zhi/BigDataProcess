import json
import re
import os
import time
import sys
import os
import csv
from phe import paillier

def loadDataFromtxt(filepath):
    with open(filepath, 'r', encoding='UTF-8') as f:
        #line = f.readline()
        head = ['AT', 'V', 'AP', 'RH', 'PE']
        # print(len(head))
        line = f.readline()
        # line_list = line.split('\t')
        # print(len(line_list))

        while line:
            line_list = line[:-1].split(',')
            tmp_dict = dict(zip(head, line_list))



if __name__ == '__main__':
    public_key, private_key = paillier.generate_paillier_keypair()
    secret_number_list = [3.141592653, 300, -4.6e-12,25.98]
    encrypted_number_list = [public_key.encrypt(x) for x in secret_number_list]
    print(encrypted_number_list[3])
    print(encrypted_number_list[1])
    print(private_key.decrypt(encrypted_number_list[1]._EncryptedNumber__ciphertext+encrypted_number_list[1]._EncryptedNumber__ciphertext))
    print(private_key.decrypt(encrypted_number_list[3]+encrypted_number_list[1]))
    #print ([private_key.decrypt(x) for x in encrypted_number_list])







