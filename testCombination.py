import re
import csv

#combine the userid and their interesting
#coding:utf-8
__author__ = 'Conley'

rawFile = open("F:/Daily Lab/data_version1.1/用户图/person.csv",'r')
#file2 = open("C:\\Users\\jsfbetter\\Desktop\\2015.12.30\\book_data\\bookfinal3_translation_del_double3.txt",'r')
outFile = open("F:/Daily Lab/data_version1.1/用户图/person.csv/result.txt",'a')

#将rawFile中的数据读入到content变量中
content = rawFile.readlines()
#translation = file2.readlines()
for i in range(10):
    str = content[i].strip("\n")
    #+ translation[i]
    outFile.write(str)
    if i % 100 == 0:
        print i
rawFile.close()
file2.close()
outFile.close()
