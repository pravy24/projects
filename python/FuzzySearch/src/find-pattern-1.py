# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 11:41:35 2017

@author: Praveen

patt = 'aabcdde'
data = [{1:'aabcdde'}, {2:'abcde'}, {3:'abbccdde'}, {4:'axabcdde'},{5:'aaxccde'}, {6:'zsdacge'}]
result = process.extract(patt, data)
print(result)

"""
from functools import reduce
from itertools import groupby
from fuzzywuzzy import process
import pandas as pd



def reduceByKey(f1, rec):

    output = map(lambda l: {l[0]: reduce(f1, map(lambda x: x[1], l[1]))}, \
                          groupby(sorted(rec, key=lambda x: x[0]), lambda x: x[0]))
    
    return(output)



if __name__=="__main__":
    
    df_pattern = pd.read_csv("../data/pattern.csv", header=None)
    df_logdata = pd.read_csv("../data/log-data.csv", header=None)
    
    pattern = reduce(lambda x, y: x + '/' + y, df_pattern[1])
    
    logdata = reduceByKey(lambda x, y: x + '/' + y, \
                         map(lambda x, y: (x, y), df_logdata[0], df_logdata[2]))
    
    result = process.extract(pattern, logdata, limit=100000000)
    
    print(result)