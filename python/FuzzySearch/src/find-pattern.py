# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 11:41:35 2017

@author: root

patt = 'aabcdde'
data = [{1:'aabcdde'}, {2:'abcde'}, {3:'abbccdde'}, {4:'axabcdde'},{5:'aaxccde'}, {6:'zsdacge'}]
result = process.extract(patt, data)
print(result)

"""
import pandas as pd
from  fuzzywuzzy import fuzz
from  fuzzywuzzy import process

df_pattern = pd.read_csv("../data/pattern.csv", header=None)
df_logdata = pd.read_csv("../data/log-data.csv")

st_pattern = [x for x in df_pattern[1]]
print(pattern)