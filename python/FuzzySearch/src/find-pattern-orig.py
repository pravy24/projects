# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 11:41:35 2017

@author: Praveen

patt = 'aabcdde'
data = [{1:'aabcdde'}, {2:'abcde'}, {3:'abbccdde'}, {4:'axabcdde'},{5:'aaxccde'}, {6:'zsdacge'}]
result = process.extract(patt, data)
print(result)

FUZZYWUZZY.PROCESS.EXTRACT
---------------------------
    
    def extract (query, choices, processor=default_processor, scorer=default_scorer, limit=5)

    Select the best match in a list or dictionary of choices.
    Find best matches in a list or dictionary of choices, return a
    list of tuples containing the match and it's score. If a dictionary
    is used, also returns the key for each match.
    Arguments:
        query: An object representing the thing we want to find.
        choices: An iterable or dictionary-like object containing choices
            to be matched against the query. Dictionary arguments of
            {key: value} pairs will attempt to match the query against
            each value.
        processor: Optional function of the form f(a) -> b, where a is an
            individual choice and b is the choice to be used in matching.
            This can be used to match against, say, the first element of
            a list:
            lambda x: x[0]
            Defaults to fuzzywuzzy.utils.full_process().
        scorer: Optional function for scoring matches between the query and
            an individual processed choice. This should be a function
            of the form f(query, choice) -> int.
            By default, fuzz.WRatio() is used and expects both query and
            choice to be strings.
        limit: Optional maximum for the number of elements returned. Defaults
            to 5.
    Returns:
        List of tuples containing the match and its score.
        If a list is used for choices, then the result will be 2-tuples.
        If a dictionary is used, then the result will be 3-tuples containing
        he key for each match.
        For example, searching for 'bird' in the dictionary
        {'bard': 'train', 'dog': 'man'}
        may return
        [('train', 22, 'bard'), ('man', 0, 'dog')]


"""
from functools import reduce
from itertools import groupby
from fuzzywuzzy import process
import pandas as pd



def reduceByKey(func, iterable):
    # Created by: Juanlu001, github
    # https://gist.github.com/Juanlu001/562d1ec55be970403442
    """Reduce by key.
    Equivalent to the Spark counterpart
    Inspired by http://stackoverflow.com/q/33648581/554319
    1. Sort by key
    2. Group by key yielding (key, grouper)
    3. For each pair yield (key, reduce(func, last element of each grouper))
    """
    get_first = lambda p: p[0]
    get_second = lambda p: p[1]

    # iterable.groupBy(_._1).map(l => (l._1, l._2.map(_._2).reduce(func)))
    output = map (
                    lambda l: {l[0]: reduce(func, map(get_second, l[1]))}, 
                    groupby(sorted(iterable, key=get_first), get_first)
                 )
    
    return(output)



if __name__=="__main__":
    
    df_pattern = pd.read_csv("../data/pattern.csv", header=None)
    df_logdata = pd.read_csv("../data/log-data.csv", header=None)
    separator = '%'
    
    """ Make a string of log-events separated by 'separator' charatecter
        Ex: 'a%b%c%'
    """
    pattern = reduce(lambda x, y: x + separator + y, df_pattern[1])
    
    """ Make a list of tuples [(id, event)] of id and log-events
        Ex: [(1, 'a'), (1, 'b'), (1, 'c'), (2, 'a'), (2, 'e')]
    """
    dict_logdata = map(lambda x, y: (x, y), df_logdata[0], df_logdata[2])
    
    """ Returns a list of dictionary consists of key and a string of events
        Ex: [{1:'a%b%c', 2:'a%e'}]
    """
    list_logdata = reduceByKey(lambda x, y: x + separator + y, dict_logdata)
    
    result = process.extract(pattern, list_logdata, limit=1000000)
    
    print(result)