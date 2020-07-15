# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 02:42:32 2019

@author: Charlie
"""

#import findspark
#findspark.init()

from pyspark import SparkConf, SparkContext
import itertools as it
import sys
import time

def mapToList1(iterator):
    '''First convert line which comes in as bytes to a string by decoding'''
    return_iter = []
    for line in iterator:
        line = line.decode('utf-8')
        line = line.split(',')
        #split the string to separate
        user = line[0]
        business = line[1]
        return_iter.append((user, [business]))
    return return_iter

def combinationUnfold(line):
    '''This function unfolds all the unique combination itemssets'''
    return line[1]

def findComb(line, n):
    result_set = set()
    result_list = []
    tmp = it.combinations(line, n)
    for item in tmp:
        result_list.append(item)
    for item in result_list:
        result_set.add(item)
    return result_set

def findComb2(line, n, possible_frequent, unique_singles):
    '''This is a mapValues function so line by line'''
    new_line = set()
    for single in unique_singles:
        if single in line:
            new_line.add(single)
    
    result_set = set()
    result_list = []
    tmp = it.combinations(new_line, n)
    for item in tmp:
        item = list(item)
        item.sort()
        item = tuple(item)
        if item in possible_frequent:
            result_list.append(item)
    for item in result_list:
        result_set.add(item)
    return result_set

def aggregatePartition(iterator):
    '''Aggregate count of each key for each partition without final shuffling'''
    result = {}
    for item in iterator:
        item = list(item)
        item.sort()
        item = tuple(item)
        if item in result:
            result[item] += 1
        else:
            result[item] = 1
    return_result = []
    for key, value in result.items():
        return_result.append((key, value))
    return return_result

def aggregatePartitionFrequent(iterator, possible_frequent):
    result = {}
    for item in iterator:
        item = list(item)
        item.sort()
        item = tuple(item)
        if item in possible_frequent:
            if item in result:
                result[item] += 1
            else:
                result[item] = 1
    return_result = []
    for key, value in result.items():
        return_result.append((key, value))
    return return_result

'''Take command line arguments'''
k = int(sys.argv[1])
support = int(sys.argv[2])
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]

#support = 50
#k = 70
#input_file_path = 'user_business.csv'
#output_file_path = input_file_path + '_case' + str(case) + '_support' + str(support) + '_k' + str(k) + '.txt'

case = 1   
start = time.time()   
#Case number and dictionary definition for applying different functions
case_dict = {1: mapToList1}

conf = SparkConf().setMaster("local[*]").setAppName("INF553_HW2_Task1")
#conf.set("spark.default.parallelism", '2')
sc = SparkContext(conf = conf)

#Import data as a RDD Object
small_rdd = sc.textFile(input_file_path, use_unicode = False)
num_partitions = small_rdd.getNumPartitions()
modified_support = int(support / num_partitions)

#Filter out the header in csv
header = small_rdd.first()
small_rdd = small_rdd.filter(lambda row: row != header)

#Collect the values by key depending on the case
'''map_func is decided depending on which case we are dealing with'''
map_func = case_dict[case]
small_rdd_valuelist = small_rdd.mapPartitions(map_func)

''' the | symbol combines elements in sets and this creates a basket
in the form document: {elements}'''
basket_pre_pre = small_rdd_valuelist.reduceByKey(lambda x, y: x + y)
basket_pre = basket_pre_pre.filter(lambda x: len(x[1]) > k)
basket = basket_pre.mapValues(lambda x: set(x))

subset_size = 1
frequent_itemset = []
candidate_itemset = []
while True:   
    if subset_size <= 2:
        '''From RDD, extract all subset sized itemsets'''
        basket_combinations = basket.mapValues(lambda x: findComb(x, subset_size))
        
        '''Unfold all the items'''
        basket_combinations_unfold = basket_combinations.flatMap(combinationUnfold)
        
        '''MapReduce Phase 1'''
        '''Aggregate result only for those that are in the possible frequent list'''
        basket_combinations_filter_aggregate = basket_combinations_unfold.mapPartitions(aggregatePartition)
        
        '''Pick candidates using the threshold value'''
        basket_candidate = basket_combinations_filter_aggregate.filter(lambda x: x[1] >= modified_support)
        
        '''Get candidates'''
        candidates = set(basket_candidate.map(lambda x: x[0]).collect())
        
        '''MapReduce Phase 2'''    
        '''First get counts of items in each partition that is in the candidate list'''
        partition_candidates_count = basket_combinations_unfold.mapPartitions(lambda iterator: aggregatePartitionFrequent(iterator, candidates))
        
        '''Total count of each candidate itemset'''
        total_candidates_count = partition_candidates_count.reduceByKey(lambda x, y: x + y)
        
        '''Filter out keys whose count is less than the actual support'''
        total_candidates_count_filtered = total_candidates_count.filter(lambda x: x[1] >= support)
    
        '''Compile all the keys inside total candidates count filtered to get frequent singles'''
        frequent_items = total_candidates_count_filtered.map(lambda x: x[0]).collect()
    else:
        prev_frequent = frequent_itemset[subset_size - 2]
        '''Find all possible combinations of size of subset_size using previous frequent itemset'''
        possible_frequent = []
        for i in range(len(prev_frequent)):
            for j in range(i + 1, len(prev_frequent)):
                tmp = set(prev_frequent[i] + prev_frequent[j])
                if len(tmp) == subset_size:
                    tmp = list(tmp)
                    tmp.sort()
                    possible_frequent.append(tuple(tmp))
        possible_frequent.sort()
        possible_frequent = set(possible_frequent)
        
        '''Find all singles present in possible_frequent'''
        unique_singles = set()
        for items in possible_frequent:
            for item in items:
                unique_singles.add(item)
        
        '''From RDD, extract all subset sized itemsets'''
        basket_combinations = basket.mapValues(lambda x: findComb2(x, subset_size, possible_frequent, unique_singles))
        
        '''Unfold all the items'''
        basket_combinations_unfold = basket_combinations.flatMap(combinationUnfold)
       
        '''MapReduce Phase 1'''
        '''Aggregate result only for those that are in the possible frequent list'''
        basket_combinations_filter_aggregate = basket_combinations_unfold.mapPartitions(aggregatePartition)
        
        '''Pick candidates using the threshold value'''
        basket_candidate = basket_combinations_filter_aggregate.filter(lambda x: x[1] >= modified_support)
        
        '''Get candidates'''
        candidates = set(basket_candidate.map(lambda x: x[0]).collect())
        
        '''MapReduce Phase 2'''    
        '''First get counts of items in each partition that is in the candidate list'''
        partition_candidates_count = basket_combinations_unfold.mapPartitions(lambda iterator: aggregatePartitionFrequent(iterator, candidates))
        
        '''Total count of each candidate itemset'''
        total_candidates_count = partition_candidates_count.reduceByKey(lambda x, y: x + y)

        '''Filter out keys whose count is less than the actual support'''
        total_candidates_count_filtered = total_candidates_count.filter(lambda x: x[1] >= support)

        '''Compile all the keys inside total candidates count filtered to get frequent singles'''
        frequent_items = total_candidates_count_filtered.map(lambda x: x[0]).collect()
    
    '''Compile results'''
    candidate_itemset.append(candidates)
    frequent_itemset.append(frequent_items)

    subset_size += 1
    '''The exit condition should be if frequent_items is of size 0 or 1'''
    if len(candidates) == 0:
        break
    
end = time.time()
duration = end - start
print("Duration: ", duration)

'''Write out the candidates and actual frequent itemsets'''
with open(output_file_path, 'w', encoding = "utf-8") as theFile:
    theFile.write("Candidates:")
    theFile.write("\n")
    candidate_string = ""
    count = 0
    for items in candidate_itemset:
        if len(items) == 0:
            continue
        items = list(items)
        items.sort()
        item_list = list(map(str, items))
        if count == 0:
            for i in range(len(item_list)):
                item_list[i] = item_list[i][:-2] + item_list[i][-1]
        result = ",".join(item_list)
        candidate_string += result
        candidate_string += "\n\n"
        count += 1
    theFile.write(candidate_string)
    
    theFile.write("Frequent Itemsets:")
    theFile.write("\n")
    frequent_string = ""
    count = 1
    for items in frequent_itemset:
        if len(frequent_itemset) > 0:
            items.sort()
            item_list = list(map(str, items))
            if count == 1:
                for i in range(len(item_list)):
                    item_list[i] = item_list[i][:-2] + item_list[i][-1]
                count += 1
            result = ",".join(item_list)
            frequent_string += result
            frequent_string += "\n\n"
    theFile.write(frequent_string)