# Frequent Itemset Analysis using Apache Spark

Based on a sample YELP dataset (Geolocation: Las Vegas) containing user ID and business ID, use SON algorithm (a modified Map Reduce approach to Apriori algorithm) 
to find out which restaurants are frequently visited together by same customers.


## Note about working environment

* The code was developed on Anaconda Package based on Python version 3.6.9 and pyspark version 2.4.4.


## File Explanation

* datasmall.csv -> A small sample of the YELP sample dataset that was used for analysis. The original dataset contains approximate 1 million rows of data point.
* result.png -> A simple image demonstrating the result achieved from applying SON algorithm to the YELP dataset.
* task2.py -> SON algorithm implementation in Python using Pyspark.


## Result
![alt text](https://github.com/frozendrpepper/Spark-Frequent-Itemset-Analysis/blob/master/result.png?raw=true)
* Why SON algorithm (over regular Apriori)? Single thread implementation of Apriori can be extremely slow and memory limiting when analyzing pairs of frequent items that occur together when analyzing over a larget dataset. An alternative algorithms such as PCY algorithm had been used to get around the memory issue. SON algorithm can be thought of as a Map Reduce approach to the Apriori algorithm and can utilize the multi-threaded nature and memory efficiency of Map-Reduce technology
* As one can observe from result.png, there are clear patterns among restaurants that are frequently visited together  
  * Customers who visit Japanese/Asian restaurant are more likely to visit other Japanese/Asian restaurants  
  * Customers who are visiting the city in the casion/hotel area are likely to visit restaurants in near vicinity to where hotels/casinos are congregated  
  * Customers who visit the Town Square Las Vegas Shopping Mall are likely to visit different businesses within the mall  
  * There was another group of restaurants that are extremely well rated and trendy that are frequently visited together  


## Useful References

Resources for Face Recognition Deep Neural Network
* [Stony Brooks University CSE 634 Lecture on Apriori Algorithm](https://www3.cs.stonybrook.edu/~cse634/lecture_notes/07apriori.pdf)
* [Brown Univeristy CS195 Lecture on Frequent Itemset (Apriori and SON algorithm](http://cs.brown.edu/courses/cs195w/slides/freqitems.pdf)
* [Frank Kane's Taming Big Data with Apache Spark and Python](https://www.amazon.com/Frank-Kanes-Taming-Apache-Python/dp/1787287947/ref=sr_1_3?crid=1B9GYVZBS826X&dchild=1&keywords=frank+kane%27s+taming+big+data+with+apache+spark+and+python&qid=1594846898&sprefix=frank+kane%27s%2Caps%2C200&sr=8-3)
