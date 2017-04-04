from pyspark import SparkConf, SparkContext
import sys
import re
import os
import datetime
from pyspark import Row

APP_NAME = "Second Part"


def parsing_date(date):
	months_dic = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

	return datetime.datetime(int(date[7:11]),months_dic[date[3:6]],int(date[0:2]),int(date[12:14]),int(date[15:17]),int(date[18:20]))

def parse_log_files(log_file):

	pattern = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
	match = re.search(pattern, log_file)
	if match is None:
		return (log_file, 0)

	return (Row(
		ip = match.group(1),
        date = parsing_date(match.group(4)),
        method  = match.group(5),
        urls = match.group(6),
        status = int(match.group(8)),), 1)


def generate_rdds(filename):

    logs = sc.textFile(filename).map(parse_log_files).cache()

    access_logs =  logs.filter(lambda s: s[1] == 1).map(lambda s: s[0]).cache()

    failed_logs = logs.filter(lambda s: s[1] == 0).map(lambda s: s[0]).cache()
    
    return logs, access_logs, failed_logs
 

def main(sc, filename, choice, urls=None):

	logs, access_logs, failed_logs = generate_rdds(filename)

	print 'We have read {0} lines \n - {1} lines are good \n - {2} lines are bad \n'\
	.format(logs.count(),access_logs.count(),failed_logs.count())


	# #1)
	# #2)
	# def url_count():
	# 	result = (access_logs.filter(lambda s : '/assets/js/lowpro.js' == s.urls).map(lambda s : (s.urls, 1)).reduceByKey(lambda x,y : x + y).collect())
	# 	print 'The first question is %s' %(result)
	# 	result = (access_logs.filter(lambda s : '/favicon.ico' == s.urls).map(lambda s : (s.urls,1)).reduceByKey(lambda x,y : x + y).collect())
	# 	print 'The Second question is %s' %(result)


	#1)
	#2)
	def url_count():
		result = (access_logs.filter(lambda s : urls == s.urls).map(lambda s : (s.urls, 1)).reduceByKey(lambda x,y : x + y).collect())
		print 'The first question is %s' %(result)


	#3)
	def url_max():
		result = (access_logs.map(lambda s : (s.urls,1)).reduceByKey(lambda x,y : x + y).sortBy(lambda s : -s[1]).take(1))
		print 'The Third question is %s' %(result)

	#4)
	def ip_max():
		result = (access_logs.map(lambda s : (s.ip,1)).reduceByKey(lambda x,y : x + y).sortBy(lambda s : -s[1]).take(1))
		print 'The forth question is %s' %(result)



	choices = {
			'urlCount' : url_count,
			'ipMax': ip_max,
			'urlMax' : url_max
	}

	if choice in choices:
		choices[choice]()

	

if __name__ == '__main__':
	
	# conf = SparkConf().setAppName(APP_NAME).set("spark.executor.memory", "2g")
	# conf = conf.setMaster("spark://Abdulazizs-MacBook-Pro.local:7077")

	conf = SparkConf().setAppName(APP_NAME)
   	# conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)

	if len(sys.argv) == 3:
		filename = sys.argv[1]
		choice = sys.argv[2]	# 'urlCount' , 'ipMax', 'urlMax'
		main(sc, filename,choice, None)

	else:
		filename = sys.argv[1]
		choice = sys.argv[2]	# 'urlCount' , 'ipMax', 'urlMax'
		urls = sys.argv[3]
		main(sc, filename,choice, urls)
	







