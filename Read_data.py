import sys
import os
from pyspark import SparkConf
import Produce_data
from pyspark import SparkContext

basedata = os.path.join('D:/Codes/Cache/data')
inputpath = os.path.join('access_2013_05_30.log')
logfile = os.path.join(basedata, inputpath)

sc = SparkContext.getOrCreate(SparkConf())

def parseLogs():
    parsed_logs = (sc.textFile(logfile).map(Produce_data.parse_apache_logline).cache())
    access_logs = (parsed_logs.filter(lambda s: s[1] == 1).map(lambda s: s[0]).cache())
    failed_logs = (parsed_logs.filter(lambda s: s[1] == 0).map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print('Num of invaild logline: %d' % failed_logs_count)
        for line in failed_logs.take(20):
            print('Invalid line %s' % line)
    print('All %d lines, successde parsed %d lines, failed parsed %d lines'
          % (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs

parsed_logs, access_logs, failed_logs = parseLogs()
print(parsed_logs, access_logs, failed_logs)