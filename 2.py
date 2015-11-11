from pyspark import SparkContext, SparkConf

sc = SparkContext("local", "2")


logfile="file:/home/training/training_materials/sparkdev/data/weblogs/*"
logs = sc.textFile(logfile)
jpglogs=logs.filter(lambda x: ".jpg" in x)

print jpglogs.take(10)

print sc.textFile(logfile).filter(lambda x: ".jpg" in x).count()

print logs.map(lambda s: len(s)).take(5)

print logs.map(lambda s: s.split()).take(5)

# extract the first column; ip-address
ips = logs.map(lambda line: '%s/%s' % (line.split()[0], line.split()[2]))

# print top 10 ip-addresses
for x in ips.take(10): print x

# save the whole list
ips.saveAsTextFile("file:/home/training/iplist-with-userid")


