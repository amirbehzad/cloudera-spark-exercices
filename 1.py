from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

mydata = sc.textFile("file:/home/training/training_materials/sparkdev/data/frostroad.txt")
print 'Total records in frostroad.txt: %d' % mydata.count()
print 'Dump of data in frostroad.txt'
print mydata.collect()


