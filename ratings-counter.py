from pyspark import SparkConf, SparkContext
import collections

# Configurations on Spark
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# Parse the data into different fields
ratings = lines.map(lambda x: x.split()[2])

# Run some functions on the ratings
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
