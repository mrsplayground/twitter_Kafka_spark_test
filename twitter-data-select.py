import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.3 pyspark-shell'
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


conf = SparkConf().setMaster("local[2]").setAppName("Tweets3")
sc = SparkContext(conf = conf)
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 15)

kvs = KafkaUtils.createDirectStream(ssc, ['tweets'], {"metadata.broker.list": 'x.x.x.x:9092',"auto.offset.reset": "smallest"})

lines = kvs.map(lambda x: x[1])

#splitting line into dictionary using ";"
counts = lines.map(lambda line: line.split(";"))#\

#limiting fields returned to the 1st one
column1 = counts.map(lambda x: x[0])

#writing output to files
column1.saveAsTextFiles('test.txt')

ssc.start()
ssc.awaitTermination()