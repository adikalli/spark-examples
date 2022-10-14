from pyspark.context import SparkContext
sc = SparkContext('local', 'WorldCount')

text_file = sc.textFile("hamlet.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("hamlet_word_counts")
