import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("yarn").appName("hdfs visual").getOrCreate()
path = str(sys.argv[1])
depth = int(sys.argv[2])
df = spark.read.format('csv').options(header='true', inferSchema='true', delimiter='\\t').load(path)

def reduce_path(row, depth):
    path = row['Path']
    size = row['FileSize']
    blocksCount = row['BlocksCount']

    if not path or not size or not blocksCount:     # remove null entries from fsimage
        path = '/'
        size = 0
        blocksCount = 0

    path = path.encode('utf-8')

    large_dir = ['/tmp']
    for dir in large_dir:
        if path.startswith(dir):
            depth = 1
            break

    has_space = ' ' in path
    i = 0
    while not has_space and i < len(path) and depth > 0:
        if path[i] == '/':
            yield (path[:i+1], (size, blocksCount))
            depth -= 1
        i += 1

rdd = df.select('Path', 'FileSize', 'BlocksCount').rdd \
    .flatMap(lambda x:reduce_path(x, depth)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .sortByKey() \
    .map(lambda x: (x[0], x[1][0], x[1][1]))

outdf = spark.createDataFrame(rdd)
outdf.coalesce(1).write.csv("/tmp/hdfsvisual2.out", sep='\t')