import time
import os
import sys
import re
from pyspark import SparkContext, SparkConf


def build_index(path):
    conf = SparkConf().setAppName("InvertedIndex")
    sc = SparkContext(conf=conf)
    rdd = sc.wholeTextFiles(path)

    def tokenize(record):
        fpath, txt = record
        fname = os.path.basename(fpath)  # ignore the HDFS uri
        out = []
        for w in txt.split():
            c = re.sub(r"[^A-Za-z']+", "", w).lower()
            if c:
                out.append(((c, fname), 1))
        return out

    pairs = rdd.flatMap(tokenize)
    counts = pairs.reduceByKey(lambda a, b: a + b)
    entries = counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
    idx = entries.groupByKey().mapValues(list)
    return sc, idx


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit InvertedIndexSpark.py <size> <output>")
        sys.exit(1)

    size, output = sys.argv[1], sys.argv[2]
    input_path = f"hdfs:///user/hadoop/InvertedIndex/input/{size}"
    output_path = f"hdfs:///user/hadoop/InvertedIndex/spark/{output}"

    start_build = time.time()
    sc, idx = build_index(input_path)
    idx.count()  # using an action to trigger the build
    build_time = time.time() - start_build

    start_save = time.time()
    formatted = idx.map(
        lambda kv: kv[0] + "\t" + "\t".join(f"{fn}:{cnt}" for fn, cnt in kv[1])
    )
    formatted.saveAsTextFile(output_path)
    save_time = time.time() - start_save

    print(f"Index build time: {build_time:.3f}s")
    print(f"Save time: {save_time:.3f}s")

    sc.stop()
