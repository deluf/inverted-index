
import time
import os
import sys
import re
from collections import defaultdict

num_executors = 5
executor_cores = 2
num_parallel_tasks = num_executors * executor_cores

from pyspark import SparkContext, SparkConf

def build_inverted_index(input_path, output_path):
    
    conf = SparkConf().setAppName("InvertedIndex")
    conf.set("spark.executor.instances", f"{num_executors}")
    conf.set("spark.executor.cores", f"{executor_cores}")
    conf.set("spark.yarn.am.memory", "1g")
    conf.set("spark.executor.memoryOverhead", "1g")
    conf.set("spark.executor.memory", "1g")

    sc = SparkContext(conf=conf)

    # Read entire files as (file_path, file_content) pairs from the input directory
    documents_rdd = sc.wholeTextFiles(input_path, num_parallel_tasks * 5)

    # Pre-compile the regex pattern for better performances
    pattern = re.compile(r"[^A-Za-z]+")

    # Basically behaves like an in-mapper combiner
    def tokenize_file_content(file_record):
        file_path, content = file_record
        filename = os.path.basename(file_path)  # Extract filename from path
        word_counts = defaultdict(int)

        for word in content.split():
            cleaned_word = pattern.sub("", word).lower()
            if cleaned_word:
                word_counts[cleaned_word] += 1

        return ((word, [(filename, count)]) for word, count in word_counts.items())

    # Tokenize and emit (word, [(filename, count)]) pairs
    word_counts_single_file = documents_rdd.flatMap(tokenize_file_content)

    # Concatenate the (filename, count) of each word
    word_counts_all_files = word_counts_single_file.reduceByKey(lambda this, that: this + that)

    # Format output: word \t file1:count \t file2:count ...
    formatted_output = word_counts_all_files.map(
        lambda entry: entry[0] + "\t" + "\t".join(f"{filename}:{count}" for filename, count in entry[1])
    )

    # Optionally coalesce the output files
    formatted_output = formatted_output.coalesce(num_parallel_tasks * 2)

    # Save the results to HDFS
    formatted_output.saveAsTextFile(output_path)

    # Cleanly shut down the application
    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit InvertedIndexSpark.py <input_size> <output_dir>")
        sys.exit(1)

    input_size_arg = sys.argv[1]
    output_dir_arg = sys.argv[2]

    input_path = f"hdfs:///user/hadoop/InvertedIndex/input/{input_size_arg}"
    output_path = f"hdfs:///user/hadoop/InvertedIndex/spark/{output_dir_arg}"

    start_time = time.time()
    build_inverted_index(input_path, output_path)
    duration = time.time() - start_time

    print(f"Total execution time: {duration:.3f}s")
