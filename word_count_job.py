import sys
from pyspark.sql import SparkSession

# --- 1. Initialize Spark ---
# PySpark handles the connection to the Dataproc cluster and resource manager (YARN).
def word_count():
    # Build the SparkSession. In Dataproc, this configuration is automatic.
    spark = SparkSession.builder.appName("DistributedWordCountLab").getOrCreate()
    sc = spark.sparkContext # Get the underlying Spark Context (used for RDD operations)
    
    # Check for required input/output paths
    if len(sys.argv) != 3:
        print("Usage: word_count_job.py <input_path> <output_path>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # --- 2. Load Data and Define Map Phase ---
    # Load the text file from Google Cloud Storage into a Resilient Distributed Dataset (RDD)
    # The framework splits this file across worker nodes automatically.
    lines = sc.textFile(input_path)
    
    # RDD Transformation 1: MAP PHASE (flatMap)
    # The lambda function is used directly here for concise, inline processing.
    # It converts the line to lowercase and splits it into a list of words.
    words = lines.flatMap(lambda line: line.lower().split(" "))
    
    # RDD Transformation 2: INTERMEDIATE KEY-VALUE
    # Emit the (word, 1) pair for every word found, using a lambda function.
    word_pairs = words.map(lambda word: (word, 1))

    # --- 3. Define Shuffle and Reduce Phase ---
    # RDD Transformation 3: SHUFFLE, SORT, and REDUCE (reduceByKey)
    # This single operation combines the expensive Shuffle/Sort phase with the aggregation.
    # 1. SHUFFLE/SORT: Groups all pairs by the 'word' key.
    # 2. REDUCE: Applies the provided function (sum) to all lists of values for that key.
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

    # --- 4. Write Output ---
    # Write the final result (the RDD of (word, count) pairs) back to Cloud Storage.
    # This is the final major I/O step.
    word_counts.saveAsTextFile(output_path)
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    word_count()
