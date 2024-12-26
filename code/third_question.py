from pyspark import SparkContext

from timeit import default_timer as timer

#### Driver program

# define the number of worker threads
num_workers = 1
sc = SparkContext(f"local[{num_workers}]")
sc.setLogLevel("ERROR")

# read the input file into an RDD[String]
rdd = sc.textFile("../data/job_events/part-00000-of-00500.csv")

num_partitions = rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

if num_partitions < num_workers:
    rdd = rdd.repartition(num_workers)
    print(f"Repartitioned from {num_partitions} to {num_workers}")

# Since files do not have header, we define it manually
job_column_list = ['timestamp', 'missing_info', 'job_ID', 'event_type', 'user_name', 'scheduling_class', 'job_name', 'logical_job_name']
job_column_dict = {column: i for i, column in enumerate(job_column_list)}

# Start recording the execution time
start = timer()

# Convert the rows from string to list of values
entries = rdd.map(lambda x : x.split(','))

entries.cache()

# Map the entries into (scheduling_class, 1) pairs
mapped_entries = entries.map(lambda row: (row[job_column_dict['scheduling_class']], 1))

counted_entries = mapped_entries.reduceByKey(lambda row1, row2: row1 + row2)

for scheduling_class, count in counted_entries.collect():
    print(f"scheduling class: {scheduling_class}, count: {count}")

# End recording the execution time
end = timer()
print(f"\nExecution time: {end - start}")

# prevent the program from terminating immediately
input("\nPress Enter to continue...")