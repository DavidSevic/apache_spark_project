from pyspark import SparkContext

import time
from timeit import default_timer as timer

#### Driver program

# define the number of worker threads
num_workers = 1
sc = SparkContext(f"local[{num_workers}]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
rdd = sc.textFile("../data/machine_events/part-00000-of-00001.csv")

num_partitions = rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

if num_partitions < num_workers:
    rdd = rdd.repartition(num_workers)
    print(f"Repartitioned from {num_partitions} to {num_workers}")

# Since files do not have header, we define it manually
column_list = ['timestamp', 'machine_ID', 'event_type', 'platform_ID', 'capacity_CPU', 'capacity_MEM']
column_dict = {column: i for i, column in enumerate(column_list)}

# Constants for event types
EVENT_ADD = 0
EVENT_REMOVE = 1
EVENT_UPDATE = 3

# Start recording the execution time
start = timer()

# filter out the invalid rows (those that are missing fields adn therefore have multiple sequential comma delimiters)
filtered = rdd.filter(lambda x: not (",," in x))

# Convert the rows from string to list of values
entries = filtered.map(lambda x : x.split(','))

entries.cache()

# Remove the duplicates, to get the real distribution
# Map the entries by (machine_id, row), so that they can be reduced afterwards
mapped_entries = entries.map(lambda row: (row[column_dict['machine_ID']], row))
# Reduce by key to keep the first occurence of duplicate rows
distinct_entries = mapped_entries.reduceByKey(lambda row1, row2: row1).map(lambda x: x[1])

# Keep only the cpu_capacities
cpu_capacities = distinct_entries.map(lambda x: x[column_dict['capacity_CPU']])

# Count by capacity_CPU
cpu_distributions = cpu_capacities.countByValue()
for capacity, count in sorted(cpu_distributions.items()):
    print(f"CPU capacity: {capacity}, count: {count}")


# End recording the execution time
end = timer()
print(f"Execution time: {end - start}")

# prevent the program from terminating immediately
input("Press Enter to continue...")