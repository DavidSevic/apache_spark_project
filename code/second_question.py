from pyspark import SparkContext

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
EVENT_ADD = '0'
EVENT_REMOVE = '1'
EVENT_UPDATE = '2'

# Start recording the execution time
start = timer()

# Convert the rows from string to list of values
entries = rdd.map(lambda x : x.split(','))

entries.cache()

# Map rows by (machine_ID, [timestamp, event_type])
mapped_entries = entries.map(lambda row: (row[column_dict['machine_ID']], (row[column_dict['timestamp']], row[column_dict['event_type']], row[column_dict['machine_ID']])))

# Sort the mapped rows to make sure the events are in timestamp order
#sorted_entries = mapped_entries.groupByKey().mapValues(lambda events: sorted(events, key=lambda x: x[0]))

# Total running and down times for all machines. Since workers are distributed, these counters have to be accumulators
time_running = sc.accumulator(0)
time_down = sc.accumulator(0)

# Custom reduce function to apply more complex logic
def custom_reduce(row1, row2):
    # If the first event is shut down and the second event is turn on, then the passed time is down time
    if row1[1] == EVENT_REMOVE:
        time_down.add(int(row2[0]) - int(row1[0]))
    else:
        # if the first time is turn on or update, then whatever the second event is, the passed time is running time
        time_running.add(int(row2[0]) - int(row1[0]))
    
    # return the second row so that we can keep repeating these comparisons
    return row2

# Reduce by key so that we can compare rows of same machines and sum up the time passed between events
# Collect is called to trigger the lazy workers
distinct_entries = mapped_entries.reduceByKey(custom_reduce).collect()

# Calculate the percentage
computation_lost = (time_down.value / (time_running.value + time_down.value)) * 100

# Convert timestamps from ns to hours, minutes and seconds
def convert_ns(time_ns):
    seconds_total = time_ns // 1_000_000_000
    hours, left_over = divmod(seconds_total, 3600)
    minutes, seconds = divmod(left_over, 60)
    return f"{hours}h:{minutes}m:{seconds}s"

# Printout the result
print(f"\nTime running: {convert_ns(time_running.value)}")
print(f"\nTime down: {convert_ns(time_down.value)}")
print(f"\nComputation lost in percentage: {computation_lost} %")

# End recording the execution time
end = timer()
print(f"\nExecution time: {end - start}")

# prevent the program from terminating immediately
input("\nPress Enter to continue...")