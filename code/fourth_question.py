from pyspark import SparkContext

from timeit import default_timer as timer

#### Driver program

# define the number of worker threads
num_workers = 1
sc = SparkContext(f"local[{num_workers}]")
sc.setLogLevel("ERROR")

# read the input file into an RDD[String]
rdd = sc.textFile("../data/task_events/part-00000-of-00500.csv")

num_partitions = rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

if num_partitions < num_workers:
    rdd = rdd.repartition(num_workers)
    print(f"Repartitioned from {num_partitions} to {num_workers}")

# Define the task column list and dictionary
task_column_list = ['timestamp', 'missing_info', 'job_ID', 'task_index', 'machine_ID', 
                    'event_type', 'user_name', 'scheduling_class', 'priority', 'resource_req_cpu', 
                    'resource_req_ram', 'resource_req_disk', 'constraint']
task_column_dict = {column: i for i, column in enumerate(task_column_list)}

# Constants for event types
SUBMIT = '0'
SCHEDULE = '1'
EVICT = '2'
FAIL = '3'
FINISH = '4'
KILL = '5'
LOST = '6'
UPDATE_PENDING = '7'
UPDATE_RUNNING = '8'

# Start recording the execution time
start = timer()

# Convert the rows from string to list of values
entries = rdd.map(lambda x: x.split(','))

entries.cache()

# Create a unique task ID by combining job_ID and task_index
def create_task_id(row):
    job_id = row[task_column_dict['job_ID']]
    task_index = row[task_column_dict['task_index']]
    return f"{job_id}_{task_index}"

# Map rows by (task_ID, (event_type, scheduling_class))
mapped_entries = entries.map(lambda row: (
    create_task_id(row), 
    (row[task_column_dict['event_type']], row[task_column_dict['scheduling_class']])
))

# Perform distinct to get unique scheduling classes
scheduling_classes = mapped_entries.map(lambda x: x[1][1]).distinct().collect()
print(f"Unique scheduling classes: {scheduling_classes}")

# Initialize accumulators for evicted and non-evicted counts per scheduling class
evicted_accumulators = {cls: sc.accumulator(0) for cls in scheduling_classes}
non_evicted_accumulators = {cls: sc.accumulator(0) for cls in scheduling_classes}

# Custom reduce function to compare event pairs and update accumulators
def custom_reduce(event1, event2):
    event_type1, sched_class1 = event1
    event_type2, sched_class2 = event2
    
    # Check if the first event is SCHEDULE or UPDATE_RUNNING
    if event_type1 in [SCHEDULE, UPDATE_RUNNING]:
        if event_type2 == EVICT:
            evicted_accumulators[sched_class1].add(1)
        elif event_type2 in [FINISH, KILL, LOST]:
            non_evicted_accumulators[sched_class1].add(1)
    
    # Return the second event for the next comparison
    return event2

# Reduce by key to process event sequences per task
mapped_entries.reduceByKey(custom_reduce).collect()

# Calculate the probability of eviction per scheduling class
probability_of_eviction = {}
for cls in scheduling_classes:
    evicted = evicted_accumulators[cls].value
    non_evicted = non_evicted_accumulators[cls].value
    total = evicted + non_evicted
    if total > 0:
        probability = evicted / total
    else:
        probability = 0
    probability_of_eviction[cls] = probability

# Print out the scheduling classes and their eviction probabilities
print("\nProbability of Eviction per Scheduling Class:")
for cls in sorted(probability_of_eviction.keys()):
    prob_percent = probability_of_eviction[cls] * 100
    print(f"Scheduling Class {cls}: {prob_percent:.2f}%")

# End recording the execution time
end = timer()
print(f"\nExecution time: {end - start} seconds")

# prevent the program from terminating immediately
input("\nPress Enter to continue...")
