from pyspark import SparkContext
from timeit import default_timer as timer

#### Driver program

# define the number of worker threads
num_workers = 1
sc = SparkContext(f"local[{num_workers}]")
sc.setLogLevel("ERROR")

# read the input files into an RDD[String]
# Task events
rdd_tasks = sc.textFile("../data/task_events/part-00000-of-00500.csv")
# Task usages
rdd_usage = sc.textFile("../data/task_usage/part-00000-of-00500.csv")

num_partitions_tasks = rdd_tasks.getNumPartitions()
num_partitions_usage = rdd_usage.getNumPartitions()
print(f"Number of partitions for task events: {num_partitions_tasks}")
print(f"Number of partitions for task usage: {num_partitions_usage}")

if num_partitions_tasks < num_workers:
    rdd_tasks = rdd_tasks.repartition(num_workers)
    print(f"Repartitioned task events from {num_partitions_tasks} to {num_workers}")

if num_partitions_usage < num_workers:
    rdd_usage = rdd_usage.repartition(num_workers)
    print(f"Repartitioned task usage from {num_partitions_usage} to {num_workers}")

# Since files do not have header, we define it manually
task_column_list = [
    'timestamp', 'missing_info', 'job_ID', 'task_index', 'machine_ID', 'event_type',
    'user_name', 'scheduling_class', 'priority', 'resource_req_cpu', 'resource_req_ram',
    'resource_req_disk','constraint'
]
task_column_dict = {col: i for i, col in enumerate(task_column_list)}

usage_column_list = [
    'start_time', 'end_time', 'job_ID', 'task_index', 'machine_ID',
    'mean_CPU_usage_rate', 'canonical_memory_usage', 'assigned_memory_usage',
    'unmapped_page_cache', 'total_page_cache', 'max_memory_usage',
    'mean_disk_IO_time', 'mean_local_disk_space_used', 'max_CPU_usage', 'max_disk_IO_time',
    'cycles_per_instruction', 'memory_accesses_per_instr', 'sample_portion',
    'aggregation_type', 'sampled_CPU_usage'
]
usage_column_dict = {col: i for i, col in enumerate(usage_column_list)}

EVICT_EVENT_TYPE = '2'
FINISH_EVENT_TYPE = '4'

# Start recording the execution time
start = timer()

# Convert the rows from string to list of values
usage_entries = rdd_usage.map(lambda x: x.split(','))
# Mapping task index and job ids to mean local disk space used
usage_data = usage_entries.map(
    lambda row: (
        f"{row[usage_column_dict['job_ID']]}_{row[usage_column_dict['task_index']]}",
        float(row[usage_column_dict['mean_local_disk_space_used']])
    )
)
# We keep the maximum of all measurement recording for each task
used_disk_rdd = usage_data.reduceByKey(lambda a, b: max(a, b))

def compute_ratio_for_event_type(event_type):
    # Filter task events for the given event type
    filtered_tasks = rdd_tasks.map(lambda x: x.split(',')) \
        .filter(lambda row: len(row) > task_column_dict['event_type'] 
                           and row[task_column_dict['event_type']] == event_type) \
        .filter(lambda row: row[task_column_dict['resource_req_disk']] != '')

    # Map task index and job id to the requested disk local space
    task_data = filtered_tasks.map(
        lambda row: (
            f"{row[task_column_dict['job_ID']]}_{row[task_column_dict['task_index']]}",
            float(row[task_column_dict['resource_req_disk']])
        )
    )
    # In case there are duplicated, keep the maximum
    requested_disk_rdd = task_data.reduceByKey(lambda a, b: max(a, b))

    # Join with usage RDD
    joined = requested_disk_rdd.join(used_disk_rdd)

    # Compute ratio = (used / requested) per task
    ratio_rdd = joined.mapValues(
        lambda pair: pair[1] / pair[0] if pair[0] != 0 else 0
    )

    # Compute average ratio across tasks
    avg_ratio = ratio_rdd.map(lambda x: x[1]).mean()
    return avg_ratio


avg_ratio_finish = compute_ratio_for_event_type(FINISH_EVENT_TYPE)
avg_ratio_evicted = compute_ratio_for_event_type(EVICT_EVENT_TYPE)

# Convert to percentages
pct_finish = avg_ratio_finish * 100
pct_evict = avg_ratio_evicted * 100

print("\n===== COMPARISON: FINISHED vs. EVICTED TASKS =====")
print(f"Average ratio (FINISHED tasks) = {avg_ratio_finish:.4f}  ({pct_finish:.2f}%)")
print(f"Average ratio (EVICTED tasks)  = {avg_ratio_evicted:.4f}  ({pct_evict:.2f}%)")

# End recording the execution time
end = timer()
print(f"\nExecution time: {end - start} seconds")

# Prevent the program from terminating immediately
input("\nPress Enter to continue...")
