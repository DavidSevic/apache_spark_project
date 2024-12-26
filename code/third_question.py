from pyspark import SparkContext

from timeit import default_timer as timer

#### Driver program

# define the number of worker threads
num_workers = 1
sc = SparkContext(f"local[{num_workers}]")
sc.setLogLevel("ERROR")

# read the input files into an RDD[String]
# Job events
rdd_jobs = sc.textFile("../data/job_events/part-00000-of-00500.csv")
# Task events
rdd_tasks = sc.textFile("../data/task_events/part-00000-of-00500.csv")

num_partitions_jobs = rdd_jobs.getNumPartitions()
num_partitions_tasks = rdd_tasks.getNumPartitions()
print(f"Number of partitions for job events: {num_partitions_jobs}")
print(f"Number of partitions for task events: {num_partitions_tasks}")

if num_partitions_jobs < num_workers:
    rdd_jobs = rdd_jobs.repartition(num_workers)
    print(f"Repartitioned job events from {num_partitions_jobs} to {num_workers}")

if num_partitions_tasks < num_workers:
    rdd_tasks = rdd_tasks.repartition(num_workers)
    print(f"Repartitioned task events from {num_partitions_tasks} to {num_workers}")

# Since files do not have header, we define it manually
job_column_list = ['timestamp', 'missing_info', 'job_ID', 'event_type', 'user_name', 'scheduling_class', 'job_name', 'logical_job_name']
job_column_dict = {column: i for i, column in enumerate(job_column_list)}

task_column_list = ['timestamp', 'missing_info', 'job_ID', 'task_index', 'machine_ID', 
                    'event_type', 'user_name', 'scheduling_class', 'priority', 'resource_req_cpu', 
                    'resource_req_ram', 'resource_req_disk', 'constraint']
task_column_dict = {column: i for i, column in enumerate(task_column_list)}

# Start recording the execution time
start = timer()

# Convert the rows from string to list of values
job_entries = rdd_jobs.map(lambda x : x.split(','))
task_entries = rdd_tasks.map(lambda x : x.split(','))

# Since we do the same analysis for both jobs and tasks, we will combine their scheduling_class data
# Mapping to keep only the scheduling_class column
jobs_data = rdd_jobs.map(lambda row: row[job_column_dict['scheduling_class']])
tasks_data = rdd_tasks.map(lambda row: row[task_column_dict['scheduling_class']])

# Combine the data
combined_entries = jobs_data.union(tasks_data)

combined_entries.cache()

# Map the entries into (scheduling_class, 1) pairs
mapped_entries = combined_entries.map(lambda row: (row[0], 1))

counted_entries = mapped_entries.reduceByKey(lambda row1, row2: row1 + row2)

for scheduling_class, count in counted_entries.collect():
    print(f"scheduling class: {scheduling_class}, count: {count}")

# End recording the execution time
end = timer()
print(f"\nExecution time: {end - start}")

# prevent the program from terminating immediately
input("\nPress Enter to continue...")