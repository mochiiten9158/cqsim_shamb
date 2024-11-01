"""
swf_columns = [
    'id',             #1 
    'submit',         #2
    'wait',           #3
    'run',            #4
    'used_proc',      #5
    'used_ave_cpu',   #6
    'used_mem',       #7
    'req_proc',       #8
    'req_time',       #9
    'req_mem',        #10 
    'status',         #11
    'cluster_id',     #12 Changed from user_id to cluster_id
    'cluster_job_id', #13 Changed from group_id to cluster_job_id
    'num_exe',        #14
    'num_queue',      #15
    'num_part',       #16
    'num_pre',        #17
    'think_time',     #18
    ]"""

import csv
from datetime import datetime
import os

# Function to convert timestamp to Unix time
def convert_to_unix(timestamp_str):
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    dt = datetime.strptime(timestamp_str, timestamp_format)
    return int(dt.timestamp())

# Function to read GPU job IDs from the GPU file
def read_gpu_jobs(gpu_jobs_file):
    gpu_job_ids = set()
    with open(gpu_jobs_file, 'r') as gpu_file:
        reader = csv.reader(gpu_file, delimiter=',')
        for row in reader:
            if row[7] == 'used':
                gpu_job_ids.add(row[5])
            else:
                continue
    return gpu_job_ids

# Function to convert the CSV file to the desired format
def csv_to_custom_format(csv_filename, output_folder, output_filename, gpu_jobs_file):
    gpu_job_ids = read_gpu_jobs(gpu_jobs_file)  # Store GPU job IDs in a set
    job_list = []

    # Ensure the output folder exists, if not, create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Construct the full output file path
    full_output_path = os.path.join(output_folder, output_filename)
    
    with open(csv_filename, 'r') as csv_file, open(full_output_path, 'w') as output_file, open(gpu_jobs, 'r') as gpu_file:
        reader = csv.DictReader(csv_file)
        id = 1
        for row in reader:
            # Extract the necessary fields from the CSV
            # print(row['START_TIMESTAMP'])
            index = (row["JOB_NAME"]).split('.')[0]
            if '[' in index:
                index = index.split('[')[0]
            submit = convert_to_unix(row["QUEUED_TIMESTAMP"])
            start = convert_to_unix(row['START_TIMESTAMP'])
            wait = (convert_to_unix(row["START_TIMESTAMP"])) - (convert_to_unix(row["QUEUED_TIMESTAMP"]))
            runtime = (convert_to_unix(row["END_TIMESTAMP"])) - (convert_to_unix(row["START_TIMESTAMP"]))
            walltime = int(float(row["WALLTIME_SECONDS"]))
            used_proc = int(float(row["NODES_USED"]))
            req_proc = int(float(row["NODES_REQUESTED"]))
            type = (row["QUEUE_NAME"])
            cluster_id = 1
            if index in gpu_job_ids:
                num_queue = 1
            else:
                num_queue = 0

            # Build the job info dictionary based on the provided template
            # sep 1 2023 0:00:00 1693544400
            # sep 1 2024 0:00:00 1725166800
            if (type == 'debug' or type == 'debug-scaling') and req_proc <= 8:
                continue
            if submit > 1725166800 or start < 1693544400 or submit < 1693544400 or req_proc >= 552 or req_proc == 0 or runtime == 0:
                continue
            else:
                temp_info = {
                    'index': id,
                    'submit': submit,
                    'wait': wait,
                    'walltime': walltime,
                    'runtime': runtime,
                    'usedProc': used_proc,
                    'reqProc': req_proc,
                    'type': type,
                    'cluster_id' : cluster_id,
                    'num_queue' : num_queue
                }

                # Append to the job list
                job_list.append(temp_info)
                id += 1
            
        # Output to a file
        for job in job_list:
            output_file.write((str)(job['index']) + " " +
                                (str)(job['submit'])+ " " +
                                (str)(job['wait']) + " " +
                                (str)(job['runtime']) + " " +
                                (str)(job['usedProc']) + " " +
                                "-1" + " " +
                                "-1" + " " +
                                (str)(job['reqProc']) + " " +                             
                                (str)(job['walltime']) + " " +
                                "-1" + " " +
                                "0" + " " + 
                                (str)(job['cluster_id']) + " " + 
                                "-1" + " " +
                                "-1" + " " +
                                (str)(job['num_queue']) + " " +
                                "-1" + " " +
                                "-1" + " " +
                                "-1" + " " + "\n")

# Usage example
#csv_filename = r"preprocessing\data\ANL-ALCF-DJC-POLARIS_20230101_20231231.csv"
csv_filename = r"preprocessing\data\ANL-ALCF-DJC-POLARIS_20240101_20240831.csv"
gpu_jobs = r"preprocessing\data\job_summary_used_gpus.csv"

output_path = r"C:\Users\shamb\Documents\cqsimplus\data\InputFiles"
output_filename = 'polaris_2024.swf'

csv_to_custom_format(csv_filename, output_path, output_filename, gpu_jobs)