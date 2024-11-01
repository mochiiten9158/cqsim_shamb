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
    'num_queue',      #15 Check if gpu used or not
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

# Function to convert the CSV file to the desired format
def csv_to_custom_format(csv_filename, output_folder, output_filename):
    job_list = []

    # Ensure the output folder exists, if not, create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Construct the full output file path
    full_output_path = os.path.join(output_folder, output_filename)
    
    with open(csv_filename, 'r') as csv_file, open(full_output_path, 'w') as output_file:
        reader = csv.DictReader(csv_file)

        id = 1
        for row in reader:
            # Extract the necessary fields from the CSV
            index = (row["JOB_NAME"]).split('.')[0]
            submit = convert_to_unix(row["QUEUED_TIMESTAMP"])
            wait = (convert_to_unix(row["START_TIMESTAMP"])) - (convert_to_unix(row["QUEUED_TIMESTAMP"]))
            runtime = (convert_to_unix(row["END_TIMESTAMP"])) - (convert_to_unix(row["START_TIMESTAMP"]))
            walltime = int(float(row["WALLTIME_SECONDS"]))
            used_proc = int(float(row["NODES_USED"]))
            req_proc = int(float(row["NODES_REQUESTED"]))
            type = (row["QUEUE_NAME"])
            cluster_id = 0
            num_queue = 0
            # Build the job info dictionary based on the provided template
            # sep 1 2023 0:00:00 1693544400
            # sep 1 2024 0:00:00 1725166800
            if (type == 'debug-cache-quad' or type == 'debug-flat-quad') or submit < 1693544400 or req_proc > 4360 or req_proc == 0 or runtime == 0:
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
csv_filename = r"preprocessing\data\ANL-ALCF-DJC-THETA_20230101_20231231.csv"
#csv_filename = r"preprocessing\data\ANL-ALCF-DJC-THETA_20240101_20240831.csv"
output_path = r"C:\Users\shamb\Documents\cqsimplus\data\InputFiles"
output_filename = 'theta_2023.swf'

csv_to_custom_format(csv_filename, output_path, output_filename)