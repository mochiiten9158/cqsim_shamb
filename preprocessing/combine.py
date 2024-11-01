import pandas as pd

# Define column names for the SWF format
swf_columns = [
    'id', 'submit', 'wait', 'run', 'used_proc', 'used_ave_cpu',
    'used_mem', 'req_proc', 'req_time', 'req_mem', 'status',
    'cluster_id', 'cluster_job_id', 'num_exe', 'num_queue',
    'num_part', 'num_pre', 'think_time'
]

def load_swf_as_dataframe(filename, source):
    """Loads an SWF file into a Pandas DataFrame."""
    data = []
    with open(filename, 'r') as file:
        for line in file:
            if line.startswith(";") or not line.strip():  # Skip comments and empty lines
                continue
            fields = line.strip().split()
            if len(fields) == 18:
                data.append([
                    int(fields[0]),    # job ID
                    int(fields[1]),    # submit time
                    int(fields[2]),    # wait time
                    int(fields[3]),    # run time
                    int(fields[4]),    # used processors
                    int(fields[5]),  # average CPU usage
                    int(fields[6]),    # used memory
                    int(fields[7]),    # requested processors
                    int(fields[8]),    # requested time
                    int(fields[9]),    # requested memory
                    int(fields[10]),   # status
                    source,            # cluster ID (mapped to Polaris or Theta)
                    int(fields[0]),    # cluster job ID
                    int(fields[13]),   # num executed
                    int(fields[14]),   # gpu used
                    int(fields[15]),   # num partitioned
                    int(fields[16]),   # num preempted
                    int(fields[17])    # think time
                ])
    return pd.DataFrame(data, columns=swf_columns)

def combine_and_sort_swf_files(polaris_filename, theta_filename, output_filename):
    """Combines and sorts two SWF files using Pandas DataFrames."""
    # Load both SWF files into dataframes
    polaris_df = load_swf_as_dataframe(polaris_filename, '1')
    theta_df = load_swf_as_dataframe(theta_filename, '0')

    # Concatenate the dataframes
    combined_df = pd.concat([polaris_df, theta_df])

    # Sort by the 'submit' column
    combined_df = combined_df.sort_values(by='submit').reset_index(drop=True)

    # Assign new IDs (1-based indexing)
    combined_df['id'] = combined_df.index + 1

    # Write the combined and sorted dataframe to the output SWF file
    with open(output_filename, 'w') as output_file:
        for _, row in combined_df.iterrows():
            output_file.write(
                f"{row['id']} {row['submit']} {row['wait']} {row['run']} {row['used_proc']} "
                f"{row['used_ave_cpu']} {row['used_mem']} {row['req_proc']} {row['req_time']} "
                f"{row['req_mem']} {row['status']} {row['cluster_id']} {row['cluster_job_id']} "
                f"{row['num_exe']} {row['num_queue']} {row['num_part']} {row['num_pre']} {row['think_time']}\n"
            )

# Usage example
#polaris_filename = r"data\InputFiles\theta_2023.swf"
#theta_filename = r"data\InputFiles\theta_2024.swf"
#output_filename = r"C:\Users\shamb\Documents\cqsimplus\data\InputFiles\theta_23_24.swf"

#polaris_filename = r"data\InputFiles\polaris_2023.swf"
#theta_filename = r"data\InputFiles\polaris_2024.swf"
#output_filename = r"C:\Users\shamb\Documents\cqsimplus\data\InputFiles\polaris_23_24.swf"

polaris_filename = r"data\InputFiles\polaris_23_24.swf"
theta_filename = r"data\InputFiles\theta_23_24.swf"
output_filename = r"C:\Users\shamb\Documents\cqsimplus\data\InputFiles\polaris_theta_23_24.swf"

combine_and_sort_swf_files(polaris_filename, theta_filename, output_filename)