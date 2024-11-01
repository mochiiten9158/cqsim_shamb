"""
Polaris and Theta experiments
"""
import datetime
from CqSim.Cqsim_plus import Cqsim_plus
from tqdm.auto import tqdm
from utils import probabilistic_true, disable_print
import pandas as pd
import random
import multiprocessing
from trace_utils import read_job_data_swf


def exp_theta(tqdm_pos, tqdm_lock):
    """
    Experiment Theta

    Simulates Theta 2023 jobs on Theta

    """
    tag = f'exp_theta_2023'
    trace_dir = '../data/InputFiles'
    trace_file = 'theta_polaris_2023.swf'
    cluster_proc = 4360


    cqp = Cqsim_plus(tag = tag)
    

    job_ids, job_procs, job_submits, cluster_ids, gpu_req = cqp.get_job_data(trace_dir, trace_file, parsed_trace=False)


    sim = cqp.single_cqsim(
        trace_dir = trace_dir, 
        trace_file = trace_file, 
        proc_count= cluster_proc, 
        parsed_trace=False,
        sim_tag='theta')

    # Configure sims to read all jobs
    cqp.set_max_lines(sim, len(job_ids))
    cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)
    cqp.disable_debug_module(sim)


    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for _ in job_ids:
        with disable_print():
            cqp.line_step(sim, write_results=True)

        with tqdm_lock:
            bar.update(1)

    while not cqp.check_sim_ended(sim):
        with disable_print():
            cqp.line_step(sim, write_results=True)

    with tqdm_lock:
        bar.close()

    return {
        "theta" : cqp.get_job_results(sim)
    }

def exp_polaris(tqdm_pos, tqdm_lock):
    """
    Experiment Polaris

    Simulates Polaris 2023 jobs on Polaris
    """
    tag = f'exp_polaris_2023'
    trace_dir = '../data/InputFiles'
    trace_file = 'theta_polaris_2023.swf'
    cluster_proc = 552


    cqp = Cqsim_plus(tag = tag)
    

    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=False)


    sim = cqp.single_cqsim(
        trace_dir = trace_dir, 
        trace_file = trace_file, 
        proc_count= cluster_proc, 
        parsed_trace=False,
        sim_tag='polaris')

    # Configure sims to read all jobs
    cqp.set_max_lines(sim, len(job_ids))
    cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)


    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for _ in job_ids:
        with disable_print():
            cqp.line_step(sim, write_results=True)

        with tqdm_lock:
            bar.update(1)

    while not cqp.check_sim_ended(sim):
        with disable_print():
            cqp.line_step(sim, write_results=True)

    with tqdm_lock:
        bar.close()

    return {
        "polaris" : cqp.get_job_results(sim)
    }

'''def exp_polaris_theta_random(tqdm_pos, tqdm_lock):
    """
    Theta and Polaris Metascheduled using random allocation
    """
    tag = f'polaris_theta_random'
    trace_dir = '../data/InputFiles/theta_polaris_2023'
    trace_file = 'polaris_theta_2023.swf'
    theta_proc = 4360
    polaris_proc = 552


    cqp = Cqsim_plus(tag = tag)
    cqp.disable_child_stdout = True

    # Cluster 1 is Theta
    theta = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=theta_proc,
        parsed_trace=False,
        sim_tag='theta')
    

    # Cluster 2 is Polaris
    polaris = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=polaris_proc,
        parsed_trace=False,
        sim_tag='polaris')

    sims = [theta, polaris]


    # Get job stats
    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=False)
    swf = read_job_data_swf(trace_dir, trace_file)
    job_gpus = swf['is_gpu'].to_list()


    # Configure sims to read all jobs
    for sim in sims:
        cqp.set_max_lines(sim, len(job_ids))
        cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)
        cqp.disable_debug_module(sim)

    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for i in range(len(job_ids)):

        if job_gpus[i] == 1:
            selected_sim = polaris
        else:
            # turnarounds = {}
            clusters = []
            
            # Check if the job can be run.
            for sim in sims:
                if job_procs[i] > cqp.sim_procs[sim]:
                    continue
                clusters.append(sim)

            # If none of the clusters could run, skip the job.
            assert(len(clusters) != 0)
            # if len(turnarounds) == 0:
            #     for sim in sims:
            #         cqp.disable_next_job(sim)
            #         with disable_print():
            #             cqp.line_step(sim, write_results=True)
            #     continue
            

            selected_sim = random.choice(clusters)

        # Add the job to the appropriate cluster and continue main simulation.
        for sim in sims:
            if sim == selected_sim:
                cqp.enable_next_job(sim)            
            else:
                cqp.disable_next_job(sim)
            with disable_print():
                cqp.line_step(sim, write_results=True)
        
        with tqdm_lock:
            bar.update(1)

    with tqdm_lock:
        bar.close()

    # Run all the simulations until complete.
    while not cqp.check_all_sim_ended(sims):
        for sim_id in sims:
            with disable_print():
                cqp.line_step(sim_id, write_results=True)

    return {
        "theta" : cqp.get_job_results(sims[0]),
        "polaris" : cqp.get_job_results(sims[1])
    }
'''

def exp_polaris_theta_opt_turn(tqdm_pos, tqdm_lock):
    """
    Theta and Polaris Metascheduled using OPT turnaround
    0 1693545511 6 22 10 -1 -1 10 3600 -1 0 1 0 -1 0 -1 -1 -1
    """
    now = datetime.datetime.now()
    formatted_date_time = now.strftime("%m_%d_%H_%M")
    master_exp_directory = f'../data/Results/exp_polaris_theta/'

    trace_dir = '../data/InputFiles'
    trace_file = 'theta_polaris_23_24.swf'
    theta_proc = 4360
    polaris_proc = 552

    tag = f'polaris_theta_opt_turn'
    cqp = Cqsim_plus()
    exp_out = f'{master_exp_directory}'
    cqp.set_exp_directory(exp_out)

    # Cluster 1 is Theta
    theta = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=theta_proc,
        parsed_trace=False,
        sim_tag='theta')
    

    # Cluster 2 is Polaris
    polaris = cqp.single_cqsim(
        trace_dir, 
        trace_file, 
        proc_count=polaris_proc,
        parsed_trace=False,
        sim_tag='polaris')

    sims = [theta, polaris]

    # Get job stats
    job_ids, job_procs, job_submits = cqp.get_job_data(trace_dir, trace_file, parsed_trace=False)
    cluster_ids, gpu_req = cqp.get_miscellaneous_data(trace_dir, trace_file, parsed_trace=False)
    #swf = read_job_data_swf(trace_dir, trace_file)
    #job_gpus = swf['is_gpu'].to_list()
    print(trace_dir)
    # Configure sims to read all jobs
    for sim in sims:
        cqp.set_max_lines(sim, len(job_ids))
        cqp.set_sim_times(sim, real_start_time=job_submits[0], virtual_start_time=0)
        cqp.disable_debug_module(sim)

    tqdm_text = tag
    with tqdm_lock:
        bar = tqdm(
            desc=tqdm_text,
            total=len(job_ids),
            position=tqdm_pos,
            leave=False)

    for i in range(len(job_ids)):

        if gpu_req[i] == 1:
            selected_sim = polaris
        else:
            turnarounds = {}
            # First simulate the new job on both clusters.
            # for sim in sims:

            #     # Check if the job can be run.
            #     if job_procs[i] > cqp.sim_procs[sim]:
            #         continue
                
            #     # Run the simulation for only upto the next job.
            #     results = cqp.line_step_run_on(sim)

            #     # Parse the results
            #     presults = [result.split(';') for result in results]
            #     df = pd.DataFrame(presults, columns = ['id', 'reqProc', 'reqProc2', 'walltime', 'run', 'wait', 'submit', 'start', 'end']) 
            #     df = df.astype(float)
        
            #     # Get the results for the job we just simulated
            #     last_job_results = df.loc[df['id'] == job_ids[i]]

            #     # Get the turnaround of the latest job.
            #     last_job_turnaround = last_job_results['end'] - last_job_results['submit']
            #     turnarounds[sim] = last_job_turnaround.item()

            if cluster_ids[i] == 1:
                cqp.set_job_run_scale_factor(sims[0], 1.0*4.0)
                cqp.set_job_walltime_scale_factor(sims[0], 1.0*4.0)

            elif cluster_ids[i] == 0:
                cqp.set_job_run_scale_factor(sims[1], 1.0/4.0)
                cqp.set_job_walltime_scale_factor(sims[1], 1.0/4.0)

            turnarounds = cqp.predict_next_job_turnarounds(sims, job_ids[i], job_procs[i])

            #Reset
            cqp.set_job_run_scale_factor(sims[0], 1.0)
            cqp.set_job_walltime_scale_factor(sims[0], 1.0)
            cqp.set_job_run_scale_factor(sims[1], 1.0)
            cqp.set_job_walltime_scale_factor(sims[1], 1.0)

            # If none of the clusters could run, skip the job.
            assert(len(turnarounds) != 0)
            # if len(turnarounds) == 0:
            #     for sim in sims:
            #         cqp.disable_next_job(sim)
            #         with disable_print():
            #             cqp.line_step(sim, write_results=True)
            #     continue
            

            # Get the cluster with the lowest turnaround.
            lowest_turnaround = min(turnarounds.values())
            sims_with_lowest_turnaround = [key for key, value in turnarounds.items() if value == lowest_turnaround]

            selected_sim = random.choice(sims_with_lowest_turnaround)

        # Add the job to the appropriate cluster and continue main simulation.
        for sim in sims:
            if sim == selected_sim:
                    if sim == sims[0] and cluster_ids[i] == 1:
                        cqp.set_job_run_scale_factor(sim, 1.0*4.0)
                        cqp.set_job_walltime_scale_factor(sim, 1.0*4.0)    
                    elif sim == sims[1] and cluster_ids[i] == 0:
                        cqp.set_job_run_scale_factor(sim, 1.0/4.0)
                        cqp.set_job_walltime_scale_factor(sim, 1.0/4.0)
                    cqp.enable_next_job(sim)         
            else:
                cqp.disable_next_job(sim)
            with disable_print():
                cqp.line_step(sim, write_results=True)
        
        cqp.set_job_run_scale_factor(sims[0], 1.0)
        cqp.set_job_walltime_scale_factor(sims[0], 1.0)
        cqp.set_job_run_scale_factor(sims[1], 1.0)
        cqp.set_job_walltime_scale_factor(sims[1], 1.0)

        with tqdm_lock:
            bar.update(1)

    with tqdm_lock:
        bar.close()

    # Run all the simulations until complete.
    while not cqp.check_all_sim_ended(sims):
        for sim_id in sims:
            with disable_print():
                cqp.line_step(sim_id, write_results=True)

    return {
        "theta" : cqp.get_job_results(sims[0]),
        "polaris" : cqp.get_job_results(sims[1])
    }



if __name__ == '__main__':

    # create_theta_cori_traces('../data/InputFiles', )

    lock = multiprocessing.Manager().Lock()
    p = []

    # p.append(multiprocessing.Process(target=exp_theta, args=(1, lock,)))
    # p.append(multiprocessing.Process(target=exp_polaris, args=(2, lock,)))
    # p.append(multiprocessing.Process(target=exp_polaris_theta_opt_turn, args=(3, lock,)))
    # p.append(multiprocessing.Process(target=exp_theta_cori_opt_turn, args=(4, lock,)))

    for proc in p:
        proc.start()
    
    for proc in p:
        proc.join()

    #exp_polaris_theta_opt_turn(1, lock)

    import sys
    selector = int(sys.argv[1])


    if selector == 0:
#        # Just theta
        p.append(multiprocessing.Process(target=exp_theta, args=(1, lock,)))

    if selector == 1:
        # Just Polaris
        p.append(multiprocessing.Process(target=exp_polaris, args=(1, lock,)))

    if selector == 2:
        # Theta Polaris opt turn
        p.append(multiprocessing.Process(target=exp_polaris_theta_opt_turn, args=(1, lock,)))

    #if selector == 3:
        # Theta Polaris random
    #    p.append(multiprocessing.Process(target=exp_polaris_theta_random, args=(1, lock,)))


    for proc in p:
        proc.start()
    

    for proc in p:
        proc.join()