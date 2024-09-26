import os
import sys
import time
from multiprocessing import Process, Pipe
from unique_names_generator import get_random_name
import json
from types import SimpleNamespace
import os


import IOModule.Log_print as Log_print
import CqSim.Cqsim_sim as Class_Cqsim_sim
import cqsim_path
import IOModule.Debug_log as Class_Debug_log
import IOModule.Output_log as Class_Output_log

import CqSim.Job_trace as Class_Job_trace
#import CqSim.Node_struc as Class_Node_struc
import CqSim.Backfill as Class_Backfill
import CqSim.Start_window as Class_Start_window
import CqSim.Basic_algorithm as Class_Basic_algorithm
import CqSim.Info_collect as Class_Info_collect
import CqSim.Cqsim_sim as Class_Cqsim_sim

import Extend.SWF.Filter_job_SWF as filter_job_ext
import Extend.SWF.Filter_node_SWF as filter_node_ext
import Extend.SWF.Node_struc_SWF as node_struc_ext
__metaclass__ = type

class Cqsim_plus:
    """
    CQsim plus

    Class for CQSim plus core features.
    """


    def __init__(self, tag = None) -> None:
        """Initialize CQSim plus.

        Args:

        Attributes:
            monitor: interval for monitor event. (default: 500)
            sims: List of CQSim generator instances.
            line_counters: List of line counts in the job file for each cqsim instance.
            end_flags: List of end flags for each cqsim insance, denothing whether the simuation has ended.
            sim_names: List of names for each cqsim instance.
            sim_modules: List of CQSim modules for each cqsim instance.
            exp_directory: The directory for output files for all simulators.
            traces: A map from trace paths to simulator ids, prevents the parsing of a trace that was already parsed.
            disable_child_stdout: Flag to disable the stdout of the child. (default: False)
            tag: A string used to create output folder under ../data/Results/
        """
        
        self.monitor = 500
        self.sims = []
        self.line_counters=[]
        self.end_flags = []
        self.sim_names = []
        self.sim_modules = []
        self.sim_procs = []
        # TODO: For now, each cqsim instance's IO folder is given a random name
        self.exp_directory = f'../data/Results/exp_{get_random_name()}'
        self.traces = {}
        self.disable_child_stdout = False
        self.tag = tag
        if self.tag != None:
            self.exp_directory = f'../data/Results/{self.tag}'

    def set_sim_times(self, id, real_start_time, virtual_start_time):
        job_module = self.sim_modules[id].module['job']
        job_module.real_start_time = real_start_time
        job_module.virtual_start_time = virtual_start_time


    def check_sim_ended(self, id):
        """
        Checks if the simulator with given id has ended.
        """
        return self.end_flags[id]


    def check_all_sim_ended(self, ids):
        """
        Checks if all the simulators with given ids have ended.

        Returns true, when all simulators have finished.
        """
        result = True
        for id in ids:
            result = result and self.end_flags[id]
        return result


    def get_job_data(self, trace_dir, trace_file):
        """
        Get the job data from some trace.

        Parameters
        ----------
        trace_dir : str
            A path to the directory where the trace file is located.
        trace_file : str
            The trace file name to read.

        Returns
        -------
        job_ids : lits[int]
            List of job ids.
        job_procs : list[int]
            List of processes requested for each job.
        """
        module_debug = Class_Debug_log.Debug_log(
            lvl=0,
            show=0,
            path= f'/dev/null',
            log_freq=1
        )
        module_debug.disable()
        save_name_j = f'/dev/null'
        config_name_j = f'/dev/null'
        module_filter_job = filter_job_ext.Filter_job_SWF(
            trace=f'{trace_dir}/{trace_file}', 
            save=save_name_j, 
            config=config_name_j, 
            debug=module_debug
        )
        module_filter_job.feed_job_trace()
        module_filter_job.output_job_config()

        job_ids = module_filter_job.job_ids
        job_procs = module_filter_job.job_procs

        return job_ids, job_procs


    def single_cqsim(self, trace_dir, trace_file, proc_count):
        """
        Sets up a single cqsim instance.

        Parameters
        ----------
        trace_dir : str
            A path to the directory where the trace file is located.
        trace_file : str
            The trace file name to read.
        proc_count: int
            The amount of processes for the simualted cluster.

        Returns
        -------
        sim_id : int
            An integer id of the newly created cqsim instance.
        """
        sim_name = get_random_name()
        sim_id = len(self.sims)

        output_dir = f'{self.exp_directory}/Results'
        debug_dir = f'{self.exp_directory}/Debug'
        fmt_dir = f'{self.exp_directory}/Fmt'

        for dir in [output_dir, debug_dir, fmt_dir]:
            if not os.path.exists(dir):
                os.makedirs(dir)

        trace_name = trace_file.split('.')[0]

        output_sys_file = f'{trace_name}_{sim_id}.ult'
        output_adapt_file = f'{trace_name}_{sim_id}.adp'
        output_result_file = f'{trace_name}_{sim_id}.rst'

        # Debug module
        debug_log = f'{trace_name}_{sim_id}_debug.log'
        module_debug = Class_Debug_log.Debug_log(
            lvl=3,
            show=2,
            path= f'{debug_dir}/{debug_log}',
            log_freq=1
        )
        self.module_debug = module_debug

        # Filter SWF module -- If needed
        fmt_job_file = f'{trace_name}_{sim_id}.csv'
        fmt_job_config_file = f'{trace_name}_{sim_id}.con'
        fmt_node_file = f'{trace_name}_{sim_id}_node.csv'
        fmt_node_config_file = f'{trace_name}_{sim_id}_node.con'
        
        # Avoid parsing SWF traces that have been parsed before by another sim
        if f'{trace_dir}/{trace_file}' in self.traces:
            temp_sim_id = self.traces[f'{trace_dir}/{trace_file}']
            fmt_job_file = f'{trace_name}_{temp_sim_id}.csv'
            fmt_job_config_file = f'{trace_name}_{temp_sim_id}.con'
            fmt_node_file = f'{trace_name}_{temp_sim_id}_node.csv'
            fmt_node_config_file = f'{trace_name}_{temp_sim_id}_node.con'
        # Parse SWF file
        else:
            module_filter_job = filter_job_ext.Filter_job_SWF(
                trace=f'{trace_dir}/{trace_file}', 
                save=f'{fmt_dir}/{fmt_job_file}', 
                config=f'{fmt_dir}/{fmt_job_config_file}', 
                debug=module_debug
            )
            module_filter_job.feed_job_trace()
            module_filter_job.output_job_config()

            module_filter_node = filter_node_ext.Filter_node_SWF(
                struc=f'{trace_dir}/{trace_file}',
                save=f'{fmt_dir}/{fmt_node_file}', 
                config=f'{fmt_dir}/{fmt_node_config_file}', 
                debug=module_debug
            )
            module_filter_node.static_node_struc(proc_count)
            module_filter_node.output_node_data()
            module_filter_node.output_node_config()

        # Job trace module
        module_job_trace = Class_Job_trace.Job_trace(
            job_file_path=f'{fmt_dir}/{fmt_job_file}',
            debug=module_debug,
            real_start_time=0,
            virtual_start_time=0,
            max_lines=1000
        )
        module_job_trace.import_job_config(f'{fmt_dir}/{fmt_job_config_file}')

        # Node structure module
        module_node_struc = node_struc_ext.Node_struc_SWF(debug=module_debug)
        module_node_struc.import_node_file(f'{fmt_dir}/{fmt_node_file}')
        module_node_struc.import_node_config(f'{fmt_dir}/{fmt_node_config_file}')

        # Backfill module
        module_backfill = Class_Backfill.Backfill(
            mode=2,
            node_module=module_node_struc,
            debug=module_debug,
            para_list=None
        )

        # Start window module
        module_win = Class_Start_window.Start_window(
            mode=5,
            node_module=module_node_struc,
            debug=module_debug,
            para_list=['5', '0', '0'],
            para_list_ad=None)

        # Basic alg module
        module_alg = Class_Basic_algorithm.Basic_algorithm (
            element=[['w', '+', '2'],[1, 0, 1]],
            debug=module_debug,
            para_list=None
        )

        # Info collect module
        module_info_collect = Class_Info_collect.Info_collect (
            alg_module=module_alg,debug=module_debug)

        # Output module
        module_output_log = Class_Output_log.Output_log (
            output = {
                'sys':f'{output_dir}/{output_sys_file}', 
                'adapt':f'{output_dir}/{output_adapt_file}', 
                'result':f'{output_dir}/{output_result_file}'
            },
            log_freq=1
        )

        module_list = {
            'debug':module_debug,
            'job':module_job_trace,
            'node':module_node_struc,
            'backfill':module_backfill,
            'win':module_win,
            'alg':module_alg,
            'info':module_info_collect,
            'output':module_output_log
        }
    
        # CQSim module
        module_sim = Class_Cqsim_sim.Cqsim_sim(
            module=module_list, 
            debug=module_debug, 
            monitor = 500
        )

        # Get the generator object
        cqsim = module_sim.cqsim_sim()

        # Book keeping
        self.sims.append(cqsim)
        self.line_counters.append(0)
        self.end_flags.append(False)
        self.sim_names.append(sim_name)
        self.sim_modules.append(module_sim)
        self.traces[f'{trace_dir}/{trace_file}'] = sim_id
        self.sim_procs.append(proc_count)

        return sim_id


    def line_step(self, id) -> None:
        """
        Advances a certain simulator with given id by one line in the job file.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        None
        """
        try:
            next(self.sims[id])
            self.line_counters[id] += 1
        except StopIteration:
            self.end_flags[id] = True


    def run_on(self, id):
        """
        Creates a copy of the simulator with given id, and
        runs it to completion without new job input.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        results: list[str]
           Job results of the copied simulator.

        """
        parent_conn, child_conn = Pipe()

        p = Process(target=self._run_on_child, args=(id, child_conn,))
        p.start()
        child_conn.close()
        results = []
        while True:
            try:
                msg = parent_conn.recv()
                results.append(msg)
            except EOFError:  # Child closed the connection
                break
        p.join()
        parent_conn.close()
        return results


    def _run_on_child(self, id, conn):
        """
        This function is a helper for run_on(). The function is run inside a 
        child process which contains the copy of a simulator. The copied
        simulator is run to completion without new job input.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims
        
        conn:
            piped connection for the child to send results back to parent process.

        Returns
        -------
        None
        """
        
        # Get the modules
        job_module = self.sim_modules[id].module['job']
        debug_module = self.sim_modules[id].module['debug']
        output_module = self.sim_modules[id].module['output']

        # Modify the job module so that no new jobs are read.
        job_module.update_max_lines(self.line_counters[id])

        # Disable outputs of debug, log and output modules.
        debug_module.disable()
        output_module.disable()

        if self.disable_child_stdout:
            with open(os.devnull, 'w') as sys.stdout:
                while not self.check_sim_ended(id):
                    self.line_step(id)
        else:
            with open(f'runon_{self.line_counters[id]}.txt', 'w') as sys.stdout:
                while not self.check_sim_ended(id):
                    self.line_step(id)
        output_module.send_result_to_pipe(conn)
        conn.close()


    def line_step_run_on(self, id):
        """
        Creates a copy of the simulator with given id. The copied
        simulator is advanced by one step in the job file then run
        to completion withtout any new jobs.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        results: list[str]
           Job results of the copied simulator.
        """
        parent_conn, child_conn = Pipe()

        p = Process(target=self._line_step_run_on_child, args=(id, child_conn,))
        p.start()
        child_conn.close()
        results = []
        while True:
            try:
                msg = parent_conn.recv()
                results.append(msg)
            except EOFError:  # Child closed the connection
                break
        p.join()
        parent_conn.close()
        return results


    def line_step_run_on_fork_based(self, id):
        """
        Same as line_step_run_on(), but used tradiation fork() instead
        of multiprocessing.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        results: list[str]
           Job results of the copied simulator.
        """
        parent_conn, child_conn = Pipe()
        pid = os.fork()

        if pid == 0:  # Child process
            parent_conn.close()  # Close the parent's end in the child
            self._line_step_run_on_child(id, child_conn)
            os._exit(0)  # Ensure clean exit from child

        else:  # Parent process
            child_conn.close()  # Close the child's end in the parent
            result_file_lines = []
            while True:
                try:
                    msg = parent_conn.recv()
                    result_file_lines.append(msg)
                except EOFError:
                    break

            _, status = os.waitpid(pid, 0)  # Wait for child to finish
            parent_conn.close()
            return result_file_lines


    def _line_step_run_on_child(self, id, conn):
        """
        This function is a helper for line_step_run_on(). The function is run 
        inside a child process which contains the copy of a simulator. The copied
        simulator is advanced by one job then run to completion without any new
        job input.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims
        
        conn:
            piped connection for the child to send results back to parent process.

        Returns
        -------
        None
        """

        # Modify the job module so that no new jobs are read.
        job_module = self.sim_modules[id].module['job']
        job_module.update_max_lines(self.line_counters[id] + 1)

        # Disable outputs of debug, log and output modules.
        debug_module = self.sim_modules[id].module['debug']
        output_module = self.sim_modules[id].module['output']
        debug_module.disable()
        output_module.disable()

        if self.disable_child_stdout:
            with open(os.devnull, 'w') as sys.stdout:
                while not self.check_sim_ended(id):
                    self.line_step(id)
        else:
            with open(f'runon_{self.line_counters[id]}.txt', 'w') as sys.stdout:
                while not self.check_sim_ended(id):
                    self.line_step(id)
        output_module.send_result_to_pipe(conn)
        conn.close()


    def set_max_lines(self, id, max_lines):
        """
        For a certain simulator, set the max_lines parameter in the 
        job module. The max_line parameters controls the maximum
        number of lines to read from the job file.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims.
        
        max_lines: int
            Maximum number of lines to read from the job file.

        Returns
        -------
        None
        """
        job_module = self.sim_modules[id].module['job']
        job_module.update_max_lines(max_lines)


    def set_job_run_scale_factor(self, id, scale_factor):
        """
        For a certain simulator, this sets the factor by which the job runtime
        should be scaled.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims
        
        scale_factor: float
            The factor the scale the job runtimes by

        Returns
        -------
        None
        """
        job_module = self.sim_modules[id].module['job']
        job_module.job_runtime_scale_factor = scale_factor


    def set_job_walltime_scale_factor(self, id, scale_factor):
        """
        For a certain simulator, this sets the factor by which the job walltime
        should be scaled.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims
        
        scale_factor: float
            The factor the scale the job runtimes by

        Returns
        -------
        None
        """
        job_module = self.sim_modules[id].module['job']
        job_module.job_walltime_scale_factor = scale_factor
    
    def disable_next_job(self, id):
        """
        For a certain simulator, this sets the mask to 0 for the next
        job.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        """
        job_module = self.sim_modules[id].module['job']
        job_module.disable_job(self.line_counters[id])

    
    def enable_next_job(self, id):
        """
        For a certain simulator, this sets the mask to 1 for the next
        job.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        """
        job_module = self.sim_modules[id].module['job']
        job_module.enable_job(self.line_counters[id])
    
    def get_job_file_mask(self, id):
        """
        Get the job file mask for some simulator.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims.

        Returns
        -------
        mask: list[int]
            A list of 0s and 1s per line in the job file.
        """
        mask: list[int] = self.sim_modules[id].module['job'].mask
        return mask
    
    def set_job_file_mask(self, id, mask):
        """
        For a certain simulator, this sets the mask to 1 for the next.
        job.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims.

        mask: list[int]
            A list of 0s and 1s per line in the job file.

        Returns
        -------
        """
        self.sim_modules[id].module['job'].mask = mask

    
    def get_job_results(self, id):
        """
        For a certain simulator, get job relted results.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims.

        Returns
        -------
        results: list[str]
           For a certain simulator, this returns the results from 
           the output module. The same can be found in the .rst file.

        """
        output_module = self.sim_modules[id].module['output']
        results: list[str] = output_module.results
        return results
    
    def get_job_submits(self, trace_dir, trace_file):
        module_debug = Class_Debug_log.Debug_log(
            lvl=0,
            show=0,
            path= f'/dev/null',
            log_freq=1
        )
        module_debug.disable()
        save_name_j = f'/dev/null'
        config_name_j = f'/dev/null'
        module_filter_job = filter_job_ext.Filter_job_SWF(
            trace=f'{trace_dir}/{trace_file}', 
            save=save_name_j, 
            config=config_name_j, 
            debug=module_debug
        )
        module_filter_job.feed_job_trace()
        module_filter_job.output_job_config()

        job_submits = module_filter_job.job_submits
        return job_submits
    

    def print_results(self, id):
        output_module = self.sim_modules[id].module['output']
        output_module.print_saved_results()