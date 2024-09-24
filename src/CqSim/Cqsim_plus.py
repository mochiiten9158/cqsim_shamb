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


    def __init__(self) -> None:
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
        """
        
        self.monitor = 500
        self.sims = []
        self.line_counters=[]
        self.end_flags = []
        self.sim_names = []
        self.sim_modules = []
        self.sim_procs = []
        self.exp_directory = f'../data/Results/exp_{get_random_name()}'
        self.traces = {}
        self.disable_child_stdout = False


    def check_sim_ended(self, id):
        """
        Checks if the simulator with given id has ended.
        """
        return self.end_flags[id]
    

    def check_all_sim_ended(self, ids):
        """
        Checks if all the simulators with given ids have ended.

        Returns false, if even one of the simulators havent finished.
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
        Advances the simulator with given id by one line in the job file.

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
        parent_conn, child_conn = Pipe()

        p = Process(target=self._run_on_child, args=(id, child_conn,))
        p.start()
        child_conn.close()
        result_file_lines = []
        while True:
            try:
                msg = parent_conn.recv()
                result_file_lines.append(msg)
            except EOFError:  # Child closed the connection
                break
        p.join()
        parent_conn.close()
        return result_file_lines
    
    def _run_on_child(self, id, conn):

        # Modify the job module so that no new jobs are read.
        job_module = self.sim_modules[id].module['job']
        job_module.update_max_lines(self.line_counters[id])

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



    def line_step_run_on(self, id):
        """
        Advances the simulator with given id by one line in the job file. 
        Then in a separete child process, runs the simulation until the end
        without reading the next jobs.

        For now the child's stdout is directed to a file. 
        See _line_step_run_on() helper for more details.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        None
        """
        parent_conn, child_conn = Pipe()

        p = Process(target=self._line_step_run_on_child, args=(id, child_conn,))
        p.start()
        child_conn.close()
        result_file_lines = []
        while True:
            try:
                msg = parent_conn.recv()
                result_file_lines.append(msg)
            except EOFError:  # Child closed the connection
                break
        p.join()
        parent_conn.close()
        return result_file_lines
    
    def line_step_run_on_fork_based(self, id):
        """
        Advances the simulator with given id by one line in the job file. 
        Then in a separete child process, runs the simulation until the end
        without reading the next jobs.

        For now the child's stdout is directed to a file. 
        See _line_step_run_on() helper for more details.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

        Returns
        -------
        None
        """
        parent_conn, child_conn = Pipe()

        # p = Process(target=self._line_step_run_on_child, args=(id, child_conn,))
        # p.start()
        # child_conn.close()
        # result_file_lines = []
        # while True:
        #     try:
        #         msg = parent_conn.recv()
        #         result_file_lines.append(msg)
        #     except EOFError:  # Child closed the connection
        #         break
        # p.join()
        # parent_conn.close()
        # return result_file_lines
    
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
        Helper to run a certain cqsim instance in a child process.
        This allows running a copy of an existing simulator, but
        with modified inputs at certain time steps.

        Parameters
        ----------
        id : int
            id of a cqsim instance stored in self.sims

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


    def set_job_run_scale_factor(self, id, scale_factor):
        """
        For a certain simulator, this sets the factor by which the job runtime
        should scaled.

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
        should scaled.

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
        job_module = self.sim_modules[id].module['job']
        job_module.disable_job(self.line_counters[id])

    
    def enable_next_job(self, id):
        job_module = self.sim_modules[id].module['job']
        job_module.enable_job(self.line_counters[id])
    
    def get_mask(self, id):
        return self.sim_modules[id].module['job'].mask
    
    def set_mask(self, id, mask):
        self.sim_modules[id].module['job'].mask = mask
    
    def get_line_number(self, id):
        return self.sim_modules[id].module['job'].line_number
    
    def get_results(self, id):
        output_module = self.sim_modules[id].module['output']
        return output_module.results