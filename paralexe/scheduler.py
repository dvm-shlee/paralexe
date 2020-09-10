from multiprocessing.pool import Pool, ThreadPool
from shleeh.errors import *
import time

try:
    from IPython import get_ipython
    if get_ipython() and len(get_ipython().config.keys()):
        notebook_env = True
    else:
        notebook_env = False
except ModuleNotFoundError:
    notebook_env = False

if notebook_env:
    from tqdm import tqdm_notebook as progressbar
    from IPython.display import display
else:
    from pprint import pprint as display
    from tqdm import tqdm as progressbar


class Scheduler(object):
    """The class to schedule multiple jobs.

    Args:
        workers (:obj:'dict' of :obj:'Worker', optional): list of the worker instances to be scheduled.
            if dict, key value indicate priority of the workers.
        n_threads (int, optional): Number of thread to use.

    Attributes:
        queues (dict): queued workers in dictionary form, the key value indicate priority.
        stderr (dict): collection of stderr from workers after execution.
        stdout (dict): collection of stdout from workers after execution.
    """
    def __init__(self, workers=None, n_threads=None, label=None):
        """
        Args:
            workers (dict or list): priority:list(workers)
            n_threads (int):
        """
        # Initiate counters
        self._reset_counter()
        self._queues = None
        self._queues_labels = dict()
        self._background_binder = None
        self._n_threads = n_threads
        self._submitted = False
        self._step_progressbar = None
        self._sub_progressbars = dict()
        self._Pool = None

        if workers is not None:
            self.queue(workers, label=label)

    @property
    def submitted(self):
        return self._submitted

    @property
    def labels(self):
        return sorted(self._queues_labels.keys())

    def submit(self, mode='foreground', use_label=False):
        """Submit schedule

        Args:
            mode (str): run process on Thread if 'background', else running foreground.
            use_label (bool)
        """
        self._submitted = True

        def workflow():
            """Internal function that submitting jobs to Thread object
            The outputs from each worker are collected during running this function"""
            if self._queues is not None:
                self._num_steps = len(self._queues)

                # initiate pool
                with self._Pool(self._n_threads) as pool:
                    for order in sorted(self._queues.keys()):
                        if order in self._succeeded_steps:
                            pass
                        else:
                            if use_label is True:
                                label = f'{self._queues_labels[order]}_{order}'
                            else:
                                label = order
                            # Initiate counters
                            self._incomplete_steps = []
                            self._failed_steps = []
                            self._succeeded_workers[order] = []
                            self._failed_workers[order] = []
                            self._stdout_collector[label] = dict()
                            self._stderr_collector[label] = dict()
                            n_work = len(self._queues[order])
                            self._total_num_of_workers[order] = n_work
                            workers = self._queues[order]

                            for idx, rcode, output in pool.imap_unordered(self.request, workers):
                                if rcode == 1:
                                    self._failed_workers[order].append(idx)
                                elif rcode == 0:
                                    self._succeeded_workers[order].append(idx)
                                else:
                                    import sys
                                    print('unidentified return code: {}'.format(rcode), file=sys.stderr)
                                    raise OSError
                                self._stdout_collector[label][idx] = output[0]
                                self._stderr_collector[label][idx] = output[1]

                            if self._succeeded_workers[order] == 0:
                                self._failed_steps.append(order)
                            else:
                                if self._total_num_of_workers[order] > len(self._succeeded_workers[order]):
                                    self._incomplete_steps.append(order)
                                else:
                                    self._succeeded_steps.append(order)

        # Pool will be staying on foreground
        if mode == 'foreground':
            workflow()

        # Pool will be staying on background
        elif mode == 'background':
            import threading
            self._background_binder = threading.Thread(target=workflow, args=())
            self._background_binder.daemon = True
            self._background_binder.start()

    @staticmethod
    def request(worker):
        """Method for requesting execution to worker

        Args:
            worker (obj): single Worker object

        Returns:
            id (int): identifier code for worker instances.
            rcode (int): return code 0 if request is executed, 1 if not.
            output (list): stdout or stderr.
        """
        # if output file is exist
        rcode = worker.run()
        output = worker.output
        return worker.id, rcode, output

    def is_alive(self):
        """Check if the process is still alive if it running on background"""
        if self._background_binder is not None:
            return self._background_binder.is_alive()
        else:
            return None

    def join(self):
        """If running on background, attract out to foreground"""
        if self.is_alive():
            self._background_binder.join()

    def check_progress(self):
        """Helper method to check overall progression"""
        self._step_progressbar = None
        self._sub_progressbars = dict()
        if self._queues is not None:
            total_bar = len(self._succeeded_steps) + len(self._failed_steps)
            sub_bars = dict()
            import sys
            self._step_progressbar = progressbar(total=self._num_steps, desc='__Total__',
                                                 initial=total_bar, postfix=None, position=0)
            for i, priority in enumerate(self._queues.keys()):
                if len(self._queues_labels) != 0:
                    label = 'Step::{}-{}'.format(self._queues_labels[priority], priority)
                else:
                    label = 'priority::{}'.format(str(priority + 1).zfill(3))
                if priority in self._succeeded_workers.keys():
                    sub_bar = len(self._succeeded_workers[priority])
                else:
                    sub_bar = 0
                self._sub_progressbars[priority] = progressbar(total=len(self._queues[priority]),
                                                               desc=label, initial=sub_bar, position=1+i)
                sub_bars[priority] = sub_bar

            def workon(n_finished_steps, n_sub_tasks):
                while n_finished_steps < self._num_steps:
                    cur_finished_steps = len(self._succeeded_steps) + len(self._failed_steps)
                    step_delta = cur_finished_steps - n_finished_steps
                    if step_delta > 0:
                        n_finished_steps += step_delta
                        self._step_progressbar.update(step_delta)
                    for p in self._queues.keys():
                        if p in self._succeeded_workers.keys():
                            sub_delta = len(self._succeeded_workers[p]) - n_sub_tasks[p]
                            if sub_delta > 0:
                                n_sub_tasks[p] += sub_delta
                                self._sub_progressbars[p].update(sub_delta)
                    time.sleep(0.2)
                self._step_progressbar.close()
                for p in self._queues.keys():
                    self._sub_progressbars[p].close()
            import threading
            thread = threading.Thread(target=workon, args=(total_bar, sub_bars))
            thread.daemon = True
            if notebook_env:
                display(self._step_progressbar)
                for p in self._queues.keys():
                    display(self._sub_progressbars[p])
                thread.start()
            else:
                thread.start()
        else:
            print('[No scheduled jobs]')

    def summary(self):
        if self._num_steps != 0:
            m = []
            zfill = len(str(self._num_steps))
            m.append('outline')
            m.append('\t** Summery')
            m.append('outline')
            m.append('Total number of steps:\t\t{}'.format(self._num_steps))
            if len(self._succeeded_steps) > 0:
                m.append('- Succeeded steps:\t\t{}'.format(len(self._succeeded_steps)))
            if len(self._incomplete_steps) > 0:
                m.append('- Incompleted steps:\t\t{}'.format(len(self._incomplete_steps)))
            if len(self._failed_steps) > 0:
                m.append('- Failed steps:\t\t\t{}'.format(len(self._failed_steps)))
            # for s in range(self.__num_steps):
            for s in self._queues.keys():
                if len(self._queues_labels) == 0:
                    label = 'Step:'.format(str(s + 1).zfill(zfill))
                else:
                    label = 'Step::{}'.format(self._queues_labels[s])
                if s in self._total_num_of_workers.keys():
                    m.append('space')
                    m.append('{}\n\tNumber of workers: \t{}'.format(label,
                                                                    self._total_num_of_workers[s]))
                    if len(self._succeeded_workers[s]) > 0:
                        m.append('\t- Succeeded workers: \t{}'.format(len(self._succeeded_workers[s])))
                    if len(self._failed_workers[s]) > 0:
                        m.append('\t- Failed workers: \t{}'.format(len(self._failed_workers[s])))
                else:
                    m.append('space')
                    m.append('{}\n- Not ready'.format(label))
            m.append('space')
            if self._background_binder is not None:
                if self.is_alive() is True:
                    state = '\tActive'
                else:
                    if len(self._succeeded_steps) == self._num_steps:
                        state = '\tFinished'
                    elif len(self._succeeded_steps) < self._num_steps:
                        state = []
                        if len(self._failed_steps) > 0:
                            state.append('\tFailed steps-{}'.format(self._failed_steps))
                        if len(self._incomplete_steps) > 0:
                            state.append('\tIncompleted steps-{}'.format(self._incomplete_steps))
                        state = ''.join(state)
                    else:
                        state = '\tSubmission needed'
                m.append('Status:\n{}'.format(state))
            else:
                if len(self._succeeded_steps) == self._num_steps:
                    state = '\tFinished'
                elif len(self._succeeded_steps) < self._num_steps:
                    state = []
                    if len(self._failed_steps) > 0:
                        state.append('\tFailed steps-{}'.format(self._failed_steps))
                    if len(self._incomplete_steps) > 0:
                        state.append('\tIncompleted steps-{}'.format(self._incomplete_steps))
                    state = ''.join(state)
                else:
                    state = '\tSubmission needed'
                m.append('Status:\n{}'.format(state))
            m.append('outline')

            spacer = '-' * (max(map(len, m)) + 1)
            outline = '=' * (max(map(len, m)) + 1)
            for i, line in enumerate(m):
                if line is 'space':
                    m[i] = spacer
                elif line is 'outline':
                    m[i] = outline
        else:
            m = ['Empty schedule...']
        print('\n'.join(m))

    def queue(self, workers, priority=None, label=None):
        """Queue the input list of workers"""

        # run inspection, if the input of worker instances is list, this step change it to dictionary.

        from .worker import Worker, FuncWorker
        if isinstance(workers[0], Worker):
            self._Pool = ThreadPool
        else:
            self._Pool = Pool

        if self._queues is None:
            self._queues = self._inspect_inputs(workers, priority=0, label=label)
        else:
            workers = self._inspect_inputs(workers, priority)
            self._update_queues(workers, label)

    def _inspect_inputs(self, workers, priority=None, label=None):
        """Inspect the integrity of the inputs.

        Args:
            workers (list or dict): The iterable set of pre-initiated Worker instances.
            priority (int): priority of the job of given workers.
            label (str): label

        Returns:
            workers (dict): corrected form of worker list.
        """
        label_index = None
        if label is not None:
            if label in self._queues_labels.values():
                for order, l in self._queues_labels.items():
                    if label is l:
                        label_index = order
            if label_index in self._succeeded_steps:
                return {}
            elif label_index in self._failed_steps or label_index in self._incomplete_steps:
                priority = label_index
                try:
                    self._failed_steps.remove(label_index)
                except ValueError:
                    pass
                except:
                    raise UnexpectedError
                try:
                    self._incomplete_steps.remove(label_index)
                except ValueError:
                    pass
                except:
                    raise UnexpectedError
            else:
                pass

        if priority is None:
            if self._queues is None:
                priority = 0
            else:
                priority = max(self._queues.keys()) + 1

        from .worker import Worker, FuncWorker

        # Check if the inputs has hierarchy works.
        if isinstance(workers, list):
            for ipt in workers:
                if not any([isinstance(ipt, Worker), isinstance(ipt, FuncWorker)]):
                    raise TypeError
            self._queues_labels[priority] = label
            return {priority: workers}

        # dictionary type of workers may have multiple workers list.
        elif isinstance(workers, dict):
            for p, w in workers.items():
                self._queues_labels[p] = label
            return workers
        else:
            raise TypeError

    def _update_queues(self, workers, label):
        """internal method to update workers into queue"""
        if len(workers) > 0:
            for priority, workers_in_priority in workers.items():
                if priority in self._queues.keys():
                    if isinstance(workers_in_priority, list):
                        self._queues[priority].extend(workers_in_priority)
                    else:
                        self._queues[priority].append(workers_in_priority)
                else:
                    self._queues[priority] = workers_in_priority
                    if label is not None:
                        self._queues_labels[priority] = label

    def _reset_counter(self):
        # counter for steps
        self._num_steps            = 0
        self._succeeded_steps      = []
        self._failed_steps         = []
        self._incomplete_steps     = []

        # counter for workers
        self._total_num_of_workers = {}
        self._failed_workers       = {}
        self._succeeded_workers    = {}

        # output collector
        self._stdout_collector     = {}
        self._stderr_collector     = {}

    @property
    def queues(self):
        return self._queues

    @property
    def stdout(self):
        return self._stdout_collector

    @property
    def stderr(self):
        return self._stderr_collector

    def __repr__(self):
        return 'Job Scheduling:{}::{}'.format(self._num_steps,
                                             'Success' if self._num_steps
                                             else 'Issued' if len(self._incomplete_steps) > 0
                                             else 'Incompleted')
