from collections import Iterable
from .scheduler import Scheduler
from shleeh.errors import *


class Manager(object):
    """The class to allocate command to workers instance and schedule the job.

    This class take input from user and initiate Worker instance with it.
    The initiated worker instances will be queued on Scheduler after executing 'schedule' method.

    Notes:
        Combining with miresi module, the command can be executed into remote host.

    Examples:
        The example of scheduling repeated command

        >>> import paralexe as pe
        >>> mng = pe.Manager()
        >>> mng.set_cmd('touch *[file_name]')
        >>> mng.set_arg(label='file_name', args=['a.txt', 'b.txt', 'c.txt', 'd.txt']
        >>> mng.submit()

    Args:
        scheduler: Scheduler instance.
        client (optional): The client instance of miresi module.

    Attributes:
        client: Place holder for client instance, if it allocated, execution will be performed on remote server.
        n_workers (int): number of the worker will be allocated for the managing job.
        cmd (str): the command with the place holder.
        args (:obj:'dict'): the arguments for command.
        meta (:obj:'dict'): the meta information for followup the workers.
        decorator (:obj:'list' of :obj:'str'): decorator for place holder in command.
    """
    def __init__(self, client=None):
        # private
        self._client = client
        self._args = dict()
        self._meta = dict()
        self._schd = None
        self._cmd = None
        self._decorator = ['*[', ']']
        self._n_workers = 0
        self._errterm = None
        self._workers = None

        # public
        self.deployed = False

    # methods
    def set_cmd(self, cmd):
        """Set the command with place holder encapsulated with decorator.
        Args:
            cmd (str): Command
        """
        self._cmd = cmd

    def set_errterm(self, error_term):
        """This method set the term for indicating error condition from stderr
        """
        if isinstance(error_term, list):
            self._errterm = error_term
        elif isinstance(error_term, str):
            self._errterm = [error_term]
        else:
            raise TypeError

    def set_arg(self, label, args, update_meta=False):
        """Set arguments will replace the decorated place holder in command.
        The number of workers will have same number with the length of argument,
        which means worker will execute the command with each argument by
        number of arguments given here.

        Notes:
            If once multiple arguments set, following arguments need to have same length
            with prior arguments.

        Args:
            label (str): label of place holder want to be replaced with given argument.
            args (:obj:'list' of :obj:'str'): the list of arguments,
                        total length of the argument must same as number of workers.
            update_meta (bool): Update meta information for this argument if True, else do not update.

        Raises:
            Exception: will be raised if the command is not set prior executing this method.
        """
        if self._cmd is None:
            # the cmd property need to be defined prior to run this method.
            raise InvalidApproach

        # inspect the integrity of input argument.
        self._args[label] = self._inspection(args)

        # update arguments to correct numbers.
        for k, v in self._args.items():
            if not isinstance(v, list):
                self._args[k] = [v] * self._n_workers
            else:
                self._args[k] = v

            # update meta information.
            for i, arg in enumerate(self._args[k]):
                if update_meta is True:
                    self.meta[i] = {k: arg}
                else:
                    self.meta[i] = None

    def deploy_jobs(self):
        self.deployed = True
        return JobAllocator(self).allocation()

    def schedule(self, scheduler=None, priority=None, label=None, n_thread=None):
        """Schedule the jobs regarding the command user set to this Manager object.
        To execute the command, the Scheduler object that is linked, need to submit the jobs.

        Notes:
            Please refer the example in the docstring of the class to prevent conflict.

        Args:
            scheduler
            priority(int):  if given, schedule the jobs with given priority. lower the prior.
            label(str)      if given, use the label to index each step instead priority.
            n_thread
        """
        if scheduler is None:
            self._schd = Scheduler(n_threads=n_thread)
        elif isinstance(scheduler, Scheduler):
            self._schd = scheduler
        else:
            raise InvalidApproach
        self._workers = self.deploy_jobs()
        self._schd.queue(self._workers, priority=priority, label=label)

    def submit(self, mode='foreground', use_label=False):
        if self._schd is not None:
            self._schd.submit(mode=mode, use_label=use_label)

    def _inspection(self, args):
        """Inspect the integrity of the input arguments.
        This method mainly check the constancy of the number of arguments
        and the data type of given argument. Hidden method for internal using.

        Args:
            args (:obj:'list' of :obj:'str'): original arguments.

        Returns:
            args: same as input if arguments are passed inspection

        Raises:
            Exception: raised if the input argument cannot passed this inspection
        """

        # function to check single argument case
        def if_single_arg(arg):
            """If not single argument, raise error
            only allows single value with the type such as
            string, integer, or float.
            """
            if isinstance(arg, Iterable):
                if not isinstance(arg, str):
                    raise InvalidApproach

        # If there is no preset argument
        if len(self._args.keys()) == 0:
            # list dtype
            if isinstance(args, list):
                self._n_workers = len(args)

            # single value
            else:
                # Only single value can be assign as argument if it is not list object
                if_single_arg(args)
                self._n_workers = 1
            return args

        # If there were any preset argument
        else:
            # is single argument, single argument is allowed.
            if not isinstance(args, list):
                if_single_arg(args)
                return args
            else:
                # filter only list arguments.
                num_args = [len(a) for a in self._args.values() if isinstance(a, list)]

                # check all arguments as same length
                if not all([n == max(num_args) for n in num_args]):
                    # the number of each preset argument is different.
                    raise InvalidApproach

                # the number of arguments are same as others preset
                if len(args) != max(num_args):
                    raise InvalidApproach
                else:
                    self._n_workers = len(args)
                    return args

    def audit(self):
        if self.deployed:
            msg = []
            for w in self._workers:
                msg.append('WorkerID-{}'.format(w.id))
                msg.append('  Command: "{}"'.format(w.cmd))
                try:
                    stdout = '\n   '.join(w.output[0]) if isinstance(w.output[0], list) else None
                    stderr = '\n   '.join(w.output[1]) if isinstance(w.output[1], list) else None
                    msg.append('  ReturnCode: {}'.format(w.rcode))
                    msg.append('  stdout:\n    {}\n  stderr:\n    {}\n'.format(stdout, stderr))
                except TypeError:
                    msg.append('  *[ Scheduled job is not processed yet. ]\n')
                except:
                    raise UnexpectedError
            if len(msg) == 0:
                print('*[ No workers deployed. ]*')
            print('\n'.join(msg))

    # properties
    @property
    def meta(self):
        return self._meta

    @property
    def n_workers(self):
        return self._n_workers

    @property
    def cmd(self):
        return self._cmd

    @property
    def args(self):
        return self._args

    @property
    def errterm(self):
        return self._errterm

    @property
    def decorator(self):
        return self._decorator

    @decorator.setter
    def decorator(self, decorator):
        """Set decorator for parsing position of each argument

        Args:
            decorator (list):

        Raises:
            Exception
        """
        if decorator is not None:
            # inspect decorator datatype
            if isinstance(decorator, list) and len(decorator) == 2:
                self._decorator = decorator
            else:
                raise Exception

    @property
    def client(self):
        return self._client

    @property
    def schd(self):
        return self._schd

    def summary(self):
        return self.schd.info()

    def __repr__(self):
        return 'Deployed Workers:[{}]{}'.format(self._n_workers,
                                                '::Submitted' if self._schd.submitted else '')


class JobAllocator(object):
    """The helper class for the Manager object.

    This class will allocate the list of executable command into Workers.
    During the allocation, it also replaces the place holder with given set of arguments.
    Notes:
        The class is designed to be used for back-end only.

    Args:
        manager (:obj:'Manager'): Manager object
    """

    def __init__(self, manager):
        self._mng = manager

    def _convert_cmd_and_retrieve_placeholder(self, command):
        """Hidden method to retrieve name of place holder from the command"""
        import re
        prefix, suffix = self._mng.decorator
        raw_prefix = ''.join([r'\{}'.format(c) for c in prefix])
        raw_suffix = ''.join([r'\{}'.format(c) for c in suffix])

        # The text
        p = re.compile(r"{0}[^{0}{1}]+{1}".format(raw_prefix, raw_suffix))
        place_holders = set([obj[len(prefix):-len(suffix)] for obj in p.findall(command)])

        p = re.compile(r"{}({}){}".format(raw_prefix, '|'.join(place_holders), raw_suffix))
        new_command = p.sub(r'{\1}', command)

        return new_command, place_holders

    def _get_cmdlist(self):
        """Hidden method to generate list of command need to be executed by Workers"""

        args = self._mng.args
        cmd, place_holders = self._convert_cmd_and_retrieve_placeholder(self._mng.cmd)
        self._inspection_cmd(args, place_holders)
        cmds = dict()
        for i in range(self._mng.n_workers):
            cmds[i] = cmd.format(**{p: args[p][i] for p in place_holders})
        return cmds

    @staticmethod
    def _inspection_cmd(args, place_holders):
        """Hidden method to inspect command.
        There is the chance that the place holder user provided is not match with label
        in argument, this method check the integrity of the given relationship between cmd and args
        """
        if set(args.keys()) != place_holders:
            raise KeyError

    def allocation(self):
        """Method to allocate workers and return the list of worker"""
        from .executor import Executor
        from .worker import Worker

        cmds = self._get_cmdlist()
        list_of_workers = []
        for i, cmd in cmds.items():
            list_of_workers.append(Worker(id=i,
                                          executor=Executor(cmd, self._mng.client),
                                          meta=self._mng.meta[i],
                                          error_term=self._mng.errterm))
        return list_of_workers


class FuncManager(object):
    def __init__(self):
        # private
        self._schd = None
        self._args = dict()
        self._func = None
        self._n_workers = 0
        self._workers = None

        # public
        self.deployed = False

    def set_func(self, func):
        self._func = func

    def set_arg(self, label, args):
        if self._func is None:
            # the cmd property need to be defined prior to run this method.
            raise InvalidApproach

        # inspect the integrity of input argument.
        argv, need_multiply_by_num_worker = self._inspection(args)
        if need_multiply_by_num_worker:
            self._args[label] = [argv] * self._n_workers
        else:
            self._args[label] = argv

    def _inspection(self, args):
        # TODO: this will convert all to string, need to be corrected.
        # function to check single argument case
        def if_single_arg(arg):
            if isinstance(arg, Iterable):
                if not isinstance(arg, str):
                    raise InvalidApproach

        # If there is no preset argument
        if len(self._args.keys()) == 0:
            # this will be input dataset
            # list dtype
            if isinstance(args, list):
                self._n_workers = len(args)

            # single value
            else:
                # Only single value can be assign as argument if it is not list object
                if_single_arg(args)
                self._n_workers = 1
            return args, False

        # If there were any preset argument
        else:
            # is single argument, single argument is allowed.
            if not isinstance(args, list):
                if_single_arg(args)
                return args, True
            else:
                # filter only list arguments.
                num_args = [len(a) for a in self._args.values() if isinstance(a, list)]

                # check all arguments as same length
                if not all([n == max(num_args) for n in num_args]):
                    # the number of each preset argument is different.
                    raise InvalidApproach

                # the number of arguments are same as others preset
                if len(args) != max(num_args):
                    # list type argument
                    return args, True
                else:
                    self._n_workers = len(args)
                    return args, False

    def deploy_jobs(self):
        self.deployed = True
        return FuncAllocator(self).allocation()

    def schedule(self, scheduler=None, priority=None, label=None, n_thread=None):
        from .scheduler import Scheduler
        if scheduler is None:
            self._schd = Scheduler(n_threads=n_thread)
        elif isinstance(scheduler, Scheduler):
            self._schd = scheduler
        else:
            raise TypeError
        self._workers = self.deploy_jobs()
        self._schd.queue(self._workers, priority=priority, label=label)

    def submit(self, mode='foreground', use_label=False):
        if self._schd is not None:
            self._schd.submit(mode=mode, use_label=use_label)

    def audit(self):
        if self.deployed:
            msg = []
            for w in self._workers:
                msg.append('WorkerID-{}'.format(w.id))
                msg.append('  Func: "{}"'.format(w.func))
                try:
                    stdout = '\n   '.join(w.output[0]) if isinstance(w.output[0], list) else None
                    stderr = '\n   '.join(w.output[1]) if isinstance(w.output[1], list) else None
                    msg.append('  ReturnCode: {}'.format(w.rcode))
                    msg.append('  stdout:\n    {}\n  stderr:\n    {}\n'.format(stdout, stderr))
                except TypeError:
                    msg.append('  *[ Scheduled job is not processed yet. ]\n')
                except:
                    raise UnexpectedError
            if len(msg) == 0:
                print('*[ No workers deployed. ]*')
            print('\n'.join(msg))

    @property
    def n_workers(self):
        return self._n_workers

    @property
    def func(self):
        return self._func.__code__.co_name

    @property
    def args(self):
        return self._args

    @property
    def schd(self):
        return self._schd

    def summary(self):
        return self.schd.info()

    def __repr__(self):
        return 'Deployed Workers:[{}]{}'.format(self._n_workers, '::Submitted' if self._schd.submitted else '')


class FuncAllocator(object):
    def __init__(self, manager):
        self._mng = manager

    @staticmethod
    def _inspection_func(args, keywords):
        if set(args.keys()) != set(keywords):
            raise KeyError(f'{set(args.keys()), set(keywords)}')

    def _get_kwargslist(self):
        args = self._mng.args
        n_args = self._mng._func.__code__.co_argcount
        keywords = self._mng._func.__code__.co_varnames[:n_args]
        keywords = [k for k in keywords if k not in ['stdout', 'stderr']]
        self._inspection_func(args, keywords)
        kwargs = dict()
        for i in range(self._mng.n_workers):
            kwargs[i] = {k: args[k][i] for k in keywords}
        return kwargs

    def allocation(self):
        from .worker import FuncWorker
        kwargs = self._get_kwargslist()

        list_of_workers = []
        for i, k in kwargs.items():
            list_of_workers.append(FuncWorker(id=i,
                                              funcobj=self._mng._func,
                                              kwargs=k))
        return list_of_workers
