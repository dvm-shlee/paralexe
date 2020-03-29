from collections import Iterable


class Manager(object):
    """The class to allocate command to workers instance and schedule the job.

    This class take input from user and initiate Worker instance with it.
    The initiated worker instances will be queued on Scheduler after executing 'schedule' method.

    Notes:
        Combining with miresi module, the command can be executed into remote host.

    Examples:
        The example of scheduling repeated command

        >>> import paralexe as pe
        >>> sch = pe.Scheduler()
        >>> mng = pe.Manager()
        >>> mng.set_cmd('touch *[file_name]')
        >>> mng.set_arg(label='file_name', args=['a.txt', 'b.txt', 'c.txt', 'd.txt']
        >>> mng.schedule(sch)
        >>> sch.submit()

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

    Todo:
        Make the better descriptions for Errors and Exceptions.
    """
    def __init__(self, client=None):
        self.__init_attributed()
        self.__client = client
        self.__schd = None

    # methods
    def set_cmd(self, cmd):
        """Set the command with place holder encapsulated with decorator.

        Args:
            cmd (str): Command
        """
        self.__cmd = cmd

    def set_errterm(self, error_term):
        """This method set the term for indicating error condition from stderr
        """
        if isinstance(error_term, list):
            self.__errterm = error_term
        elif isinstance(error_term, str):
            self.__errterm = [error_term]
        else:
            raise TypeError

    def set_arg(self, label, args, metaupdate=False):
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
            metaupdate (bool): Update meta information for this argument if True, else do not update.

        Raises:
            Exception: will be raised if the command is not set prior executing this method.
        """
        if self.__cmd is None:
            # the cmd property need to be defined prior to run this method.
            raise Exception()

        # inspect the integrity of input argument.
        self.__args[label] = self.__inspection(args)

        # update arguments to correct numbers.
        for k, v in self.__args.items():
            if not isinstance(v, list):
                self.__args[k] = [v] * self.__n_workers
            else:
                self.__args[k] = v

            # update meta information.
            for i, arg in enumerate(self.__args[k]):
                if metaupdate is True:
                    self.meta[i] = {k:arg}
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
            priority(int):  if given, schedule the jobs with given priority. lower the prior.
            label(str)      if given, use the label to index each step instead priority.
        """
        from .scheduler import Scheduler
        if scheduler is None:
            self.__schd = Scheduler(n_threads=n_thread)
        elif isinstance(scheduler, Scheduler):
            self.__schd = scheduler
        else:
            raise Exception
        self._workers = self.deploy_jobs()
        self.__schd.queue(self._workers, priority=priority, label=label)

    def submit(self, mode='foreground', use_label=False):
        if self.__schd is not None:
            self.__schd.submit(mode=mode, use_label=use_label)

    # hidden methods for internal processing
    def __init_attributed(self):
        # All attributes are twice underbarred in order to not show up.
        self.__args = dict()
        self.__meta = dict()
        self.__cmd = None
        self.__decorator = ['*[', ']']
        self.__n_workers = 0
        self.__errterm = None
        self._workers = None
        self.deployed = False

    def __inspection(self, args):
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
                    raise Exception

        # If there is no preset argument
        if len(self.__args.keys()) == 0:
            # list dtype
            if isinstance(args, list):
                self.__n_workers = len(args)

            # single value
            else:
                # Only single value can be assign as argument if it is not list object
                if_single_arg(args)
                self.__n_workers = 1
            return args

        # If there were any preset argument
        else:
            # is single argument, single argument is allowed.
            if not isinstance(args, list):
                if_single_arg(args)
                return args
            else:
                # filter only list arguments.
                num_args = [len(a) for a in self.__args.values() if isinstance(a, list)]

                # check all arguments as same length
                if not all([n == max(num_args) for n in num_args]):
                    # the number of each preset argument is different.
                    raise Exception

                # the number of arguments are same as others preset
                if len(args) != max(num_args):
                    raise Exception
                else:
                    self.__n_workers = len(args)
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
                    msg.append('  ReturnCode: {}'.format(w._rcode))
                    msg.append('  stdout:\n    {}\n  stderr:\n    {}\n'.format(stdout, stderr))
                except:
                    msg.append('  *[ Scheduled job is not executed yet. ]\n')
            if len(msg) == 0:
                print('*[ No workers deployed. ]*')
            print('\n'.join(msg))

    # properties
    @property
    def meta(self):
        return self.__meta

    @property
    def n_workers(self):
        return self.__n_workers

    @property
    def cmd(self):
        return self.__cmd

    @property
    def args(self):
        return self.__args

    @property
    def errterm(self):
        return self.__errterm

    @property
    def decorator(self):
        return self.__decorator

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
                self.__decorator = decorator
            else:
                raise Exception

    @property
    def client(self):
        return self.__client

    @property
    def schd(self):
        return self.__schd

    def summary(self):
        return self.schd.summary()

    def __repr__(self):
        return 'Deployed Workers:[{}]{}'.format(self.__n_workers,
                                                '::Submitted' if self.__schd._submitted else '')


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

    def __convert_cmd_and_retrieve_placeholder(self, command):
        """Hidden method to retrieve name of place holder from the command"""
        import re
        prefix, surfix = self._mng.decorator
        raw_prefix = ''.join([r'\{}'.format(chr) for chr in prefix])
        raw_surfix = ''.join([r'\{}'.format(chr) for chr in surfix])

        # The text
        p = re.compile(r"{0}[^{0}{1}]+{1}".format(raw_prefix, raw_surfix))
        place_holders = set([obj[len(prefix):-len(surfix)] for obj in p.findall(command)])

        p = re.compile(r"{}({}){}".format(raw_prefix,'|'.join(place_holders), raw_surfix))
        new_command = p.sub(r'{\1}', command)

        return new_command, place_holders

    def __get_cmdlist(self):
        """Hidden method to generate list of command need to be executed by Workers"""

        args = self._mng.args
        cmd, place_holders = self.__convert_cmd_and_retrieve_placeholder(self._mng.cmd)
        self.__inspection_cmd(args, place_holders)
        cmds = dict()
        for i in range(self._mng.n_workers):
            cmds[i] = cmd.format(**{p: args[p][i] for p in place_holders})
        return cmds

    def __inspection_cmd(self, args, place_holders):
        """Hidden method to inspect command.
        There is the chance that the place holder user provided is not match with label
        in argument, this method check the integrity of the given relationship between cmd and args
        """
        if set(args.keys()) != place_holders:
            raise Exception

    def allocation(self):
        """Method to allocate workers and return the list of worker"""
        from .executor import Executor
        from .worker import Worker

        cmds = self.__get_cmdlist()
        list_of_workers = []
        for i, cmd in cmds.items():
            list_of_workers.append(Worker(id=i,
                                          executor=Executor(cmd, self._mng.client),
                                          meta=self._mng.meta[i],
                                          error_term=self._mng.errterm))
        return list_of_workers


class FuncManager(object):
    def __init__(self):
        self.__init_attributed()
        self.__schd = None

    def __init_attributed(self):
        # All attributes are twice underbarred in order to not show up.
        self.__args = dict()
        self.__func = None
        self.__n_workers = 0
        self._workers = None
        self.deployed = False

    def set_func(self, func):
        self._func = func

    def set_arg(self, label, args):
        if self._func is None:
            # the cmd property need to be defined prior to run this method.
            raise Exception()

        # inspect the integrity of input argument.
        self.__args[label] = self.__inspection(args)

        # update arguments to correct numbers.
        for k, v in self.__args.items():
            if not isinstance(v, list):
                self.__args[k] = [v] * self.__n_workers
            else:
                self.__args[k] = v

    def __inspection(self, args):
        # function to check single argument case
        def if_single_arg(arg):
            if isinstance(arg, Iterable):
                if not isinstance(arg, str):
                    raise Exception

        # If there is no preset argument
        if len(self.__args.keys()) == 0:
            # list dtype
            if isinstance(args, list):
                self.__n_workers = len(args)

            # single value
            else:
                # Only single value can be assign as argument if it is not list object
                if_single_arg(args)
                self.__n_workers = 1
            return args

        # If there were any preset argument
        else:
            # is single argument, single argument is allowed.
            if not isinstance(args, list):
                if_single_arg(args)
                return args
            else:
                # filter only list arguments.
                num_args = [len(a) for a in self.__args.values() if isinstance(a, list)]

                # check all arguments as same length
                if not all([n == max(num_args) for n in num_args]):
                    # the number of each preset argument is different.
                    raise Exception

                # the number of arguments are same as others preset
                if len(args) != max(num_args):
                    raise Exception
                else:
                    self.__n_workers = len(args)
                    return args

    def deploy_jobs(self):
        self.deployed = True
        return FuncAllocator(self).allocation()

    def schedule(self, scheduler=None, priority=None, label=None, n_thread=None):
        from .scheduler import Scheduler
        if scheduler is None:
            self.__schd = Scheduler(n_threads=n_thread)
        elif isinstance(scheduler, Scheduler):
            self.__schd = scheduler
        else:
            raise Exception
        self._workers = self.deploy_jobs()
        self.__schd.queue(self._workers, priority=priority, label=label)

    def submit(self, mode='foreground', use_label=False):
        if self.__schd is not None:
            self.__schd.submit(mode=mode, use_label=use_label)

    def audit(self):
        if self.deployed:
            msg = []
            for w in self._workers:
                msg.append('WorkerID-{}'.format(w.id))
                msg.append('  Func: "{}"'.format(w.func))
                try:
                    stdout = '\n   '.join(w.output[0]) if isinstance(w.output[0], list) else None
                    stderr = '\n   '.join(w.output[1]) if isinstance(w.output[1], list) else None
                    msg.append('  ReturnCode: {}'.format(w._rcode))
                    msg.append('  stdout:\n    {}\n  stderr:\n    {}\n'.format(stdout, stderr))
                except:
                    msg.append('  *[ Scheduled job is not executed yet. ]\n')
            if len(msg) == 0:
                print('*[ No workers deployed. ]*')
            print('\n'.join(msg))

    @property
    def n_workers(self):
        return self.__n_workers

    @property
    def func(self):
        return self._func.__code__.co_name

    @property
    def args(self):
        return self.__args

    @property
    def schd(self):
        return self.__schd

    def summary(self):
        return self.schd.summary()

    def __repr__(self):
        return 'Deployed Workers:[{}]{}'.format(self.__n_workers,
                                                '::Submitted' if self.__schd._submitted else '')


class FuncAllocator(object):
    def __init__(self, manager):
        self._mng = manager

    def __inspection_func(self, args, keywords):
        if set(args.keys()) != set(keywords):
            raise Exception

    def __get_kwargslist(self):
        args = self._mng.args
        n_args = self._mng._func.__code__.co_argcount
        keywords = self._mng._func.__code__.co_varnames[:n_args]
        keywords = [k for k in keywords if k not in ['stdout', 'stderr']]
        self.__inspection_func(args, keywords)
        kwargs = dict()
        for i in range(self._mng.n_workers):
            kwargs[i] = {k: args[k][i] for k in keywords}
        return kwargs

    def allocation(self):
        from .worker import FuncWorker
        kwargs = self.__get_kwargslist()

        list_of_workers = []
        for i, k in kwargs.items():
            list_of_workers.append(FuncWorker(id=i,
                                              funcobj=self._mng._func,
                                              kwargs=k))
        return list_of_workers