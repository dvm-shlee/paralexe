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

    # methods
    def set_cmd(self, cmd):
        """Set the command with place holder encapsulated with decorator.

        Args:
            cmd (str): Command
        """
        self.__cmd = cmd

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
        return JobAllocator(self).allocation()

    def schedule(self, scheduler, priority=None, label=None):
        """Schedule the jobs regarding the command user set to this Manager object.
        To execute the command, the Scheduler object that is linked, need to submit the jobs.

        Notes:
            Please refer the example in the docstring of the class to prevent conflict.

        Args:
            priority(int):  if given, schedule the jobs with given priority. lower the prior.
            label(str)      if given, use the label to index each step instead priority.
        """
        workers = self.deploy_jobs()
        scheduler.queue(workers, priority=priority, label=label)

    # hidden methods for internal processing
    def __init_attributed(self):
        # All attributes are twice underbarred in order to not show up.
        self.__args = dict()
        self.__meta = dict()
        self.__cmd = None
        self.__decorator = ['*[', ']']
        self.__n_workers = 0
        self.__client = None

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
                                          meta=self._mng.meta[i]))
        return list_of_workers
