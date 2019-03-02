class Worker(object):
    """The helper class to execute command and store related meta information.

    The object created using this will interface between Executor class and Scheduler class.
    Basically, Manager object allocate the command to this worker object and queued into
    Schedule instance. Once it executed, it stores the stdout or stderr to 'output' attribute
    based on whether the execution was success or not.

    Examples:
        Below code will run 'ls -al' at the command shell, and store the results on 'worker.output'.
        Also user can store the meta information 'describe' of worker using 'worker.meta['describe']'.

        >>> from paralexe import Executor, Worker
        >>> cmd = 'ls -al'
        >>> exc = Executor(cmd)
        >>> worker = Worker(id=0, executor=exc, meta=dict(describe='Get list of files in current folder'))
        >>> worker.run()

    Args:
        id (int): identifier code for worker instance.
        executor (:obj:'Executor'): Executor instance with command.

    Attributes:
        executor: place holder for executor object
        id (int): place holder for identifier code
        cmd (str): command that allocated to Executor instance
        meta (:obj:'dict' of :obj:'str'): place holder for meta information
        output (:obj:'list' of :obj':'list'): place holder to store stdout or stderr
    """
    def __init__(self, id, executor, meta=None):
        self._id = id
        self._meta = meta
        self._executor = executor
        self._cmd = executor.cmd
        self.__output = None

    def run(self):
        """Execute the command and store the output

        Returns: 1 if stderr occurs, else 0
        """
        self._executor.execute()

        stdout = self.__pars_output(self._executor.stdout.read())
        stderr = self.__pars_output(self._executor.stderr.read())
        self.__output = (stdout, stderr)
        if stderr is not None:
            checked_stderr = ['ERROR' in err for err in stderr]
            if any(checked_stderr):
                return 1
            else:
                return 0
        else:
            return 0

    def __pars_output(self, bytedata):
        """Decode byte to utf-8 for stdout"""
        if len(bytedata) is 0:
            return None
        else:
            output = bytedata.decode('utf-8').split('\n')
            return [o for o in output if len(o) > 0]

    @property
    def id(self):
        return self._id

    @property
    def executor(self):
        return self._executor

    @property
    def cmd(self):
        return self._cmd

    @property
    def meta(self):
        return self._meta

    @property
    def output(self):
        return self.__output
