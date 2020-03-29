class Worker(object):
    """The helper class to execute command and store related meta information.

    The object created using this will interface between Executor class and Scheduler class.
    Basically, Manager object allocate the command to this worker object and queued into
    Scheduler instance. Once it executed, it stores the stdout or stderr to 'output' attribute
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
    def __init__(self, id, executor, meta=None, error_term=None):
        self._id = id
        self._meta = meta
        self._executor = executor
        self._cmd = executor.cmd
        self.__output = None
        self._error_term = error_term
        self._rcode = None

    def run(self):
        """Execute the command and store the output

        Returns: 1 if stderr occurs, else 0
        """
        exct = self._executor
        exct.execute()

        stdout = self.__pars_output(exct.stdout.read())
        stderr = self.__pars_output(exct.stderr.read())
        self.__output = (stdout, stderr)
        self._rcode = exct.rcode

        def check_error(line):
            if any(e.lower() in line.lower() for e in self._error_term):
                return True
            else:
                return False

        if stderr is not None:
            if self._error_term is not None:
                if any([check_error(err) for err in stderr]):
                    return 1
                else:
                    self._rcode = 0
                    self.__output = (stderr, stdout)
                    return 0

        if self._rcode > 0:
            return 1
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


class FuncWorker(object):
    def __init__(self, id, funcobj, kwargs):
        self._id = id
        self._kwargs = kwargs
        self._func = funcobj

        from io import StringIO
        self.__stdout = StringIO()
        self.__stderr = StringIO()
        self.__output = None
        self._rcode = None

    def run(self):
        try:
            self._rcode = self._func(stdout=self.__stdout,
                                     stderr=self.__stderr,
                                     **self._kwargs)
        except Exception as e:
            self.__stderr.write(str(e))
            self._rcode = 1

        stdout = self.__pars_output(self.__stdout.getvalue())
        stderr = self.__pars_output(self.__stderr.getvalue())
        self.__output = (stdout, stderr)
        return self._rcode

    def __pars_output(self, string):
        if len(string) is 0:
            return None
        else:
            output = string.split('\n')
            return [o for o in output if len(o) > 0]

    @property
    def func(self):
        return self._func.__code__.co_name

    @property
    def id(self):
        return self._id

    @property
    def output(self):
        return self.__output

    @property
    def rcode(self):
        return self._rcode