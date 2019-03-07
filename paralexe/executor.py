class Executor(object):
    """Executor class, the object to run command hand interface with subprocess
    Helper class for Worker to execute command.

    The major function of this class.
    1. Execute given command, if client is given execute on remote server
        (client: remote client of miresi module)
    2. Provide the methods to interface with STDIN STDOUT STDERR, and check PID and running state

    Todo:
        Update docstrings. Best practice are made on Manager class
    """

    def __init__(self, cmd, client=None):
        self._cmd = cmd
        self._client = client
        self._interface = None
        self._proc = None
        if client is None:
            import psutil
            self._interface = psutil
        else:
            self._interface = self._client.open_interface()

    def execute(self):
        # If client obj is not input, use subprocess
        if self._client is None:
            from subprocess import PIPE, Popen
            import shlex
            self._proc = Popen(shlex.split(self._cmd),
                               # stdin=PIPE,   # Executor not use stdin, activate later when it becomes available
                               stdin=None,
                               stdout=PIPE,
                               stderr=PIPE)

            self._proc.communicate()
        # If client obj is input, use remote process instead
        else:
            from .rsubprocess import Ropen
            self._proc = Ropen(self._cmd,
                               client=self._client)


    @property
    def client(self):
        return self._client

    @property
    def cmd(self):
        return self._cmd

    @property
    def proc(self):
        """Popen object place holder"""
        return self._proc

    def get_stat(self):
        """
        Returns: True if running
        """
        return self._interface.pid_exists(self._proc.pid)

    @property
    def stdin(self):
        """stdin, will always return None"""
        return self.proc.stdin

    @property
    def stdout(self):
        """stdout, PIPE object"""
        return self.proc.stdout

    @property
    def stderr(self):
        """stderr, PIPE object"""
        return self.proc.stderr

    @property
    def pid(self):
        return self.proc.pid
