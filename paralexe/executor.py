import psutil
import shlex
from sys import platform
from subprocess import PIPE, Popen
from io import BytesIO


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
    _wildcards = "?*%$#"

    def __init__(self, cmd, client=None, shell=False):
        self._cmd       = cmd
        self._client    = client
        self._shell = shell
        self._interface = None
        self._rcode     = None
        self._proc      = None
        self.exit_code  = None

        self._stdout    = None
        self._stderr    = None
        self._interface = psutil if client is None else self._client.open_interface()

    @property
    def execute(self):
        # for backward compatibility
        return self.run

    def run(self):
        # If client obj is not input, use subprocess
        if self._client is None:
            try:
                if any(w in self._cmd for w in self._wildcards) or self._shell:
                    proc = Popen(self._cmd, stdout=PIPE, stderr=PIPE, shell=True)
                else:
                    if platform == 'win32':
                        proc = Popen(self._cmd, stdout=PIPE, stderr=PIPE, shell=True)
                    else:
                        proc = Popen(shlex.split(self._cmd), stdout=PIPE, stderr=PIPE)
                (stdout, stderr)  = proc.communicate()

                self._stdout = BytesIO(stdout)
                self._stderr = BytesIO(stderr)
                self._rcode = proc.returncode

            except OSError as e:
                self._stdout = BytesIO(''.encode('ascii'))
                self._stderr = BytesIO(e.strerror.encode('ascii'))
                self._rcode = e.errno

        # If client obj is input, use remote process instead
        else: # TODO: remote client execution is not working
            from .rsubprocess import Ropen
            self._proc = Ropen(self._cmd,
                               client=self._client)
            self._stdout = self._proc.stdout
            self._stderr = self._proc.stderr
            self._rcode = self._proc.returncode

        if self._rcode is None: # TODO: for debugging
            raise Exception

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
        return None

    @property
    def stdout(self):
        """stdout, ByteIO object"""
        return self._stdout

    @property
    def stderr(self):
        """stderr, ByteIO object"""
        return self._stderr

    @property
    def rcode(self):
        return self._rcode

    @property
    def pid(self):
        return self.proc.pid


if __name__ == '__main__':
    # testing
    import sys
    cmd = 'dir' if sys.platform == 'win32' else 'ls'

    print('Executing command:: "{}"\n'.format(cmd))
    ex = Executor(cmd, shell=True)
    ex.run()

    print('STDOUT::')
    for o in ex.stdout.read().decode('UTF-8').split('\n'):
        print(o)
    print('STDERR::')
    for e in ex.stderr.read().decode('UTF-8').split('\n'):
        print(e)

    print('Return code:: {}'.format(ex.rcode))