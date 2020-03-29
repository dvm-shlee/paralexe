import sys
from io import BytesIO
# if sys.version_info[0] == 3:
#     from io import BinaryIO
# else:
#     import StringIO. as StringIO


class Ropen(object):
    """Analog with Popen for remote subprocess
    To communicate with multiple remote computing platform, such as clustering scheduler,
    this class will provide the execution extension if needed.
    """
    def __init__(self, cmd, client, timeout=None, bufsize=-1):
        self.__client = client
        self.__cmd = cmd
        self.__timeout = timeout
        self.__bufsize = bufsize
        self.__pid = None

        if self.__client.mode == 'ssh':
            self._exec_by_ssh()
        else:
            pass

    def _exec_by_ssh(self):
        """through ssh execution, it forced to wait until the execution is done"""
        (rc1, stdout), (rc2, stderr) = self.__client.exec_command('echo $$; exec {}'.format(self.__cmd))
        pid = stdout.readline()
        self.__pid = int(pid)
        self._output_handler(stdout, stderr, rc1-len(pid), rc2)

    def _output_handler(self, stdout, stderr, stdout_size, stderr_size):
        self.__stdin = None
        self.__stdout = stdout
        self.__stderr = stderr
        if stderr_size > 0 and stdout_size == 0:
            self.__returncode = 1
        else:
            self.__returncode = 0

    @property
    def stdin(self):
        return self.__stdin

    @property
    def stdout(self):
        return self.__stdout

    @property
    def stderr(self):
        return self.__stderr

    @property
    def returncode(self):
        return self.__returncode

    @property
    def pid(self):
        return self.__pid
