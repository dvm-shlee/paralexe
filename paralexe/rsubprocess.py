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

    TODO: Planning to integrate SLURM followed by Google Cloud
    """
    def __init__(self, cmd, client, timeout=None, bufsize=-1, get_pty=False, environment=None):
        # if client.connected():
        self.__client = client
            # new_client = None
        # else:  # client can be disconnected when it is running on thread
        #     Client, cfg = client.clone()
        #     new_client = Client()
        #     new_client.connect(cfg)
        #     self.__client = new_client
        self.__cmd = cmd
        self.__timeout = timeout
        self.__bufsize = bufsize
        self.__get_pty = get_pty
        self.__environment = environment
        self.__pid = None

        if self.__client.mode == 'ssh':
            self._exec_by_ssh()
        elif self.__client.mode == 'slurm':
            self._exec_by_slurm()
        # elif client.mode == 'gcloud':
        #     self._exec_by_gcloud()
        else:
            pass
        # if new_client is not None:
        #     new_client.close()

    def _exec_by_ssh(self):
        """through ssh execution, it forced to wait until the execution is done"""
        stdout, stderr = self.__client.exec_command('echo $$; exec {}'.format(self.__cmd))
        self.__pid = int(self.__stdout.readline())
        self._output_handler(stdout, stderr)

    def _exec_by_slurm(self):
        """SLURM is scheduler based, so the command execution may not holded, so need to add
        some function to wait until job done in here. then parse log file to get stdin and stdout"""
        import re
        p = re.compile(r'srun:\s+\w+\s+(\d+).*\n$')
        stdout, stderr = self.__client.exec_command("srun {}".format(self.__cmd))
        while self.__pid is None:
            line = stderr.readline()
            if p.match(line):
                self.__pid = int(p.sub(r'\1', line))
        self._output_handler(stdout, stderr)

    def _output_handler(self, stdout, stderr):
        self.__stdin = None
        self.__stdout = stdout
        self.__stderr = stderr

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
    def pid(self):
        return self.__pid
