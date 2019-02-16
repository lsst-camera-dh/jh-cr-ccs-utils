"""
Socket connection interface to CCS Jython interpreter.
"""
import sys
import time
import re
import socket
import threading
import uuid

__all__ = ['CcsJythonInterpreter', 'CcsException', 'CcsExecutionResult']


class CcsExecutionResult:
    """Results class."""
    def __init__(self, thread):
        self.thread = thread

    def getOutput(self):
        """Return the result of a jython command as a string."""
        while self.thread.running:
            time.sleep(0.1)
        return self.thread.execution_output


class CcsException(Exception):
    """Exception class for CCS Jython interface."""
    def __init__(self, value):
        super(CcsException, self).__init__()
        self.value = value
    def __str__(self):
        return repr(self.value)


class CcsJythonInterpreter:
    """Interface class to CCS Jython interpreter."""
    def __init__(self, name=None, host=None, port=4444):
        self.port = port
        if host is None:
            # Get local machine name
            self.host = socket.gethostname()
        else:
            self.host = host
        host_and_port = '{}:{}'.format(self.host, self.port)
        try:
            self.socket_connection = self._socket_connection()
            print('Connected to CCS Python interpreter on host:port',
                  host_and_port)
        except Exception as eobj:
            print(eobj)
            raise CcsException("Could not connect to CCS Python Interpreter " +
                               "on host:port " + host_and_port)
        if name is not None:
            name = name.replace("\n", "")
            self.syncExecution("initializeInterpreter " + name)

    def _socket_connection(self):
        sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sc.connect((self.host, self.port))
        connectionResult = sc.recv(1024).decode('utf-8')
        if "ConnectionRefused" in connectionResult:
            raise CcsException("Connection Refused")
        return sc

    def aSyncExecution(self, statement):
        return self.sendInterpreterServer(statement)

    def syncExecution(self, statement):
        result = self.sendInterpreterServer(statement)
        # Calling .getOutput() here causes the object to wait for the
        # underlying thread to stop running.
        result.getOutput()
        return result

    def aSyncScriptExecution(self, filename):
        with open(filename, "r") as fd:
            fileContent = fd.read()
        return self.sendInterpreterServer(fileContent)

    def syncScriptExecution(self, filename, setup_commands=(), verbose=False):
        if verbose and setup_commands:
            print("Executing setup commands for", filename)
        for command in setup_commands:
            if verbose:
                print(command)
            self.syncExecution(command)

        if verbose:
            print("Executing %s..." % filename)
        with open(filename, "r") as fd:
            fileContent = fd.read()
        result = self.sendInterpreterServer(fileContent)
        # Calling .getOutput() here causes the object to wait for the
        # underlying thread to stop running.
        result.getOutput()
        return result

    def sendInterpreterServer(self, content):
        thread_id = str(uuid.uuid4())
        executor_thread = CcsPythonExecutorThread(thread_id,
                                                  self.socket_connection)
        return executor_thread.executePythonContent(content)


class CcsPythonExecutorThread:
    def __init__(self, thread_id, socket_connection):
        self.socket_connection = socket_connection
        self.thread_id = thread_id
        self.output_thread = threading.Thread(target=self.listenToSocketOutput)
        self.java_exceptions = []

    def executePythonContent(self, content):
        self.running = True
        self.output_thread.start()
        content = ("startContent:" + self.thread_id + "\n" +
                   content + "\nendContent:" + self.thread_id + "\n")
        self.socket_connection.send(content.encode('utf-8'))
        return CcsExecutionResult(self)

    def listenToSocketOutput(self):
        re_obj = re.compile(r'.*java.lang.\w*Exception.*')
        self.execution_output = ""
        while self.running:
            try:
                output = self.socket_connection.recv(1024).decode('utf-8')
            except Exception as eobj:
                print(eobj)
                raise CcsException("Communication Problem with Socket")
            for item in output.split('\n'):
                if re_obj.match(item):
                    self.java_exceptions.append(item)
            if "doneExecution:" + self.thread_id not in output:
                sys.stdout.write(output)
                sys.stdout.flush()
                self.execution_output += output
            else:
                self.running = False
        del self.output_thread
