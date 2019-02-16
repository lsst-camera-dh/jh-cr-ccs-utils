"""
Tools for CCS jython scripts.
"""
from collections import namedtuple, OrderedDict
import ccs_python_proxies
try:
    from org.lsst.ccs.scripting import CCS
except ImportError:
    CCS = ccs_python_proxies.CCS

class SubsystemDecorator(object):
    """
    Decorator class to overlay logging of the commands sent to a CCS
    subsystem object.
    """
    def __init__(self, ccs_subsystem, logger=None, name=None):
        self.ccs_subsystem = ccs_subsystem
        self.logger = logger
        self.name = name

    def _log_command(self, args):
        if self.logger is not None:
            command_string = " ".join(["%s" % arg for arg in args])
            self.logger.info(command_string)

    def synchCommand(self, *args):
        "Decorator method for a synchronous command."
        self._log_command(args)
        return self.ccs_subsystem.sendSynchCommand(*args)

    def asynchCommand(self, *args):
        "Decorator method for an asynchronous command."
        self._log_command(args)
        return self.ccs_subsystem.sendAsynchCommand(*args)

CcsVersionInfo = namedtuple('CcsVersionInfo', 'project version rev')

class CcsSubsystems(object):
    """
    Container for collections of CCS subsystems.
    """
    def __init__(self, subsystems, logger=None,
                 version_file='ccs_versions.txt'):
        """
        Constructor.

        Parameters
        ----------
        subsystems : dict
            A dictionary of subsystem names keyed by desired attribute
            name, e.g., dict(ts8='ts8', pd='ts/PhotoDiode',
                             mono='ts/Monochromator')
        logger : logging.Logger, optional
            Logger to be used by the SubsystemDecorator class. Default: None.
        version_file : str, optional
            Text file to contain the CCS subsystem version information.
            This can be set to None to suppress writing the file.
            Default: 'ccs_versions.txt'.
        """
        self._proxy_subsystems = ccs_python_proxies.CCS.subsystem_names
        for key, value in subsystems.items():
            if value in self._proxy_subsystems:
                proxy_subsystem = ccs_python_proxies.CCS.attachSubsystem(value)
                self.__dict__[key] = SubsystemDecorator(proxy_subsystem,
                                                        logger=logger,
                                                        name=value)
                continue
            self.__dict__[key] = SubsystemDecorator(CCS.attachSubsystem(value),
                                                    logger=logger, name=value)
        self._get_version_info(subsystems)

    def _get_version_info(self, subsystems):
        # Version info is only available for "real" subsystems like
        # 'ts' or 'ts8-bench', not whatever things like
        # 'ts/Monochromator' are called in CCS parlance.  So extract
        # the parts before the '/' as the "real" subsystem names of
        # interest
        real_subsystems = set([x.split('/')[0] for x in subsystems.values()
                               if x not in self._proxy_subsystems])
        self.subsystems = OrderedDict()
        for subsystem in real_subsystems:
            print "getting info for subsystem - ",subsystem
            my_subsystem = CCS.attachSubsystem(subsystem)
            reply = my_subsystem.sendSynchCommand('getDistributionInfo')
#            reply = my_subsystem('getDistributionInfo')
            try:
                result = reply.getResult().toString()
                self.subsystems[subsystem] = self._parse_version_info(result)
            except AttributeError:
                # Running in python for unit tests.
                pass

    @staticmethod
    def _parse_version_info(ccs_result):
        """
        Code to parse response from CCS getDistributionInfo command.
        """
        info = dict()
        for line in ccs_result.split('\n'):
            tokens = [x.strip() for x in line.split(':')]
            if len(tokens) >= 2:
                info[tokens[0]] = tokens[1]
        return CcsVersionInfo(info['Project'], info['Project Version'],
                              info['Source Code Rev'])

    def write_versions(self, outfile):
        "Write CCS version information to an output file."
        with open(outfile, 'w') as output:
            for key, value in self.subsystems.items():
                output.write('%s = %s\n' % (value.project, value.version))
