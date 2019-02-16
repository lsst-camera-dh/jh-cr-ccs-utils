"""
Module to supplement the interface between python and the CCS jython
interpreter.
"""
from __future__ import print_function
import os
import glob
import shutil
from collections import OrderedDict
try:
    import ConfigParser as configparser
except ImportError:
    import configparser
from PythonBinding import CcsJythonInterpreter
import lcatr.schema
import siteUtils
import camera_components

class CcsSetup(OrderedDict):
    """
    The context-specific setup commands for executing a CCS script
    written in jython.  These commands set variables and paths that
    are known in the calling python code and which are needed by the
    jython script.
    """
    def __init__(self, configFile, sys_paths=()):
        """
        configFile contains the names of the site-specific
        configuration files.  File basenames are provided in
        configFile, and the full paths are constructed in the
        _read(...) method.
        """
        super(CcsSetup, self).__init__()
        self.commands = []
        self['tsCWD'] = os.getcwd()
        self['labname'] = siteUtils.getSiteName()
        self['jobname'] = siteUtils.getJobName()
        self['CCDID'] = siteUtils.getUnitId()
        self['UNITID'] = siteUtils.getUnitId()
        self['LSSTID'] = siteUtils.getLSSTId()
        try:
            self['RUNNUM'] = siteUtils.getRunNumber()
        except Exception:
            self['RUNNUM'] = "no_lcatr_run_number"

        self['ts'] = os.getenv('CCS_TS', default='ts')
        self['archon'] = os.getenv('CCS_ARCHON', default='archon')

        # The following are only available for certain contexts.
        if 'CCS_VAC_OUTLET' in os.environ:
            self['vac_outlet'] = os.getenv('CCS_VAC_OUTLET')
        if 'CCS_CRYO_OUTLET' in os.environ:
            self['cryo_outlet'] = os.getenv('CCS_CRYO_OUTLET')
        if 'CCS_PUMP_OUTLET' in os.environ:
            self['pump_outlet'] = os.getenv('CCS_PUMP_OUTLET')

        self._read(os.path.join(siteUtils.getJobDir(), configFile))

        self.sys_paths = sys_paths

    def __setitem__(self, key, value):
        super(CcsSetup, self).__setitem__(key, "'%s'" % str(value))

    def set_item(self, key, value):
        "Use the OrderedDict.__setitem__ for values that don't need quotes."
        super(CcsSetup, self).__setitem__(key, value)

    def _read(self, configFile):
        if configFile is None:
            return
        configDir = siteUtils.configDir()
        for line in open(configFile):
            key, value = line.strip().split("=")
            self[key.strip()] = os.path.realpath(os.path.join(configDir, value.strip()))

    def __call__(self):
        """
        Return the setup commands for the CCS script.
        """
        # Insert path to the modules used by the jython code.
        for item in self.sys_paths:
            self.commands.insert(0, 'sys.path.append("%s")' % item)
        self.commands.insert(0, 'sys.path.append("%s")' % siteUtils.pythonDir())
        self.commands.insert(0, 'import sys')
        # Set the local variables.
        self.commands.extend(['%s = %s' % item for item in self.items()])
        # Create the CCS subsystems mapping object.
        self.commands.extend(CcsSetup.set_ccs_subsystems())
        return self.commands

    @staticmethod
    def set_ccs_subsystems():
        "Return the setup commands for the CCS subsystem mapppings."
        mapping = ccs_subsystem_mapping()
        if mapping is None:
            return ['subsystems = None']
        commands = ['from collections import OrderedDict',
                    'subsystems = OrderedDict()']
        for key, value in mapping.items():
            commands.append("subsystems['%s'] = '%s'" % (key, value))
        return commands


class CcsRaftSetup(CcsSetup):
    """
    Subclass of CcsSetup that will query the eTraveler db tables for
    the sensors in the raft specified as LCATR_UNIT_ID.
    """
    def __init__(self, configFile, sys_paths=()):
        super(CcsRaftSetup, self).__init__(configFile, sys_paths=sys_paths)
        self.commands.append('from collections import namedtuple')
        self.commands.append("SensorInfo = namedtuple('SensorInfo', 'sensor_id manufacturer_sn'.split())")
        self.commands.append("RebInfo = namedtuple('RebInfo', 'reb_id manufacturer_sn firmware_version'.split())")
        self.commands.append("ccd_names = dict()")
        self.commands.append("reb_eT_info = dict()")
#        self._get_ccd_and_reb_names()
        self.set_item('ccd_names["S00"]' , 'SensorInfo("fakeSID00", "fakeMSN00")')
        self.set_item('ccd_names["S10"]' , 'SensorInfo("fakeSID10", "fakeMSN10")')
        self.set_item('ccd_names["S11"]' , 'SensorInfo("fakeSID11", "fakeMSN11")')
        self.set_item('sequence_file', self['itl_seqfile'])
    def _get_ccd_and_reb_names(self):
        raft_id = siteUtils.getUnitId()
        raft = camera_components.Raft.create_from_etrav(raft_id,prodServer=True,db_name='Dev',htype='LCA-10692_CRTM')
        for slot in raft.slot_names:
            sensor = raft.sensor(slot)
            self.set_item('ccd_names["%s"]' % slot, 'SensorInfo("%s", "%s")'
                          % (str(sensor.sensor_id),
                             str(sensor.manufacturer_sn)))

#        # aliveness bench reb serial numbers:
#        for slot, reb_sn in zip(('REB0', 'REB1', 'REB2'),
#                                [412220615, 412162821, 305879976]):
#            raft.rebs[slot].manufacturer_sn = '%x' % reb_sn

        for slot, reb in raft.rebs.items():
            self.set_item('reb_eT_info["%s"]' % slot,
                          'RebInfo("%s", "%s", "%s")'
                          % (reb.reb_id, reb.manufacturer_sn,
                             reb.firmware_version))
        ccd_type = str(raft.sensor_type.split('-')[0])
#hn   - for testing and also for protection while validating code
        ccd_type = 'ITL'

        self['ccd_type'] = ccd_type
        print("ccd_type = ",ccd_type)

        if ccd_type == 'ITL':
            self.set_item('sequence_file', self['itl_seqfile'])
        elif ccd_type.upper() == 'E2V':
            self.set_item('sequence_file', self['e2v_seqfile'])
        else:
            raise RuntimeError('Invalid ccd_type: %s' % ccd_type)
        shutil.copy(self['sequence_file'].strip("'"),
                    self['tsCWD'].strip("'"))


def ccsProducer(jobName, ccsScript, ccs_setup_class=None, sys_paths=(),
                verbose=True):
    """
    Run the CCS data acquistion script under the CCS jython interpreter.
    """
    if ccs_setup_class is None:
        ccs_setup_class = CcsSetup

    ccs = CcsJythonInterpreter("ts")
    configDir = siteUtils.configDir()
    setup = ccs_setup_class('%s/acq.cfg' % configDir, sys_paths=sys_paths)

    full_script_path = siteUtils.jobDirPath(ccsScript, jobName=jobName)
    result = ccs.syncScriptExecution(full_script_path, setup(), verbose=verbose)

    output = open("%s.log" % jobName, "w")
    output.write(result.getOutput())
    output.close()
    if result.thread.java_exceptions:
        raise RuntimeError("java.lang.Exceptions raised:\n%s"
                           % '\n'.join(result.thread.java_exceptions))

def ccsValidator(results=None):
    """
    Persist standard file patterns, e.g., '*.fits', 'pd-values*.txt',
    using lcatr.schema.
    """
    if results is None:
        results = []
    files = glob.glob('*/*.fits')
    files += glob.glob('pd-values*.txt')
    files += glob.glob('*.png')
    files += glob.glob('*.seq')
    unique_files = set(files)
    results.extend([lcatr.schema.fileref.make(item) for item in unique_files])
    results.extend(siteUtils.jobInfo())
    results = siteUtils.persist_ccs_versions(results)
#hn    results = siteUtils.persist_reb_info(results)
    lcatr.schema.write_file(results)
    lcatr.schema.validate_file()

def ccs_subsystem_mapping(config_file=None, section='ccs_subsystems'):
    """
    Function to find the mapping of abstracted to concrete CCS subsystem
    names for use by jython scripts inside of harnessed jobs.

    Parameters
    ----------
    config_file : str, optional
         The configuration file containing the mapping.  If None (default),
         then the file pointed to by the LCATR_CCS_SUBSYSTEM_CONFIG
         environment variable is used.  If that is not set, then None
         is returned.
    section : str, optional
         The section of the config file that contains the mapping.
         Default:  'ccs_subsystems'.

    Returns
    -------
    dict : A dictionary containing the mapping.
    """
    if config_file is None:
        if 'LCATR_CCS_SUBSYSTEM_CONFIG' in os.environ:
            config_file = os.environ['LCATR_CCS_SUBSYSTEM_CONFIG']
        else:
            return None
    parser = configparser.ConfigParser()
    parser.optionxform = str
    parser.read(config_file)
    return OrderedDict([pair for pair in parser.items(section)])
