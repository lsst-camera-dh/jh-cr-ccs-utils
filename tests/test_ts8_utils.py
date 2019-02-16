"""
Unit test code for ts8_utils.
"""
import os
import logging
import unittest
from collections import namedtuple, defaultdict
from ccs_scripting_tools import CcsSubsystems
from ts8_utils import set_ccd_info

SensorInfo = namedtuple('SensorInfo', 'sensor_id manufacturer_sn'.split())
ccd_names = {slot: SensorInfo('ITL-{}'.format(i), i) for i, slot in
             enumerate('S00 S01 S02 S10 S11 S12 S20 S21 S22'.split())}

class Ts8UtilsTestCase(unittest.TestCase):
    """
    TestCase class for ts8_utils module.
    """
    def setUp(self):
        self.outfile = 'test_ts8_output.txt'

    def tearDown(self):
        if os.path.isfile(self.outfile):
            os.remove(self.outfile)

    def test_set_ccd_info(self):
        """Unit test for set_ccd_info."""
        with open(self.outfile, 'w') as output:
            logging.basicConfig(format="%(message)s", stream=output)
            logger = logging.getLogger('test_ts8_utils')
            logger.setLevel(logging.INFO)
            subsystems = dict(ts8='ts8-proxy', rebps='subsystem-proxy')
            ccs_sub = CcsSubsystems(subsystems, logger=logger,
                                    version_file=None)
            set_ccd_info(ccs_sub, ccd_names, logger)

        ccs_commands = defaultdict(lambda: 0)
        with open(self.outfile, 'r') as fd:
            for line in fd:
                tokens = line.split()
                if tokens[1].startswith('set') or tokens[1].startswith('get'):
                    ccs_commands[tokens[1]] += 1
        for command in ccs_commands:
            if command.startswith('set'):
                self.assertEqual(ccs_commands[command], 9)
            elif command == 'getChannelValue':
                self.assertEqual(ccs_commands[command], 18)

if __name__ == '__main__':
    unittest.main()
