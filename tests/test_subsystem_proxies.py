"""
Unit tests for ccs_python_proxies.py module.
"""
import unittest
import ccs_python_proxies

class Ts8RebCommandTestCase(unittest.TestCase):
    "Test case class for command REBs via the ts8 subsystem."
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_REB_info(self):
        "Tests for REB-related .synchCommand commands."
        ts8 = ccs_python_proxies.CCS.attachSubsystem('ts8-proxy')

        reb_names = ts8.synchCommand(10, 'getREBDeviceNames').getResult()
        self.assertEqual(len(reb_names), 3)
        for reb_name, expected_name in \
            zip(reb_names, ('R00.Reb0', 'R00.Reb1', 'R00.Reb2')):
            self.assertEqual(reb_name, expected_name)

        reb_firmware_versions \
            = ts8.synchCommand(10, 'getREBHwVersions').getResult()
        self.assertEqual(len(reb_firmware_versions), 3)
        expected_versions = [808599560, 808599560, 808599560]
        for fw_ver, expected in zip(reb_firmware_versions, expected_versions):
            self.assertEqual(fw_ver, expected)

        reb_SNs = ts8.synchCommand(10, 'getREBSerialNumbers').getResult()
        self.assertEqual(len(reb_SNs), 3)
        expected_sns = [305877457, 305892521, 305879138]
        for reb_sn, expected_sn in zip(reb_SNs, expected_sns):
            self.assertEqual(reb_sn, expected_sn)

if __name__ == '__main__':
    unittest.main()
