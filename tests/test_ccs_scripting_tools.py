"Unit tests for ccs_scripting_tools module."
import os
import unittest
import io
import logging
import ccs_scripting_tools

CCS_version_text = """        Project        : org-lsst-ccs-subsystem-teststand-main
        Project Version: 1.2.0-SNAPSHOT
        Build Number   : 41
        Build Id       : 41
        Build Url      : http://srs.slac.stanford.edu/hudson/job/org-lsst-ccs-subsystem-teststand-4.0.6/41/
        Build Jdk      : 1.8.0_101
        Source Code Rev: 45b7e551b75b8771f6deadb4bbd7c7cf79e699b1
        Source Code Url: git@github.com:lsst-camera-ccs/org-lsst-ccs-subsystem-teststand.git
"""

class FileStreamProxy(object):
    "In-memory file-like stream class to use with unit tests."
    def __init__(self):
        self.output = io.StringIO()

    def write(self, phrase):
        """
        Write the phrase to the underlying StringIO object, first casting
        as a unicode.
        """
        self.output.write(unicode(phrase))

    def get_value(self):
        """
        Return the current stream, close the StringIO object, and
        create a new one for the next (set of) write(s).
        """
        value = self.output.getvalue()
        self.reset_stream()
        return value

    def reset_stream(self):
        "Reset the output stream contents."
        self.output.close()
        self.output = io.StringIO()

class CcsSubsystemsTestCase(unittest.TestCase):
    "TestCase subclass for testing the CcsSubsystems class."
    def test_interface(self):
        "Test the __init__ for the expected attributes."
        sub = ccs_scripting_tools.CcsSubsystems(dict(ts8='ts8',
                                                     pd='ts8/Monitor',
                                                     mono='ts8/Monochromator',
                                                     proxy='subsystem_proxy'),
                                                version_file=None)
        self.assertTrue(hasattr(sub, 'ts8'))
        self.assertTrue(hasattr(sub, 'pd'))
        self.assertTrue(hasattr(sub, 'mono'))
        self.assertTrue(hasattr(sub, 'proxy'))

    def test_parse_version_info(self):
        "Test the _parse_version_info function."
        version_info = ccs_scripting_tools.CcsSubsystems.\
            _parse_version_info(CCS_version_text)
        self.assertEqual(version_info.project,
                         'org-lsst-ccs-subsystem-teststand-main')
        self.assertEqual(version_info.version, '1.2.0-SNAPSHOT')
        self.assertEqual(version_info.rev,
                         '45b7e551b75b8771f6deadb4bbd7c7cf79e699b1')

    def test_write_versions(self):
        "Test the write_versions function."
        # Create a CcsSubsystems object and fake the _get_version_info
        # functionality for testing in python.
        sub = ccs_scripting_tools.CcsSubsystems(dict(ts8='ts8'),
                                                version_file=None)
        sub.subsystems['ts8'] = ccs_scripting_tools.CcsSubsystems.\
            _parse_version_info(CCS_version_text)
        version_file = 'ccs_versions.txt'
        sub.write_versions(version_file)
        expected_text = \
            ["org-lsst-ccs-subsystem-teststand-main = 1.2.0-SNAPSHOT\n"]
        self.assertEqual(expected_text, open(version_file).readlines())
        os.remove(version_file)

class SubsystemDecoratorTestCase(unittest.TestCase):
    "TestCase subclass for SubsystemDecorator."
    def test_logging(self):
        fs = FileStreamProxy()
        logging.basicConfig(format="%(message)s",
                            level=logging.INFO,
                            stream=fs)
        logger = logging.getLogger()
        sub = ccs_scripting_tools.CcsSubsystems(dict(ts8='ts8',
                                                     pd='ts/PhotoDiode',
                                                     mono='ts/Monochromator'),
                                                logger=logger,
                                                version_file=None)
        fs.reset_stream()
        sub.ts8.synchCommand(10, "setTestType FE55")
        self.assertEqual(fs.get_value(), '10 setTestType FE55\n')
        sub.ts8.synchCommand(10, "setTestType", "FE55")
        self.assertEqual(fs.get_value(), '10 setTestType FE55\n')
        sub.ts8.synchCommand(10, 'accumBuffer', 100, 0.183)
        self.assertEqual(fs.get_value(), '10 accumBuffer 100 0.183\n')
        sub.ts8.asynchCommand("setTestType", "FE55")
        self.assertEqual(fs.get_value(), 'setTestType FE55\n')

if __name__ == '__main__':
    unittest.main()
