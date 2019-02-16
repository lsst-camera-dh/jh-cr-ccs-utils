"""
Do-nothing python versions of CCS jython objects and classes to
enable testing.
"""
class CcsType(object):
    "Python proxy for the org.lsst.ccs.scripting.CCS jython object."
    def __init__(self):
        self.proxies = {'ts8-proxy': Ts8Proxy(),
                        'subsystem-proxy': NullSubsystem()}

    def attachSubsystem(self, value):
        """
        Attach a proxy subsystem object that has the CCS subsystem interface.
        """
        try:
            return self.proxies[value]
        except KeyError:
            return NullSubsystem()

    def setThrowExceptions(self, value):
        "Do-nothing function."
        pass

    @property
    def subsystem_names(self):
        """
        Return a list of valid proxy subsytems.
        """
        return self.proxies.keys()

class NullSubsystem(object):
    """
    A do-nothing class with dummy methods to emulate a CCS subsystem.
    """
    def __init__(self):
        pass

    def synchCommand(self, *args):
        "Execute a synchronous CCS command."
        return NullResponse(*args)

    def asynchCommand(self, *args):
        "Execute an asynchronous CCS command."
        return NullResponse(*args)

class Ts8Proxy(NullSubsystem):
    "Fake ts8 subsystem with canned responses to CCS commands."
    def __init__(self):
        super(Ts8Proxy, self).__init__()
        self._fill_responses()

    def _fill_responses(self):
        self.responses = dict()
        self.responses['getREBDeviceNames'] \
            = ProxyResponse(('R00.Reb0', 'R00.Reb1', 'R00.Reb2'))
        self.responses['getREBDevices'] \
            = ProxyResponse(('R00.Reb0', 'R00.Reb1', 'R00.Reb2'))
        self.responses['getREBHwVersions'] \
            = ProxyResponse([808599560, 808599560, 808599560])
        #self.responses['getREBSerialNumbers'] \
        #    = ProxyResponse([305877457, 305892521, 305879138])
#        # aliveness bench REBs:
#        self.responses['getREBSerialNumbers'] \
#            = ProxyResponse([412220615, 412162821, 305879976])
        # ETU1 REBs:
        self.responses['getREBSerialNumbers'] \
            = ProxyResponse([412165857, 412223738, 412160431])
        self.responses['printGeometry 3'] = ProxyResponse('''--> R00
---> R00.Reb2
----> R00.Reb2.S20
----> R00.Reb2.S21
----> R00.Reb2.S22
---> R00.Reb1
----> R00.Reb1.S10
----> R00.Reb1.S11
----> R00.Reb1.S12
---> R00.Reb0
----> R00.Reb0.S00
----> R00.Reb0.S01
----> R00.Reb0.S02
''')
        self.responses['getREBIds'] = ProxyResponse((0, 1, 2))
        self.responses['getSequencerParameter CleaningNumber'] = ProxyResponse([0, 0, 0])
        self.responses['getSequencerParameter ClearCount'] = ProxyResponse([1, 1, 1])
    def synchCommand(self, *args):
        command = ' '.join([str(x) for x in args[1:]])
        try:
            return self.responses[command]
        except KeyError:
            return NullResponse()

    def asynchCommand(self, *args):
        command = ' '.join([str(x) for x in args])
        try:
            return self.responses[command]
        except KeyError:
            return NullResponse()

class NullResponse(object):
    """
    Do-nothing response class to act as a return object by the
    NullSubsystem methods.
    """
    def __init__(self, *args):
        pass

    def getResult(self):
        "A generic result."
        return 1

class ProxyResponse(NullResponse):
    "Response object with canned response content."
    def __init__(self, content):
        super(ProxyResponse, self).__init__()
        self.content = content

    def getResult(self):
        return self.content

CCS = CcsType()
