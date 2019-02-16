"""
Control of the TS8 XYZ stage.

Exports
-------
X : Represents the bottom horizontal axis, which travels across the aperture.
Y : Represents the vertical axis.
Z : Represents the depth axis, which moves toward or away from the aperture.
Stage : A class providing stage commands.
TimeoutError : An exception raised when an operation takes too long.
"""

import exceptions
import inspect
from threading import Lock
from time import sleep
from datetime import datetime, timedelta

from java.time import Duration

from org.lsst.ccs.scripting import CCS, ScriptingStatusBusListener
from org.lsst.ccs.messaging import StatusMessageListener
from org.lsst.ccs.subsystem.motorplatform.bus import (
    MotorReplyListener, MoveAxisRelative, MoveAxisAbsolute, ClearAllFaults,
    StopAllMotion, HomeAxis, ChangeAxisEnable)

class _Axis:
    """
    Represents a valid axis. Contains the name, the numerical index,
    the approximate range of travel in mm and the speed limit in
    mm/sec.
    """
    def __init__(self, name, index, maxTravel, maxSpeed):
        self.name = name
        self.index = index
        self.maxTravel = maxTravel
        self.maxSpeed = maxSpeed

X = _Axis("X", 0, 480.0, 20.0)
Y = _Axis("Y", 1, 378.0, 20.0)
Z = _Axis("Z", 2, 56.5, 10.0)

class Stage:
    """
    Represents the Parker motorized platform installed in the TS8 dark box.
    Note that all methods requiring an axis specification will accept only one of the
    three objects X, Y and Z exported by the TS8Stage module.

    Public methods
    --------------
    The constructor.
    enable() : Enable an axis.
    disable() : Disable an axis.
    moveTo() : Move the axis to an absolute position.
    moveBy() : Change the axis position by a specified amount.
    home() : Move the axis to its home position and then make the coordinate equal to zero.
    clearFaults() : Clear all axis and controller fault flags.
    stop() : Immediately stop all motion on all axes and discard any queued commands.
    waitForStop() : Wait for an axis to reach the target position and stop moving.
    """
    def __init__(self, subsystemName):
        """
        Connect to the site's CCS bus, contact the worker subsystem
        controlling the stage and begin monitoring status messages
        from that subsystem.

        Arguments
        ---------
        subsystemName : str
            The name of the worker subsystem as it appears on the CCS bus.
        """
        self._target = subsystemName
        self._replies = _ReplyHandler(self._target)
        self._subsys = _SubsystemHandle(self._target, self._replies)

    def enable(self, axis):
        """
        Enable motion on the given axis.

        Arguments
        ---------
        axis : private type
            One of the three valid axis objects.
        """
        _checkAxis(axis)
        self._subsys.sendCommand("changeAxisEnable", ChangeAxisEnable(axis.name, True))

    def disable(self, axis):
        """
        Disable motion on the given axis.

        Arguments
        ---------
        axis : private type
            One of the three valid axis objects.
        """
        _checkAxis(axis)
        self._subsys.sendCommand("changeAxisEnable", ChangeAxisEnable(axis.name, False))

    def moveTo(self, axis, position, speed):
        """
        Move the axis until it's at the specified coordinate.

        Raises TimeoutError if the move takes longer than it should.

        Arguments
        ---------
        axis : private type
            One of the three valid axis objects.
        position : float
            The coordinate of the target position in mm.
        speed : float
            The maximum speed, in mm/sec, at which to move.
            There's a ceiling of 20 mm/sec for X and Y; 10 mm/sec for Z.
        """
        _checkAxis(axis)
        speed = min(speed, axis.maxSpeed)
        timeout = axis.maxTravel / speed + 1.0
        self._subsys.sendCommand("moveAxisAbsolute", MoveAxisAbsolute(axis.name, position, speed))
        self.waitForStop(axis, position, timeout)

    def moveBy(self, axis, change, speed):
        """
        Move the axis so that its coordinate has changed by the
        specified amount. The starting position is the one current at
        the time the move command begins execution.

        Raises TimeoutError if the move takes longer than it should.

        Arguments
        ---------
        axis : private type
            One of the three valid axis objects.
        change : float
            The desired coordinate change in mm.
        speed : float
            The maximum speed, in mm/sec, at which to move.
            There's a ceiling of 20 mm/sec for X and Y; 10 mm/sec for Z.
        """
        _checkAxis(axis)
        speed = min(speed, axis.maxSpeed)
        accel = 10.0 * speed # 1/10 second to get up to speed and to stop.
        d = accel * 0.1 * 0.1 # Combined distance covered while changing speed at the ends.
        moveTime = 0.1 + (change - d)/speed + 0.1
        self._subsys.sendCommand("sendAxisStatus", SendAxisStatus(axis.name))
        sleep(1.0);
        position = self.getAxisStatus(axis).getPosition()
        self._subsys.sendCommand("moveAxisRelative", MoveAxisRelative(axis.name, change, moveTime))
        self.waitForStop(axis, position + change, moveTime + 5.0)

    def home(self, *axes):
        """Bring the specified axes to their home positions and then
        reset their coordinates to zero.  The home position is just
        inside the boundary set by the negative limit switch.

        Raises TimeoutError if the operation takes longer than
        it should.

        Arguments
        ---------
        axes : tuple of private type
            Any or all of the three valid axis objects., e.g.,
            home(TS8Stage.X, TS8Stage.Y).
        """
        for ax in axes:
            _checkAxis(ax)
            timeout = ax.maxTravel / ax.maxSpeed # Homing is done at maximum speed.
            self._subsys.sendCommand("homeAxis", HomeAxis(ax.name))
            self.waitForStop(ax, timeout)

    def clearFaults(self):
        """
        Clear fault flags on all axes and for the controller in general.
        If the condition that caused a fault hasn't been corrected
        then the flag will be raised again immediately.
        """
        self._subsys.sendCommand("clearAllFaults", ClearAllFaults())

    def stop(self):
        """
        Stop motion immediately on all axes and discard any commands
        the worker subsystem may have queued internally. This will
        raise some fault flags which you'll
        have to clear before you can move the axes again.
        """
        self._subsys.sendCommand("stopAllMotion", StopAllMotion())

    def waitForStop(self, axis, targetPosition, timeout):
        """
        Wait until the current position is close to the target position.
        Raises TimeoutError if the position never gets close or the axis
        doesn't stop moving.

        Arguments
        ---------
        axis : private type
            One of the three valid axis objects.
        targetPosition: float
            The target position in mm.
        timeout : float
            How long to wait, in seconds.
        """
        _checkAxis(axis)
        deadline = datetime.now() + timedelta(seconds=timeout+2.0)
        status = self.getAxisStatus(axis)
        while (status is None) or (abs(targetPosition - status.getPosition()) > 0.01):
           sleep(0.25)
           if datetime.now() > deadline:
              raise TimeoutError("Waiting for the " + axis.name + " axis to get near the target position.")
           status = self.getAxisStatus(axis)
        # Now wait for the axis to stop moving.
        while (status is None) or (status.isMoving()):
            sleep(0.25)
            if datetime.now() > deadline:
                raise TimeoutError("Waiting for the " + axis.name + " axis to stop moving.")
            status = self.getAxisStatus(axis)

    def getAxisStatus(self, axis):
        return self._replies.getAxisStatus(axis)

    def getControllerStatus(self):
        return self._replies.getControllerStatus()


class _SubsystemHandle:
    """
    Establish the command link to the worker subsystem and
    set up monitoring its status bus messages by the reply
    handler.
    """
    def __init__(self, subsystemName, replyHandler):
        self._subsysName = subsystemName
        self._subsysHandle  = CCS.attachSubsystem(subsystemName)
        CCS.addStatusBusListener(replyHandler, replyHandler.getMessageFilter())
    def sendCommand(self, cmd, arg):
        # A timeout of ten seconds should be more than enough since the subsystem
        # commands just put tasks in a work queue rather than do the work directly.
        self._subsysHandle.synchCommand(10, cmd, arg)


class _ReplyHandler(MotorReplyListener, ScriptingStatusBusListener):
    """Save the latest status message of each type; per axis, if applicable."""
    def __init__(self, target):
        self._target = target
        self._lock = Lock()
        # The following instance variables are protected by self._lock
        self._controllerStatus = None
        self._axisStatus = dict()
        self._ioStatus = None
        self._platformConfig = None
        self._captureData = None

    def getControllerStatus(self):
        with self._lock:
            return self._controllerStatus

    def getAxisStatus(self, axis):
        _checkAxis(axis)
        with self._lock:
            return self._axisStatus.get(axis.name, None)

    def getIoStatus(self):
        with self._lock:
            return self._ioStatus

    def getPlatformConfig(self):
        with self._lock:
            return self._platformConfig

    def getCapturedata(self):
        with self._lock:
            return self._captureData

    ########## Implementation of ScriptingStatusBusListener #########
    def onStatusBusMessage(self, msg):
        msg = msg.getBusMessage().getSubsystemData().getValue()
        msg.callMotorReplyHandler(self)

    def getMessageFilter(self):
        def filter(msg):
            if msg.getOrigin() != self._target:
                return False
            msg = msg.getBusMessage()
            if not _hasmethod(msg, "getSubsystemData"):
                return False
            msg = msg.getSubsystemData()
            if not _hasmethod(msg, "getValue"):
                return False
            msg = msg.getValue()
            return _hasmethod(msg, "callMotorReplyHandler")
        return filter

    ########## Implementation of MotorReplyListener ##########
    # An incoming status bus message will call one of these methods
    # from its own callMotorReplyHandler() method. This will happen
    # in a thread other than the one that's using this module,
    # so we sync updates and retrievals of status info.
    def axisStatus(self, axstat):
        with self._lock:
            self._axisStatus[axstat.getAxisName()] = axstat

    def controllerStatus(self, constat):
        with self._lock:
            self._controllerStatus = constat

    def ioStatus(self, iostat):
        with self._lock:
            self._iostatus = iostat

    def platformConfig(self, config):
        with self._lock:
            self._platformConfig = config

    def capturedData(self, data):
        with self._lock:
            self._capturedData = data


def _hasmethod(obj, name):
    return inspect.ismethod(getattr(obj, name, None))

def _checkAxis(ax):
    if not isinstance(ax, _Axis):
        raise ValueError("The given object is not a valid axis object.")

class TimeoutError(exceptions.BaseException):
    def __init__(self, message):
        self.message = message


if __name__ == "__main__":
    # A short test of the module.
    stage = Stage("ts8-motorplatform")
    sleep(2.0)
    ctlstat = stage.getControllerStatus()
    print "Motion enabled?", ctlstat.isMotionEnabled()
    xstat = stage.getAxisStatus(X)
    print "X axis enabled?", xstat.isEnabled()
    print "X axis position:", xstat.getPosition()
    stage.clearFaults()
    start = datetime.now()
    step = 10
    speed = X.maxSpeed
    limit = int(X.maxTravel / step) * step
    print "millisecs/req pos/final pos"
    for pos in range(0, limit, step) + range(0, limit, step):
        stage.moveTo(X, float(pos), speed)
        xstat = stage.getAxisStatus(X)
        delta = datetime.now() - start
        print delta.seconds*1000 + delta.microseconds/1000, float(pos), xstat.getPosition()
