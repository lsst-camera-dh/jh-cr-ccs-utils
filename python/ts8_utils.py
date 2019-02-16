"""
Utilities to work with the ts8 subsystem.
"""
from collections import namedtuple
#try:
#    from org.lsst.ccs.scripting import CCS
#except ImportError:
#    CCS = ccs_python_proxies.CCS

def write_REB_info(ts8sub, outfile='reb_info.txt'):
    """
    Write the REB device names, firmware versions, and manufacturer
    serial numbers to a text file for persisting to the eT tables.

    Parameters
    ----------
    ts8sub : CCS subsystem
        The ts8 subsystem.
    outfile : str, optional
        The name of the text file to contain the REB info.
        Default: 'reb_info.txt'.
    """
#    ts8sub = CCS.attachSubsystem('cr-raft')

    reb_names = ts8sub.sendSynchCommand( 'getREBDeviceNames')
    print("reb_names = ",reb_names)
#.getResult()
    fw_vers = ts8sub.sendSynchCommand( 'getREBHwVersions')
#.getResult()
    SNs = ts8sub.sendSynchCommand( 'getREBSerialNumbers')
#.getResult()
    print("SNs = ",SNs)
    with open(outfile, 'w') as output:
        for reb_info in zip(reb_names, fw_vers, SNs):
            output.write('%s  %x  %x\n' % reb_info)

def get_REB_info(ts8sub, rebid):
    """
    Retrieve the REB device name, firmware version, and manufacturer
    serial number for the specified REB ID.

    Parameters
    ----------
    ts8sub : CCS subsystem
        The ts8 subsystem.
    rebid : int
        The REB ID.

    Returns
    -------
    namedtuple : (REB device name, firmware version, serial number)
    """
    RebInfo = namedtuple('RebInfo', 'deviceName hwVersion serialNumber'.split())
    rebids = list(ts8sub.sendSynchCommand( 'getREBIds').getResult())
    rebids[:] = [x % 4 for x in rebids]
    dev_names = list(ts8sub.sendSynchCommand( 'getREBDevices').getResult())
    hw_versions = list(ts8sub.sendSynchCommand( 'getREBHwVersions').getResult())
    serial_numbers \
        = ['%x' % x for x in
           ts8sub.sendSynchCommand( 'getREBSerialNumbers').getResult()]
    index = rebids.index(rebid)
    return RebInfo(dev_names[index], hw_versions[index], serial_numbers[index])

def set_ccd_info(ccs_sub, ccd_names, logger):
    """
    Set the CCD serial numbers in the CCS code.  Get the CCD
    temperature and BSS voltages from the ts8 and ccs-rebps
    subsystems, and set those values in the CCS code.

    Parameters
    ----------
    ccs_sub : CcsSubsystems
        Container of CCS subsystems.
    ccd_names : dict
        Dictionary of namedtuple containing the CCD .sensor_id and
        .maufacturer_sn information, keyed by slot name.
    logger : logging.Logger
        Log commands using the logger.info(...) function.

    Notes
    -----
    This is function is a refactored version of
    harnessed-jobs/python/eolib.EOTS8SetupCCDInfo.
    """
    # Parse the printGeometry output to map CCD values to REBs.
#    geo = ccs_sub.ts8.sendSynchCommand( "printGeometry 3").getResult()
    geo = "--> GREB\n---> GREB.S10\n---> GREB.S11\n--> WREB\n---> WREB.S00\n"
#--> R00
#---> R00.Reb2
#----> R00.Reb2.S20
#----> R00.Reb2.S21
#----> R00.Reb2.S22
#---> R00.Reb1
#----> R00.Reb1.S10

    for line in geo.split('\n'):
        # The lines with the CCD IDs and slot names will be of the form
        # '---> R00.Reb2.S20'.  So we'll extract the last three
        # non-whitespace letters to get the slot name and the full
        # entry to get the CCD ID.
        my_line = line.strip()     # remove any trailing whitespace
        slot = my_line.split('.')[-1]

        if len(slot) != 3 or slot[0] != 'S':
            continue
        print "slot = ",slot

        ccd_id = my_line.split(' ')[1]
        sensor = ccd_names[slot]

        # Set the LSST serial number.
#        command = 'setLsstSerialNumber %s %s' % (ccd_id, sensor.sensor_id)
#need something to make a map of ccdSpecificHeaderKeywords
        command = 'ccdSpecificHeaderKeywords CCDSerialLSST  %s %s' % (ccd_id, sensor.sensor_id)

#        ccs_sub.ts8.sendSynchCommand( command)

        # Set the manufacturer serial number.
#        command = ('setManufacturerSerialNumber %s %s'
#                   % (ccd_id, sensor.manufacturer_sn))
        command = 'ccdSpecificHeaderKeywords CCDSerialManufacturer  %s %s' % (ccd_id, sensor.sensor_id)
#fix        ccs_sub.ts8.sendSynchCommand( command)

        # Set the CCD temperature.
#        reb_id = int(slot[1])
#        ccd_num = int(slot[2])
        
        rebname = my_line.split('.')[0].split(' ')[1]
        ccd_num = int(slot[2])

        print("rebname = ",rebname)
        print("ccd_num = ",ccd_num)

#        command = "getChannelValue R00.Reb%d.CCDTemp%d" % (reb_id, ccd_num)
        command = "getChannelValue %s.CCDTemp%d" % (rebname, ccd_num)
        ccdtemp = ccs_sub.ts8.sendSynchCommand( command)
#.getResult()
#        command = "setMeasuredCCDTemperature %s %s" % (ccd_id, ccdtemp)
        command = 'ccdSpecificHeaderKeywords MeasuredTemperature %s %s' % (ccd_id, sensor.sensor_id)
#fix        ccs_sub.ts8.sendSynchCommand( command)

        # Set the BSS voltage.
#        command = "getChannelValue REB%s.hvbias.VbefSwch"  % reb_id
#        hv = ccs_sub.rebps.sendSynchCommand( command).getResult()
#        command = "setMeasuredCCDBSS %s %s" % (ccd_id, hv)
#        ccs_sub.ts8.sendSynchCommand( command)
