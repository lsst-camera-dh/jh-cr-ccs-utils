"""
Module to produce QA plots for online EO data acquistion.
"""
from __future__ import print_function
import os
import glob
from collections import OrderedDict
import datetime
import functools
import numpy as np
import astropy.io.fits as fitsio
import astropy.time
import pylab
from matplotlib.dates import DateFormatter

def obs_time(infile, method='filename_timestamp'):
    if method == 'mjd_obs':
        with fitsio.open(infile) as hdu_list:
            mjd_obs = hdu_list[0].header['MJD-OBS']
        return astropy.time.Time(mjd_obs, format='mjd', scale='utc')
    elif method == 'posix_timestamp':
        return datetime.datetime.fromtimestamp(os.path.getctime(infile))
    elif method == 'filename_timestamp':
        ts = infile[-len('YYYYMMDDHHMMSS.fits'):-len('.fits')]
        return datetime.datetime(int(ts[:4]), int(ts[4:6]), int(ts[6:8]),
                                 int(ts[8:10]), int(ts[10:12]), int(ts[12:14]))
    else:
        raise RuntimeError("Unrecognized file obs_time method")

def obs_time_cmp(file1, file2):
    t1 = obs_time(file1)
    t2 = obs_time(file2)
    if t1 < t2:
        return -1
    elif t2 > t1:
        return 1
    return 0

def annotate_acq(start_time, test_type, yposfrac=0.8, xoffset=0,
                 color='k', size=10):
    ymin, ymax = pylab.axis()[2:]
    try:
        xpos = start_time + xoffset
    except TypeError:
        xpos = start_time - datetime.timedelta(minutes=xoffset)
    ypos = yposfrac*(ymax - ymin) + ymin
    pylab.plot([start_time, start_time], [ymin, ymax], 'r:')
    pylab.annotate(" " + test_type, (xpos, ypos), rotation='-90',
                   horizontalalignment='left', color=color, size=size)

class Trending(object):
    def __init__(self, ylabel):
        self.ylabel = ylabel
        self.test_types = OrderedDict()
    def add_test_type(self, mjd_obs, test_type):
        self.test_types[mjd_obs] = test_type
    def plot_dates(self, **kwds):
        raise NotImplementedError("subclasses need to implement this method.")
    def plot(self, **kwds):
        try:
            show_xlabels = kwds['show_xlabels']
        except KeyError:
            show_xlabels = False
        self.plot_dates(**kwds)
        pylab.xticks(rotation='22')
        frame = pylab.gca()
        frame.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
        pylab.ylabel(self.ylabel)
        if not show_xlabels:
            frame.axes.get_xaxis().set_ticklabels([])
        try:
            ylog = kwds['ylog']
        except KeyError:
            ylog = False
        if ylog:
            frame.axes.set_yscale('log')
        for time, test_type in self.test_types.items():
            annotate_acq(time, test_type)

class FrameTrending(Trending):
    def __init__(self, ylabel):
        super(FrameTrending, self).__init__(ylabel)
        self.times = []
        self.values = []
    def add_value(self, mjd_obs, value=None):
        self.times.append(mjd_obs)
        if value is None:
            value = len(self.times)
        self.values.append(value)
    def plot_dates(self, **kwds):
        try:
            marker = kwds['marker']
        except KeyError:
            marker = 'bo'
        pylab.plot_date(np.array(self.times), np.array(self.values), marker)

class AmpTrending(Trending):
    def __init__(self, ylabel, namps=16):
        super(AmpTrending, self).__init__(ylabel)
        self.amp_times = dict([(amp, []) for amp in range(1, namps+1)])
        self.values = dict([(amp, []) for amp in range(1, namps+1)])
    def add_value(self, amp, mjd_obs, value):
#        print("amp=",amp)
        self.amp_times[amp].append(mjd_obs)
        self.values[amp].append(value)
    def plot_dates(self, **kwds):
        if 'cmap' in kwds:
            # Set the color_cycle to 16 different colors using the
            # specifiec color map.
            color_cycle = [kwds['cmap'](int(x)) for x
                           in np.linspace(0, 255, 16)]
            pylab.rc('axes', color_cycle=color_cycle)
        zero_offsets = dict([(amp, 0) for amp in self.values.keys()])
        try:
            if kwds['subtract_t0']:
                zero_offsets = dict([(amp, self.values[amp][0]) for amp in
                                     self.values.keys()])
        except KeyError:
            pass

        for amp in self.values.keys():
            my_times = np.array(self.amp_times[amp])
            # plot_date has bug such that it does not cycle through
            # the color_cycle.  Setting the fmt keyword forces it to
            # do so.  See
            # http://stackoverflow.com/questions/17250392/setting-colors-using-color-cycle-on-date-plots-using-plot-date
            y_vals = np.array(self.values[amp]) - zero_offsets[amp]
            pylab.plot_date(my_times, y_vals, fmt='.')

class TrendingObjects(object):
    def __init__(self):
        self._dict = dict()
        self._generateDefaultObjects()
    def _generateDefaultObjects(self):
        self['File Count'] = FrameTrending('File Count')
        self['CCDTEMP'] = FrameTrending('CCD Temp (C)')
        self['EXPTIME'] = FrameTrending('exptime (s)')
        self['MONDIODE'] = FrameTrending('PD (pA)')
        self['MONOWL'] = FrameTrending('WL (nm)')
        self['FILTPOS'] = FrameTrending('Filter Position')
        self['oscan mean'] = AmpTrending('oscan mean (ADU)')
        self['oscan std'] = AmpTrending('oscan std (ADU rms)')
        self['imaging mean'] = AmpTrending('imaging area mean (ADU)')
        self['imaging std'] = AmpTrending('imaging area std (ADU rms)')
    @property
    def times(self):
        return self['CCDTEMP'].times
    def __getitem__(self, key):
        if key not in self._dict:
            self._dict[key] = FrameTrending(key)
        return self._dict[key]
    def __setitem__(self, key, value):
        self._dict[key] = value
    def add_test_type(self, time, test_type):
        for value in self._dict.values():
            value.add_test_type(time, test_type)
    def processDirectory(self, dirname, test_type, verbose=True):
        files = sorted(glob.glob(os.path.join(dirname, '*.fits')),
                       key=functools.cmp_to_key(obs_time_cmp))
        t0 = None
        for item in files:
            frame = EoAcqFrame(item)
            obs_time = frame.obs_time
            if t0 is None:
                t0 = obs_time
            if verbose:
                print(os.path.basename(item), obs_time)
            self['File Count'].add_value(obs_time)
            keywords = 'CCDTEMP EXPTIME MONDIODE MONOWL FILTPOS'
            for keyword, scale in zip(keywords.split(), (1, 1, 1e3, 1, 1)):
                try:
                    self[keyword].add_value(obs_time,
                                            frame.header_value(keyword)*scale)
                except KeyError:
                    print("Missing keyword:", keyword, "\nfrom file", item)
                    self[keyword].add_value(obs_time, 0)
            for amp in frame.overscan:
                oscan_mean = np.mean(frame.overscan[amp])
                self['oscan mean'].add_value(amp, obs_time, oscan_mean)
                self['oscan std'].add_value(amp, obs_time,
                                            np.std(frame.overscan[amp]))
                # Perform bias subtraction of overscan mean
                self['imaging mean'].add_value(amp, obs_time,
                                               np.mean(frame.imaging[amp]
                                                       - oscan_mean))
                self['imaging std'].add_value(amp, obs_time,
                                              np.std(frame.imaging[amp]))
        self.add_test_type(t0, test_type)
    def plot(self, sensor_id, ext=None, frame_id=0):
        my_frame_id = frame_id
        title = "%s: %s to %s" % (sensor_id,
                                  self.times[0].strftime('%m-%d-%y %H:%M:%S'),
                                  self.times[-1].strftime('%m-%d-%y %H:%M:%S'))
        pylab.figure(num=my_frame_id, figsize=(8.5, 11))
        my_frame_id += 1
        pylab.subplot(6, 1, 1)
        self['File Count'].plot()
        pylab.title(title)
        pylab.subplot(6, 1, 2)
        self['CCDTEMP'].plot()
        pylab.subplot(6, 1, 3)
        self['EXPTIME'].plot()
        pylab.subplot(6, 1, 4)
        self['MONDIODE'].plot()
        pylab.subplot(6, 1, 5)
        self['MONOWL'].plot()
        pylab.subplot(6, 1, 6)
        self['FILTPOS'].plot(show_xlabels=True)
        if ext is None:
            outfile = '%s_QA_monitoring.png' % sensor_id
        else:
            outfile = '%s_QA_monitoring_%s.png' % (sensor_id, ext)
        pylab.savefig(outfile)

        pylab.figure(num=my_frame_id, figsize=(8.5, 11))
        my_frame_id += 1
        pylab.subplot(4, 1, 1)
        self['oscan mean'].plot(ylog=True)
        pylab.title(title)
        pylab.subplot(4, 1, 2)
        self['oscan std'].plot(ylog=True)
        pylab.subplot(4, 1, 3)
        self['imaging mean'].plot()
        pylab.subplot(4, 1, 4)
        self['imaging std'].plot(show_xlabels=True)
        if ext is None:
            outfile = '%s_QA_imstats.png' % sensor_id
        else:
            outfile = '%s_QA_imstats_%s.png' % (sensor_id, ext)
        pylab.savefig(outfile)
        return my_frame_id

class RaftTrendingObjects(TrendingObjects):
    def __init__(self, sensor_ids):
        super(RaftTrendingObjects, self).__init__()
        self.sensor_nums = dict([(sensor_id, i+1) for i, sensor_id in
                                 enumerate(sensor_ids)])
    def processDirectory(self, dirname, test_type, verbose=True):
        files = sorted(glob.glob(os.path.join(dirname, '*.fits')),
                       key=functools.cmp_to_key(obs_time_cmp))
        t0 = None
        for item in files:
            frame = EoAcqFrame(item)
#hn            sensor_num = self.sensor_nums[frame.header_value('LSST_NUM')]
            sensor_num = frame.header_value('CCDSLOT')
            obs_time = frame.obs_time
            if t0 is None:
                t0 = obs_time
            if verbose:
                print(os.path.basename(item), obs_time)
            self['File Count'].add_value(obs_time)
            keywords = 'CCDTEMP EXPTIME MONDIODE MONOWL FILTPOS'
            for keyword, scale in zip(keywords.split(), (1, 1, 1e3, 1, 1)):
                try:
                    self[keyword].add_value(obs_time,
                                            frame.header_value(keyword)*scale)
                except KeyError:
                    print("Missing keyword:", keyword, "\nfrom file", item)
                    self[keyword].add_value(obs_time, 0)
            for amp in frame.overscan:
                oscan_mean = np.mean(frame.overscan[amp])
                self['oscan mean'].add_value(amp, obs_time, oscan_mean)
                self['oscan std'].add_value(amp, obs_time,
                                            np.std(frame.overscan[amp]))
                # Perform bias subtraction of overscan mean
                self['imaging mean'].add_value(amp, obs_time,
                                               np.mean(frame.imaging[amp]
                                                       - oscan_mean))
                self['imaging std'].add_value(amp, obs_time,
                                              np.std(frame.imaging[amp]))
        self.add_test_type(t0, test_type)


class EoAcqFrame(object):
    def __init__(self, infile, namps=16):
        self.infile = infile
        self.fits_obj = fitsio.open(infile)
        self._read_segments(namps)
        self._obs_time = None
    def header_value(self, keyword, hdu=0):
        return self.fits_obj[hdu].header[keyword]
    @property
    def obs_time(self):
        if self._obs_time is None:
            self._obs_time = obs_time(self.infile)
        return self._obs_time
    def _datasec_values(self, amp):
        value = self.header_value('DATASEC', amp)
        geom = {}
        data = value[1:-1].split(',')
        xmin, xmax = (int(x) for x in data[0].split(':'))
        geom['xmin'] = xmin
        geom['xmax'] = xmax
        ymin, ymax = (int(y) for y in data[1].split(':'))
        geom['ymin'] = ymin
        geom['ymax'] = ymax
        return geom
    def _read_segments(self, namps):
        # Read the imaging and serial overscan regions of each of the
        # segments based on the NAXIS1, NAXIS2, and DATASEC keywords
        # and store the pixel data as numpy arrays.
        self.imaging = {}
        self.overscan = {}
        for amp in range(1, namps+1):
            naxis1 = self.header_value('NAXIS1', amp)
            datasec = self._datasec_values(amp)
            segdata = self.fits_obj[amp].data
            self.imaging[amp] = segdata[datasec['ymin']-1:datasec['ymax']-1,
                                        datasec['xmin']-1:datasec['xmax']-1]
            self.overscan[amp] = segdata[datasec['ymin']-1:datasec['ymax']-1,
                                         datasec['xmax']:naxis1]

if __name__ == '__main__':
    root_dir = '/nfs/farm/g/lsst/u1/mirror/BNL-test/test/e2v-CCD/E2V-CCD250-049'
    def find_subdirs(root_dir, test_types):
        subdirs = []
        for acq in test_types:
            acq_folder = os.path.join(root_dir, acq + '_acq', 'v0')
            job_id = sorted(os.listdir(acq_folder))[0]
            subdirs.append(os.path.join(acq_folder, job_id))
        return subdirs

    test_types = 'fe55 dark flat ppump sflat qe persist'.split()
    directories = find_subdirs(root_dir, test_types)

    foo = TrendingObjects()
    for dirname, test_type in zip(directories, test_types):
        foo.processDirectory(dirname, test_type)
    foo.plot('E2V-CCD250-049', ext='foo')
