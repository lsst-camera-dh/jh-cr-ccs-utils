"""
Module to access CCS trending database via the RESTful interface.
"""
from __future__ import absolute_import, print_function
import os
import xml.dom.minidom as minidom
import time
import datetime
from collections import OrderedDict
try:
    import ConfigParser as configparser
except ImportError:
    import configparser
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mds
import requests

__all__ = ['Channels', 'RestUrl', 'TimeAxis', 'TrendingPlotter',
           'TrendingHistory', 'TrendingPoint', 'ccs_trending_config']


def ccs_trending_config(config_file):
    """
    Read the config file to get the quantities to be retrieved from
    the trending database.

    Parameters
    ----------
    config_file : str
        Configuration file with sections listing the desired trending
        quantities.

    Returns
    -------
    ConfigParser.SafeConfigParser
    """
    if not os.path.isfile(config_file):
        raise RuntimeError("ccs_trending_config -- File not found: %s"
                           % config_file)
    cp = configparser.SafeConfigParser()
    cp.optionxform = str
    cp.read(config_file)
    return cp


def date_time(msec):
    "Convert milliseconds since epoch to a datetime object."
    return datetime.datetime.fromtimestamp(msec/1e3)


class Channels(object):
    "Class to read the channels available from the CCS database"
    def __init__(self, host='tid-pc93482'):
        url = 'http://%s:8080/rest/data/dataserver/listchannels' % host
        doc = minidom.parseString(requests.get(url).text)
        self.channels = dict()
        for channel in doc.getElementsByTagName('datachannel'):
            path_elements = channel.getElementsByTagName('pathelement')
            subsystem = str(path_elements[0].childNodes[0].data)
            quantity = str(path_elements[1].childNodes[0].data)
            self.channels['/'.join((subsystem, quantity))] \
                = int(channel.getElementsByTagName('id')[0].childNodes[0].data)

    def __call__(self, subsystem, quantity):
        """
        Access to the channel id.

        Parameters
        ----------
        subsystem : str
            The CCS subsystem name, e.g., 'ccs-reb5-0'
        quantity : str
            The trending quantity name, e.g., 'REB0.Temp1'

        Returns
        -------
        int
            The channel id number.
        """
        return self.channels['/'.join((subsystem, quantity))]


class RestUrl(object):
    """
    The url of the RESTful interface server.
    """
    def __init__(self, subsystem, host='tid-pc93482', time_axis=None,
                 raw=False):
        self.subsystem = subsystem
        self.host = host
        self.channels = Channels(host=host)
        self.time_axis = time_axis
        self.raw = raw

    def __call__(self, quantity):
        id_ = self.channels(self.subsystem, quantity)
        url = 'http://%s:8080/rest/data/dataserver/data/%i' % (self.host, id_)
        if self.raw:
            url += '?flavor=raw'
        if self.time_axis is not None:
            url = self.time_axis.append_axis_info(url)
        return url


class TimeAxis(object):
    """
    Abstraction of the time axis information for CCS trending plots.
    """
    def __init__(self, dt=24., start=None, end=None, nbins=None):
        """
        Constructor for time intervals.

        Parameters
        ----------
        dt : float, optional
            Duration of time axis in hours.  Ignored if both start and
            end are given.  Default: 24.
        start : str, optional
            Start of time interval. ISO-8601 format, e.g., "2017-01-21T09:58:01"
        end : str, optional
            End of time interval. ISO-8601 format.
        nbins : int, optional
            Number of bins for time axis.  Automatically chosen by RESTful
            server if not given.
        """
        self.start = self._convert_iso_8601(start)
        self.end = self._convert_iso_8601(end)
        if self.start is None:
            if self.end is None:
                self.end = time.mktime(self.local_time().timetuple())
            self.start = self.end - dt*3600.
        elif self.end is None:
            self.end = self.start + dt*3600.
        self.nbins = nbins

    @staticmethod
    def local_time():
        return datetime.datetime.now()

    def append_axis_info(self, url):
        """Append time axis info to the REST url."""
        tokens = ['t1=%i' % (self.start*1e3), 't2=%i' % (self.end*1e3)]
        if self.nbins is not None:
            tokens.append('n=%i' % self.nbins)
        axis_info = '&'.join(tokens)
        if url.find('flavor') != -1:
            result = url + '&' + axis_info
        else:
            result = url + '?' + axis_info
        return result

    @staticmethod
    def _convert_iso_8601(iso_date):
        "Convert ISO-8601 formatted string to seconds since epoch"
        if iso_date is None:
            return None
        dt = datetime.datetime.strptime(iso_date, '%Y-%m-%dT%H:%M:%S')
        return time.mktime(dt.timetuple())


class TrendingPlotterException(RuntimeError):
    pass


class TrendingPlotter(object):
    """
    Class to plot and persist quantities from the CCS trending database.
    """
    def __init__(self, subsystem, host, time_axis=None):
        self.subsystem = subsystem
        self.host = host
        self.rest_url = RestUrl(subsystem, host=host, time_axis=time_axis)
        self.histories = OrderedDict()
        self.y_label = ''

    def read_config(self, config, section):
        """
        Read the list of quantities from the requested section of the
        config object and read the trending histories from the database.
        """
        items = OrderedDict(config.items(section))
        self.y_label = '%s (%s)' % (section, items.pop('units'))
        self._read_histories(items.values())

    def _read_histories(self, quantities):
        for quantity in quantities:
            self.histories[quantity] = TrendingHistory(self.rest_url(quantity))

    def save_file(self, outfile):
        """
        Save the trending quantities to a text file.
        """
        # Create a numpy array object with the data
        header_items = ["date", "time"]
        data = [self.histories.values()[0].x_values]
        for quantity, history in self.histories.items():
            if len(history.y_values) == len(data[0]):
                header_items.extend((quantity, 'error'))
                data.extend((history.y_values, history.y_errors))
        data = np.array(data).transpose()
        header = ' '.join(header_items)
        np.savetxt(outfile, data, fmt=['%s'] + ['%.4e']*(data.shape[1]-1),
                   header=header)

    def plot(self, x_range=None, y_range=None, y_label=None,
             title=None, legendfontsize='x-small'):
        """
        Plot the trending quantities as a function of time.
        """
        if len(self.histories) == 0:
            raise TrendingPlotterException("No trending histories loaded.")
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        for quantity, history in self.histories.items():
            ebar = ax.errorbar(mds.date2num(history.x_values), history.y_values,
                               yerr=history.y_errors, fmt='.')
            ax.plot(mds.date2num(history.x_values), history.y_values, '.',
                    color=ebar[0].get_color(), label=quantity)
        frame = plt.gca()
        frame.xaxis.set_major_formatter(mds.DateFormatter('%y-%m-%d\n%H:%M:%S'))
        ax.tick_params(axis='x', which='major', labelsize='small')
        self.set_x_range(x_range=x_range)
        self.set_y_range(y_range=y_range)
        plt.xlabel('local time')
        if y_label is None:
            y_label = self.y_label
        plt.ylabel(y_label)
        if title is None:
            title = '%s, %s' % (self.host, self.subsystem)
        plt.title(title)
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width*0.85, box.height])
        try:
            ax.legend(loc='upper left', bbox_to_anchor=(1, 1.01),
                      fontsize=legendfontsize)
        except Exception as eobj:
            print("Exception caught in TrendingPlotter.plot:")
            print(eobj)
        return fig

    @staticmethod
    def set_x_range(x_range=None):
        if x_range is None:
            return
        axis = list(plt.axis())
        axis[:2] = x_range[0], x_range[1]
        plt.axis(axis)

    @staticmethod
    def set_y_range(y_range=None):
        if y_range is None:
            return
        axis = list(plt.axis())
        axis[2:] = y_range[0], y_range[1]
        plt.axis(axis)

class TrendingHistory(object):
    def __init__(self, url):
        doc = minidom.parseString(requests.get(url).text)
        self.history = [TrendingPoint(x) for x in
                        doc.getElementsByTagName('trendingdata')]
        try:
            self.x_axis_name = self.history[0].x_axis_name
        except IndexError:
            pass
        self._x_values = ()
        self._x_errors = ()
        self._y_values = ()
        self._y_errors = ()

    @property
    def x_values(self):
        if len(self._x_values) == 0:
            self._x_values = np.array([pt.x_value for pt in self.history])
        return self._x_values

    @property
    def x_errors(self):
        if len(self._x_errors) == 0:
            self._x_errors = np.array([pt.x_error for pt in self.history])
        return self._x_errors

    @property
    def y_values(self):
        if len(self._y_values) == 0:
            self._y_values = np.array([pt.value for pt in self.history])
        return self._y_values

    @property
    def y_errors(self):
        if len(self._y_errors) == 0:
            self._y_errors = np.array([pt.rms for pt in self.history])
        return self._y_errors


class TrendingPoint(object):
    def __init__(self, element):
        datavalues = element.getElementsByTagName('datavalue')
        self.__dict__.update(dict([(x.getAttribute('name'),
                                    float(x.getAttribute('value')))
                                   for x in datavalues]))
        axisvalue = element.getElementsByTagName('axisvalue')[0]
        self.x_axis_name = axisvalue.getAttribute('name')
        if self.x_axis_name == 'time':
            convert = date_time
        else:
            convert = lambda x: x
        self.x_value = convert(float(axisvalue.getAttribute('value')))
        self.x_error = (float(axisvalue.getAttribute('upperedge')) -
                        float(axisvalue.getAttribute('loweredge')))/2e3


if __name__ == '__main__':
    plt.ion()
    host = 'tid-pc93480'
    subsystem = 'ccs-reb5-0'
    time_axis = TimeAxis(dt=24, nbins=100)

    plot_config = ccs_trending_config('../data/REB_trending_plot.cfg')
    figs = {}
    for section in plot_config.sections()[:1]:
        print("processing", section)
        plotter = TrendingPlotter(subsystem, host, time_axis=time_axis)
        plotter.read_config(plot_config, section)
        plotter.save_file('%s.txt' % section.replace(' ', '_'))
#        y_range = None
#        if section == 'Temperature':
#            y_range = (-30, 90)
#        figs[section] = plotter.plot(y_range=y_range)
##        plt.savefig('%s.png' % section.replace(' ', '_'))
