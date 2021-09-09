#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright
#    Yiyu Ni (niyiyu@uw.edu), 2021
# License
#    MIT License

try:
    import sliderule
except:
    print("Check sliderule python-binding as it is not imported.")

import numpy as np
import h5py
import obspy
import datetime
import io

from .utils import (
    read_dict,
    StationAccessor,
    AuxiliaryDataGroupAccessor
)

from .exceptions import (
    CloudASDFValueError,
    ASDFDictNotInFileError
)

class CloudASDFDataSet(object):
    def __init__(self, resource, format, path, region = "us-west-2", endpoint = "https://s3.us-west-2.amazonaws.com", 
                asdfdict_path = "/AuxiliaryData/ASDFDict"):
        '''
            Initialization class

            Args:
                resource (str): H5 filename
                format (str): file mode, could be "file" (local file), "s3" (read from s3) or "s3cache" (download then read from local)
                path (str): file name path
                region (str): for s3 bucket region
                endpoint (str): for s3 endpoint
            
            Returns:
                h5file (sliderule.h5coro): h5coro file object

            Examples:
                >>> h5file = sliderule.h5coro("asdf.h5", "s3", "seisbasin/ASDF/", "us-west-2", "https://s3.us-west-2.amazonaws.com")
        '''

        self.resource = resource
        self.format = format
        self.path = path 
        self.region = region
        self.endpoint = endpoint

        self._file = sliderule.h5coro(self.resource, self.format, self.path, self.region, self.endpoint)

        try:
            self.read_asdfdict(path = asdfdict_path)
            self.waveforms = StationAccessor(self)
            self.auxiliary_data = AuxiliaryDataGroupAccessor(self)
        except:
            raise ASDFDictNotInFileError(
                "ASDF dictionary error. Please check asdf dict path.\n%s" % asdfdict_path
            )

        if "QuakeML" not in self.ASDFDict.keys():
            self.events = obspy.core.event.Catalog()
        else:
            self.events = self.read_events()


    def read_trace(self, dataset):
        '''
            Read waveforms from h5file into ObsPy Traces.

            Args:
                dataset (str): path to waveform data

            Example:
                >>> CloudASDFDataSet.read_trace("/Waveforms/UW.OSD/UW.OSD..EHZ__2021-01-01T00:00:00__2021-01-01T00:10:00__raw_recording")
                UW.OSD..EHZ | 2021-01-01T00:00:00.000000Z - 2021-01-01T00:10:00.000000Z | 100.0 Hz, 60001 samples
        '''
        _j = dataset.split('/')[-1]
        _i = _j.split('.')
        _waveform = self._file.read(dataset, 0, 0, -1)
        _tr = obspy.Trace(data = np.array(_waveform))

        starttime = datetime.datetime.strptime(dataset.split("__")[1], "%Y-%m-%dT%H:%M:%S")
        endtime = datetime.datetime.strptime(dataset.split("__")[2], "%Y-%m-%dT%H:%M:%S")
        delta = (endtime - starttime).total_seconds()
        sampling_rate = (len(_tr.data)-1)/delta

        setattr(_tr.stats, "starttime", starttime)
        setattr(_tr.stats, "sampling_rate", sampling_rate)
        setattr(_tr.stats, "network", _i[0])
        setattr(_tr.stats, "station", _i[1])
        setattr(_tr.stats, "location", _i[2])
        setattr(_tr.stats, "channel", _i[3][:3])

        return _tr

    def read_events(self):
        '''
        '''
        # We do a serial read to the EventXML object ...
        eventxml = self._file.read("/QuakeML", 0, 0, -1)

        # ... and convert them into ObsPy StationXML object.
        eventxml = np.array(eventxml, dtype = 'int8')
        eventxml = eventxml[()].tobytes().strip()
        return obspy.read_events(io.BytesIO(eventxml), format="quakeml")

    def read_stationxml(self, dataset, raw = False):
        # We do a serial read to the StationXML object of UW.OSD object...
        stationxml = self._file.read(dataset, 0, 0, -1)

        # ... and convert them into ObsPy StationXML object.
        stationxml = np.array(stationxml, dtype = 'int8')
        stationxml = stationxml[()].tobytes().strip()
        if raw:
            return stationxml
        else:
            return obspy.read_inventory(io.BytesIO(stationxml), format="stationxml")

    def read_asdfdict(self, path = "/AuxiliaryData/ASDFDict"):
        '''
            Get ASDF dictionary that describes the file structure.

            Args:
                path (str): default asdf dictionary is stored in /AuxiliaryData/ASDFDict
            
            Returns:
                dict: ASDF dictionary object that describe H5 structure.
        '''
        try:
            asdfdict = read_dict(self._file, path)['']
            self.ASDFDict = asdfdict

        except:
            raise CloudASDFValueError(
                "Check ASDF dictionary position."
                "\n\t Default position is /AuxiliaryData/ASDFDict"
                "\n\t ... and we're searching at %s" % path
            )
            self.ASDFDict = None

    def __dir__(self):
        '''
            Intrinsic function for accessing the attricbut/method with Tab.
        '''
        return ["abc", "acscc"]

    def __str__(self):
        '''
            Intrinsic function for printing the summary.

            Examples:
                >>> print(cloudasdfdataset)
        '''

        s = "CloudASDFDataSet object\n============="
        s += "\nPath: %s/%s" % (self.path, self.resource)
        if self.format == "file":
            s += "\nFormat: local file"
        elif self.format == "s3": 
            s += "\nFormat: AWS S3 file"
            s += "\nRegion: %s" % self.region
            s += "\nEndpoint: %s" % self.endpoint

        s += "\n============="
        if self.ASDFDict is None:
            s += "\nASDF file structure unknown. Please get file structure first."
            return s
        else:
            s += "\nASDF file structure is avaiable."
            s += "\nContains %i event(s)" % len(self.events)
            s += "\nContains waveform data from %i station(s)" % len(self.waveforms)
            if len(self.auxiliary_data):
                s += "\nContains %i type(s) of auxiliary data: %s" %(
                    len(self.auxiliary_data),
                    ", ".join(self.auxiliary_data.list()),
                )

            return s
    
    def _repr_pretty_(self, p, cycle):  # pragma: no cover
        """
            Show class name as ASDF data set information

            Example:
                >>> ds
                CloudASDFDataSet object
                =============
                Path: ../testfile/smallasdf.h5
                Format: local file
                =============
                ASDF file structure is avaiable.
                Contains TODO event(s)
                Contains waveform data from 2 station(s)
                Contains TODO type(s) of auxiliary data: TODO

        """
        p.text(self.__str__())

def traverse_dataset(cloudasdfdataset):
    asdfdict = cloudasdfdataset.read_asdfdict()
    stations = asdfdict['Waveforms'].keys()
    for sta in stations:
        for tag in asdfdict['Waveforms'][sta]:
            dataset = '/Waveforms/' + sta + '/' + tag
            if tag == 'StationXML':
                print(cloudasdfdataset.read_stationxml(dataset))
            else:
                print(cloudasdfdataset.read_trace(dataset))