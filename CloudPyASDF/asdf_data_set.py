#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:copyright:
    Yiyu Ni (niyiyu@uw.edu), 2021
:license:
    MIT License
"""

# import sliderule
import numpy as np
import h5py
import obspy
import datetime
import io

class CloudASDFDataSet(object):
    def __init__(self, resource, format, path, region = "us-west-2", endpoint = "https://s3.us-west-2.amazonaws.com"):
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
    


    def read_trace(self, dataset):
        '''

        '''
        _waveform = self._file.read(dataset, 0, 0, -1)
        _tr = obspy.Trace(data = np.array(_waveform))

        starttime = datetime.datetime.strptime(dataset.split("__")[1], "%Y-%m-%dT%H:%M:%S")
        endtime = datetime.datetime.strptime(dataset.split("__")[2], "%Y-%m-%dT%H:%M:%S")
        delta = (endtime - starttime).total_seconds()
        sampling_rate = (len(_tr.data)-1)/delta

        setattr(_tr.stats, "starttime", starttime)
        setattr(_tr.stats, "sampling_rate", sampling_rate)

        return _tr



    def read_events(self, dataset):
        '''
        '''
        # We do a serial read to the EventXML object ...
        eventxml = self._file.read("/QuakeML", 0, 0, -1)

        # ... and convert them into ObsPy StationXML object.
        eventxml = np.array(eventxml, dtype = 'int8')
        eventxml = eventxml[()].tobytes().strip()
        return obspy.read_events(io.BytesIO(eventxml), format="quakeml")



    def read_stationxml(self, dataet):
        # We do a serial read to the StationXML object of UW.OSD object...
        stationxml = self._file.read("/Waveforms/UW.OSD/StationXML", 0, 0, -1)

        # ... and convert them into ObsPy StationXML object.
        stationxml = np.array(stationxml, dtype = 'int8')
        stationxml = stationxml[()].tobytes().strip()
        return obspy.read_inventory(io.BytesIO(stationxml), format="stationxml")



    def get_asdfdict(self):
        '''
            Get ASDF dictionary that describes the file structure.

            Args:
                None
            
            Returns:
                dict: ASDF dictionary object that describe H5 structure.
        '''
        _string = self._file.read("/AuxiliaryData/ASDFDict", 0, 0, -1)
        self.ASDFDict = eval(np.array(_string, dtype = 'int8')[()].tobytes().strip().decode("utf-8"))
        return self.ASDFDict

    

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
        return "CloudASDFDataSet object"


def traverse_dataset(cloudasdfdataset):
    asdfdict = cloudasdfdataset.get_asdfdict()
    stations = asdfdict['Waveforms'].keys()
    for sta in stations:
        for tag in asdfdict['Waveforms'][sta]:
            dataset = '/Waveforms/' + sta + '/' + tag
            if tag == 'StationXML':
                print(cloudasdfdataset.read_stationxml(dataset))
            else:
                print(cloudasdfdataset.read_trace(dataset))