#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright
#    Yiyu Ni (niyiyu@uw.edu), 2021
# License
#    MIT License

import h5py
import xarray
import numpy as np
import weakref
import obspy
import io


from .exceptions import (
    WaveformNotInFileError
)
from .inventory_utils import get_coordinates

def gen_group_dict(group):
    '''
        Recursively traverse a group, generate and return the structure as a dictionary.

        Args:
            group (h5py._hl.group.Group): hdf5 group to store xarray dataset.

        Returns:
            dic (dict): the dictionary tht describe the group's strcuture.
    '''

    # get the group name and avoid full path
    _n = group.name.split('/')[-1]                   
    
    if isinstance(group, h5py._hl.dataset.Dataset):
        # return dataset name directly
        return {_n: None}
    else:
        # recrusively seek into groups
        dic = {_n: {}}                              
        for _obj in set(group.keys()):
            dic[_n].update(gen_group_dict(group[_obj]))
        return dic



def dump_dict(from_group, to_group, name = "dict", compress = True):
    '''
        Traverse a group, and store a dictionary in the group that describe 
        the group's structure.

        Compression is done by default as when gzip is used (LZW algorithm), 
        the string could be dramatically shortened. This is especially useful
        for file with small trace/network/station name heterogenity.
        
        Args:
            from_group (h5py._hl.group.Group): from which group the dict is dumped
            to_group (h5py._hl.group.Group): to where the dict is stored
            name (str): dataset name taht store the dictionary
            compress (bool): decide whether to compress string

        Examples:
            >>> CloudPyASDF.utils.dump_dict(ds['/'], ds['/AuxiliaryData'], "ASDFDict")
    '''
    import gzip

    # Check argument type...
    for _g in [from_group, to_group]:
        assert isinstance(_g, h5py._hl.group.Group), \
            "Expect " + str(h5py._hl.group.Group) + "\n\t\tGet " + str(type(_g))
    
    s = str(gen_group_dict(from_group)).encode()

    if compress:
        comp_s = gzip.compress(s)
        to_group.create_dataset(name, 
            data = np.frombuffer(comp_s, dtype = 'uint8'),
            maxshape = (None, ))
    else:    
        to_group.create_dataset(name, 
            data = np.frombuffer(s, dtype = 'uint8'),
            maxshape = (None, ))



def read_dict(h5file, path):
    """
        Read and decode into dictionary from HDF5 file with h5coro.

        Args:
            h5file (sliderule.h5coro): h5coro file object
            path (str): a hdf5 dataset path, looks like "/group/subgroup/dataset"

        Return:
            dic (dict): the dictionary tht describe the group's strcuture.
    """
    import gzip
    from numpy import array as array
    from numpy import float32 as float32

    _str = h5file.read(path)
    _str = np.array(_str, dtype = 'uint8')
    _str_byte = _read_string_array(_str)

    try:
        # Decompress string
        _str_byte = gzip.decompress(_str_byte)
    except:
        # Perhaps compression is surpressed?
        pass

    dic = eval(_str_byte)
    
    return dic

def readp_dict(h5file, readp_list, prefix = '', suffix = ''):
    """
        Read and decode into dictionary from HDF5 file with h5coro in multi-thread.

        Args:
            h5file (sliderule.h5coro): h5coro file object
            readp_list (list): see h5coro.readp()
            prefix (str): global prefix for readp_list's path
            suffix (str): global suffix for readp_list's path

    """
    # For some modules like xarray, it just don't show full name of arrays and data type.
    # This is required to avoid eval error.
    import gzip
    from numpy import array as array
    from numpy import float32 as float32

    _readp_list = [[prefix + i[0], i[1], i[2], i[3]] for i in readp_list]
    _readp_list = [[i[0] + suffix, i[1], i[2], i[3]] for i in _readp_list]
    _dict = h5file.readp(_readp_list)
    
    new_dict = {}
    for item in readp_list:
        _str = np.array(_dict[prefix + item[0] + suffix], dtype = 'uint8')
        _str_byte = _read_string_array(_str)

        try:
            # Decompress string
            _str_byte = gzip.decompress(_str_byte)
        except:
            # Perhaps compression is surpressed?
            pass

        new_dict[item[0]] = eval(_str_byte)
    return new_dict

def readp_array(h5file, readp_list, prefix = '', suffix = ''):
    """
        Read and decode into array from HDF5 file with h5coro in multi-thread.
        Only different from readp_dict is skipping string parsing.

        Args:
            h5file (sliderule.h5coro): h5coro file object
            readp_list (list): see h5coro.readp()
            prefix (str): global prefix for readp_list's path
            suffix (str): global suffix for readp_list's path
    """
    _readp_list = [[prefix + i[0] + suffix, i[1], i[2], i[3]] for i in readp_list]
    _dict = h5file.readp(_readp_list)
    
    _new_dict = {}
    for item in readp_list:
        _new_dict[item[0]] = _dict[prefix + item[0] + suffix]

    return _new_dict

def _read_string_array(array):
    """
        Helper function taking a string data and preparing it so it can be
        read to other object.

        As string array are stored as ASCII-int in HDF5, decode is required.

        data (numpy.array): data array that encodes a string

        Args:
            array (numpy.array):
        
        Returns:
            bytes (bytes):
    """
    return array[()].tobytes().strip()



class StationAccessor(object):
    """
        Helper class to facilitate access to the waveforms and stations.
    """

    def __init__(self, cloudasdfdataset):
        self.data_set = weakref.ref(cloudasdfdataset)
        self.station_dict = self.data_set().ASDFDict['Waveforms']

    def list(self):
        return sorted([_i for _i in self.station_dict.keys()])
    
    def __dir__(self):
        """
            Examples:
                >>> dir(ds.waveforms)
        """
        return [_i.replace(".", "_") for _i in self.list()]

    def __len__(self):
        """
            Examples:
                >>> len(ds.waveforms)
        """
        return len(self.list())
    
    def __iter__(self):
        """
            Examples:
                >>> [i for i in ds.waveforms]
        """
        for _i in self.list():
            yield self.__getattr__(_i)

    def __getattr__(self, item, replace = True):
        """
            Examples:
                >>> ds.waveforms.UW_SEP
        """
        if replace:
            item = str(item).replace("_", ".")
        if item not in self.list():
            raise AttributeError("Attribute '%s' not found." % item)
        return WaveformAccessor(item, self.data_set())

    def __getitem__(self, item):
        # Item access with replaced underscore and without. This is not
        # strictly valid ASDF but it helps to be flexible.
        try:
            return self.__getattr__(item, replace=False)
        except AttributeError:
            try:
                return self.__getattr__(item, replace=True)
            except AttributeError as e:
                raise KeyError(str(e))



class WaveformAccessor(object):
    """
        Helper class facilitating access to the actual waveforms and stations.
    """
    def __init__(self, station_name, data_set):
        # Use weak references to not have any dangling references to the HDF5
        # file around.
        self.station_name = station_name
        self.data_set = weakref.ref(data_set)
        self.waveform_dict = self.data_set().ASDFDict['Waveforms'][station_name]

    def get_waveform_tags(self):
        """
            Get all available waveform tags for this station.
        """
        return sorted(
            set(_i.split("__")[-1] for _i in self.list() if _i != "StationXML")
        )

    @property
    def coordinates(self):
        """
            Get coordinates of the station if any.
        """
        coords = self.__get_coordinates(level = "station")
        # Such a file actually cannot be created with pyasdf but maybe with
        # other codes. Thus we skip the coverage here.
        if self.station_name not in coords:  # pragma: no cover
            raise ASDFValueError(
                "StationXML file has no coordinates for "
                "station '%s'." % self.station_name
            )
        return coords[self.station_name]

    @property
    def channel_coordinates(self):
        """
            Get coordinates of the station at the channel level if any.
        """
        coords = self.__get_coordinates(level = "channel")
        # Filter to only keep channels with the current station name.
        coords = {
            key: value
            for key, value in coords.items()
            if key.startswith(self.station_name + ".")
        }
        if not coords:
            raise ASDFValueError(
                "StationXML file has no coordinates at "
                "the channel level for station '%s'." % self.station_name
            )
        return coords
    
    def __get_coordinates(self, level):
        """
            Helper function.
        """
        if "StationXML" not in self.waveform_dict:
            raise NoStationXMLForStation(
                "Station '%s' has no StationXML " "file." % self.station_name
            )
        try:
            with io.BytesIO(self.data_set().read_stationxml(
                "/Waveforms/" + self.station_name + "/StationXML", True
            )) as buf:
                coordinates = get_coordinates(buf, level = level)
        finally:
            pass

        return coordinates

    def __getattr__(self, item):
        return self.get_item(item = item)

    def filter_data(self, item):
        """
            Internal filtering for item access and deletion.
        """
        # StationXML access.
        if item == "StationXML":
            return [item]

        _l = self.list()

        # Single trace access
        # items would be something looks like 
        # 'UW.OSD..EHZ__2021-01-01T00:00:00__2021-01-01T01:00:00__raw_recording'
        if item in _l:
            return [item]

        # Tag access. '__' is always contained in a trace's name.
        elif "__" not in item:
            keys = [_i for _i in self.list() if _i.endswith("__" + item)]

            if not keys:
                raise WaveformNotInFileError(
                    "Tag '%s' not part of the data set for station '%s'."
                    % (item, self.station_name)
                )
            return keys

        raise AttributeError("Item '%s' not found." % item)


    def get_item(self, item, starttime = None, endtime = None):
        items = self.filter_data(item)
        # StationXML access.
        if items == ["StationXML"]:
            station = self.data_set().read_stationxml("/Waveforms/%s/StationXML" % self.station_name)
            if station is None:
                raise AttributeError(
                    "'%s' object has no attribute '%s'"
                    % (self.__class__.__name__, str(item))
                )
            return station
        # # Get an estimate of the total require memory. But only estimate it
        # # if the file is actually larger than the memory as the test is fairly
        # # expensive but we don't really care for small files.
        # if (
        #     self.__data_set().filesize
        #     > self.__data_set().single_item_read_limit_in_mb * 1024 ** 2
        # ):
        #     total_size = sum(
        #         [
        #             self.__data_set()._get_idx_and_size_estimate(
        #                 _i, starttime=starttime, endtime=endtime
        #             )[3]
        #             for _i in items
        #         ]
        #     )

        #     # Raise an error to not read an extreme amount of data into memory.
        #     if total_size > self.__data_set().single_item_read_limit_in_mb:
        #         msg = (
        #             "All waveforms for station '%s' and item '%s' would "
        #             "require '%.2f MB of memory. The current limit is %.2f "
        #             "MB. Adjust by setting "
        #             "'ASDFDataSet.single_item_read_limit_in_mb' or use a "
        #             "different method to read the waveform data."
        #             % (
        #                 self._station_name,
        #                 item,
        #                 total_size,
        #                 self.__data_set().single_item_read_limit_in_mb,
        #             )
        #         )
        #         raise ASDFValueError(msg)

        traces = [self.data_set().read_trace("/Waveforms/" + self.station_name + "/" + _i) for _i in items]
        return obspy.Stream(traces = traces)

    def list(self):
        """
            Get a list of all data sets for this station.
        """
        return sorted(
            self.waveform_dict.keys()
        )

    def __dir__(self):
        """
            The dir method will list all this object's methods, the StationXML
            if it has one, and all tags.
        """
        # Python 3.
        if hasattr(object, "__dir__"):  # pragma: no cover
            directory = object.__dir__(self)

        directory.extend(self.get_waveform_tags())
        if "StationXML" in self.list():
            directory.append("StationXML")
        directory.extend(
            ["station_name", "coordinates", "channel_coordinates"]
        )
        return sorted(set(directory))

    def __str__(self):
        contents = self.__dir__()
        waveform_contents = sorted(self.get_waveform_tags())

        ret_str = (
            "Contents of the data set for station {station}:\n"
            "    - {station_xml}\n"
            "    - {count} Waveform Tag(s):\n"
            "        {waveforms}"
        )
        return ret_str.format(
            station = self.station_name,
            station_xml = "Has a StationXML file"
            if "StationXML" in contents
            else "Has no StationXML file",
            count = len(waveform_contents),
            waveforms = "\n        ".join(waveform_contents),
        )
    
    def _repr_pretty_(self, p, cycle):  # pragma: no cover
        """
            Show class name as waveform information rather than WaveformAccessor

            Example:
                >>> ds.waveforms.UW_OSD
                Contents of the data set for station UW.OSD:
                - Has a StationXML file
                - 1 Waveform Tag(s):
                    raw_recording
        """
        p.text(self.__str__())


class AuxiliaryDataGroupAccessor(object):
    def __init__(self, data_set):
        # Use weak references to not have any dangling references to the HDF5
        # file around.
        self.data_set = weakref.ref(data_set)
        self.auxiliarydata_dict = self.data_set().ASDFDict['AuxiliaryData']

    def list(self):
        return sorted(self.auxiliarydata_dict.keys())

    def __len__(self):
        return len(self.list())