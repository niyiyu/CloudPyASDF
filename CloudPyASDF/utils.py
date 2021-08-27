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



def dump_group_dict(from_group, to_group, name, compress = True):
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