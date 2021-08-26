#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
:copyright:
    Yiyu Ni (niyiyu@uw.edu), 2021
:license:
    MIT License
"""

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



def dump_group_dict(group, name):
    '''
        Traverse a group, and store a dictionary in the group that describe 
        the group's structure.
        
        Args:
            group (h5py._hl.group.Group): hdf5 group to store xarray dataset
            name (str): dataset name taht store the dictionary.
    '''

    # Check argument type...
    assert isinstance(group, h5py._hl.group.Group), \
        "Expect " + str(h5py._hl.group.Group) + "\n\t\tGet " + str(type(group))
    
    dic = gen_group_dict(group)
    group.create_dataset(name, 
        data = np.fromstring(str(dic), dtype = 'int8'),
        maxshape = (None, ))



def _read_string_array(data):
    """
        Helper function taking a string data and preparing it so it can be
        read to other object.

        As string array are stored as ASCII-int in HDF5, decode is required.

        data (numpy.array): data array that encodes a string
    """
    return data[()].tobytes().strip()



def read_dict(h5file, path):
    """
        Read and decode into dictionary from HDF5 file with h5coro.

        Args:
            h5file (sliderule.h5coro): h5coro file object
            path (str): a hdf5 dataset path, looks like "/group/subgroup/dataset"

        Return:
            _dic (dict): the dictionary tht describe the group's strcuture.
    """
    from numpy import array as array
    from numpy import float32 as float32

    _str = h5file.read(path)
    _str = np.array(_str, dtype = 'int8')
    _dic = eval(_read_string_array(_str))
    return _dic



def readp_dict(h5file, readp_list, prefix = '', suffix = ''):
    """
        Read and decode into dictionary from HDF5 file with h5coro in multi-thread.

        Args:
            h5file (sliderule.h5coro): h5coro file object
            readp_list (list): see h5coro.readp()
            prefix (str): global prefix for readp_list's path
            suffix (str): global suffix for readp_list's path

    """
    from numpy import array as array
    from numpy import float32 as float32

    _readp_list = [[prefix + i[0], i[1], i[2], i[3]] for i in readp_list]
    _readp_list = [[i[0] + suffix, i[1], i[2], i[3]] for i in _readp_list]
    _dict = h5file.readp(_readp_list)
    
    _new_dict = {}
    for item in readp_list:
        _new_dict[item[0]] = \
            eval(_read_string_array(np.array(
                _dict[prefix + item[0] + suffix], dtype = 'int8')))

    return _new_dict




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