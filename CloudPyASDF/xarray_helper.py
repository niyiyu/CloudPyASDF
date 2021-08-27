#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright
#    Yiyu Ni (niyiyu@uw.edu), 2021
# License
#    MIT License

import xarray
import h5py
import numpy as np
# from .utils import ()



def embed_xarray(ds, group):
    '''
        Embed a Xarray object into HDF5 group/dataset structure.
        To avoid attribute reading with H5coro, attributes are stored 
        as dictionary-string rather than HDF5 native attribute.

        Args:
            ds (xarray.core.dataset.Dataset): Xarray dataset
            group (h5py._hl.group.Group): hdf5 group to store xarray dataset
    '''

    # Check argument type...
    assert isinstance(ds, xarray.core.dataset.Dataset), \
            "Expect " + str(xarray.core.dataset.Dataset) + "\n\t\tGet " + str(type(ds))

    assert isinstance(group, h5py._hl.group.Group), \
            "Expect " + str(h5py._hl.group.Group) + "\n\t\tGet " + str(type(group))

    xr_obj = {'data_vars': list(ds.data_vars),
              'coords':    list(ds.coords),
              'dims': dict(ds.dims),
              'attrs': dict(ds.attrs)}

    xroot = group.create_group("XR")
    for _obj, _val in xr_obj.items():
        if isinstance(_val, list):
            xroot.create_group(_obj)

            for _v in _val:
                # For each variable, create a group to store data and varibale's attributes
                xroot[_obj].create_group(_v)

                # Some data may have datetime type, thus conversion is required...
                if not np.issubdtype(ds[_v].data.dtype, np.number):
                    xroot[_obj][_v].create_dataset('data', 
                            data = ds[_v].data.reshape([-1]).astype('float64'))
                else:
                    xroot[_obj][_v].create_dataset('data', 
                            data = ds[_v].data.reshape([-1]))
                xroot[_obj][_v].create_dataset('attrs', 
                            data = np.fromstring(str(ds[_v].attrs), dtype = 'int8'))

        elif isinstance(_val, dict):
            xroot.create_dataset(_obj, data = np.fromstring(str(_val), dtype = 'int8'))