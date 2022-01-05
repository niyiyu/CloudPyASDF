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
from .utils import dump_dict


def embed_xarray(ds, group, name="XR"):
    """
    Embed a Xarray object into HDF5 group/dataset structure.
    To avoid attribute reading with H5coro, attributes are stored
    as dictionary-string rather than HDF5 native attribute.

    Args:
        ds (xarray.core.dataset.Dataset): Xarray dataset
        group (h5py._hl.group.Group): hdf5 group to store xarray dataset
    """

    # Check argument type...
    assert isinstance(ds, xarray.core.dataset.Dataset), (
        "Expect " + str(xarray.core.dataset.Dataset) + "\n\t\tGet " + str(type(ds))
    )

    assert isinstance(group, h5py._hl.group.Group), (
        "Expect " + str(h5py._hl.group.Group) + "\n\t\tGet " + str(type(group))
    )

    xr_obj = {"data": list(ds.data_vars) + list(ds.coords), "attrs": dict(ds.attrs)}

    for _k, _v in ds.dims.items():
        assert len(ds[_k].data) == _v

    xroot = group.create_group(name)
    for _obj, _val in xr_obj.items():
        if isinstance(_val, list):

            for _v in _val:
                # For each variable, create a group to store data and varibale's attributes
                xroot.create_group(_v)

                # Some data may have datetime type, thus conversion is required...
                if not np.issubdtype(ds[_v].data.dtype, np.number):
                    xroot[_v].create_dataset(
                        "data", data=ds[_v].data.reshape([-1]).astype("float64")
                    )
                else:
                    xroot[_v].create_dataset("data", data=ds[_v].data.reshape([-1]))

                _attr = ds[_v].attrs
                _attr.update(
                    {
                        "_shape": ds[_v].data.shape,
                        "_type": "coords" if _v in list(ds.coords) else "variable",
                    }
                )
                xroot[_v].create_dataset(
                    "attrs", data=np.fromstring(str(_attr), dtype="int8")
                )

        elif isinstance(_val, dict):
            xroot.create_dataset(_obj, data=np.fromstring(str(_val), dtype="int8"))

    dump_dict(xroot, xroot, "dict")
