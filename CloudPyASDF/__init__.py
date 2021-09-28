#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright
#    Yiyu Ni (niyiyu@uw.edu), 2021
# License
#    MIT License

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

from .asdf_data_set import CloudASDFDataSet
from .exceptions import (
    ASDFDictNotInFileError,
    AWSCredentialError
)

import os

cred = os.environ['HOME'] + "/.aws/credentials"
if os.path.exists(cred):
    print("AWS credential exists.")
else:
    raise AWSCredentialError("\nCheck credentials.\nSearching at %s" % cred)