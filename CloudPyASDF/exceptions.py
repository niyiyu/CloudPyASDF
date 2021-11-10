#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright
#    Yiyu Ni (niyiyu@uw.edu), 2021
# License
#    MIT License

class CloudASDFValueError(ValueError):
    pass

class WaveformNotInFileError(ValueError):
    """
        Raised when a non-existent waveform is accessed.
    """
    pass

class ASDFDictNotInFileError(ValueError):
    """
        Raised when ASDF Dictionary is either not exist or parsed with error.
    """
    pass

class NoStationXMLForStation(ValueError):
    """
        Raised when station continas no XML information.
    """
    pass

class ASDFValueError(ValueError):
    pass

class AWSCredentialError(ValueError):
    pass