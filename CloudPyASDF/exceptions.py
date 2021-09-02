#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright
#    Yiyu Ni (niyiyu@uw.edu), 2021
# License
#    MIT License

class CloudASDFValueError(ValueError):
    pass

class WaveformNotInFileException(ValueError):
    """
        Raised when a non-existent waveform is accessed.
    """

    pass