__author__ = 'james'

import sys
import wave
from math import sin, pi
import struct
import random
import numpy as np
from itertools import *

class stats:
    """
        collect stats .. used for unit testing
    """
    DEFAULT_FS = 44100


    def __init__(self):
        """
        """
        pass

    def white_noise(self, amplitude=0.5):
        """
            generate white noise
            noise = cycle(islice(white_noise(), 44100))
        """
        return (float(amplitude) * random.uniform(-1, 1) for _ in count(0))

    def grouper(self, n, iterable, fillvalue=None):
        """
            grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
        """
        args = [iter(iterable)] * n
        return izip_longest(fillvalue=fillvalue, *args)

    def write_wavefile(self, filename, samples, nframes=None, nchannels=2, sampwidth=2, framerate=44100, bufsize=2048):
        """
            write wav module to a wave file
        """
        if nframes is None:
            nframes = -1

        w = wave.open(filename, 'w')
        w.setparams((nchannels, sampwidth, framerate, nframes, 'NONE', 'not compressed'))

        max_amplitude = float(int((2 ** (sampwidth * 8)) / 2) - 1)

        # split the samples into chunks (to reduce memory consumption and improve performance)
        for chunk in self.grouper(bufsize, samples):
            frames = ''.join(''.join(struct.pack('h', int(max_amplitude * sample)) for sample in channels) for channels in chunk if channels is not None)
            w.writeframesraw(frames)

        w.close()

        return filename

    def noise(self):
        noise = np.random.randn((44100))
        self.write_wavefile(filename="noise.wav", samples=noise)
        #write_wavefile(self, filename, samples, nframes=None, nchannels=2, sampwidth=2, framerate=44100, bufsize=2048):

    def stereo_noise(self, filename='BirdSounds/testing/white_noise_stereo.wav', duration=60):
        """
            generate stereo(2 channels) noise
        """
        noise = wave.open(filename, 'w')
        noise.setparams((2, 2, stats.DEFAULT_FS, 0, 'NONE', 'not compressed'))
        #max amplitude
        maxVol = 2**15 - 1.0
        noise_data = ''

        for i in range(0, stats.DEFAULT_FS*duration):
            #left channel
            noise_data += struct.pack('h', int(np.random.randn()*256))
            #right channel
            noise_data += struct.pack('h', int(np.random.randn()*256))

        noise.writeframes(noise_data)
        noise.close()
