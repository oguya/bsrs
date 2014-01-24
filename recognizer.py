__author__ = 'james'

import pyaudio
import wave
import time
import numpy as np
from scipy.io import wavfile

from fingerprinter import Fingerprinter
from Logging import Logging


class Recognizer(object):
    CHUNK = 8192
    FORMAT = pyaudio.paInt16
    CHANNELS = 2
    RATE = 44100

    def __init__(self):
        self.fingerprinter = Fingerprinter()
        self.logging = Logging()
        self.audio = pyaudio.PyAudio()

    def recognize_file(self, filename, verbose=True):
        """
            recognize unknown audio from file
        """
        print "Recognizing audio file: %s " % filename
        channels = []
        Fs, frames = wavfile.read(file=filename)
        wave_object = wave.open(filename)
        nchannels, sampwidth, framerate, num_frames, comptype, compname = wave_object.getparams()

        for channel in range(nchannels):
            channels.append(frames[:, channel])

        #get matches
        starttime = time.time()
        matches = []
        for channel in channels:
            matches.extend(self.fingerprinter.match(samples=channel))

        return self.fingerprinter.align_matches(matches=matches, starttime=starttime, verbose=verbose)

    def listen(self, seconds=30, verbose=True):
        """
            recognize unknown audio from microphone
        """
        #open audio stream
        stream = self.audio.open(format=Recognizer.FORMAT, channels=Recognizer.CHANNELS,
                                 rate=Recognizer.RATE, input=True, frames_per_buffer=Recognizer.CHUNK)

        #record
        if verbose: print "****Listening for %d seconds****" % seconds
        left, right = [], []
        for i in range(0, int(Recognizer.RATE / Recognizer.CHUNK * seconds)):
            data = stream.read(Recognizer.CHUNK)
            nums = np.fromstring(data, np.int16)
            left.extend(nums[1::2])
            right.extend(nums[0::2])

        if verbose: print ("***done listening***")

        #stop & close audio stream
        stream.stop_stream()
        stream.close()

        #match both channels
        if verbose: print ("***now matching***")
        starttime = time.time()
        matches = []
        matches.extend(self.fingerprinter.match(samples=left))
        matches.extend(self.fingerprinter.match(samples=right))

        #align matches
        return self.fingerprinter.align_matches(matches=matches, starttime=starttime, verbose=verbose)