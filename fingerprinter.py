from pygments.lexer import using

__author__ = 'james'


import wave, os
import numpy as np
import hashlib

from scipy.ndimage.morphology import generate_binary_structure, iterate_structure, binary_erosion
from scipy.ndimage.filters import maximum_filter
from scipy.io import wavfile
from matplotlib import mlab
from matplotlib import pyplot

class Fingerprinter(object):
    """
        performs audio fingerprinting ops
    """

    #consts
    IDX_FREQ_I = 0
    IDX_FREQ_J = 1

    DEFAULT_FS = 44100
    DEFAULT_WINDOW_SIZE = 4096
    DEFAULT_OVERLAP_RATIO = 0.5
    DEFAULT_FAN_VALUE = 15

    DEFAULT_AMP_MIN = 10
    PEAK_NEIGHBORHOOD_SIZE = 20
    MIN_HASH_TIME_DELTA = 0

    def __init__(self,
                 Fs=DEFAULT_FS,
                 wsize=DEFAULT_WINDOW_SIZE,
                 wratio=DEFAULT_OVERLAP_RATIO,
                 fan_value=DEFAULT_FAN_VALUE,
                 amp_min=DEFAULT_AMP_MIN):

        self.Fs = Fs
        self.dt = 1.0 / self.Fs
        self.window_size = wsize
        self.window_overlap_ratio = wratio
        self.fan_value = fan_value
        self.amp_min = amp_min
        self.noverlap = int(self.window_size * self.window_overlap_ratio)

    def extract_channels(self, path):
        """
            extract audio channels
        """
        channels = []
        Fs, frames = wavfile.read(path)
        wave_obj = wave.open(path)
        nchannels, sampwidth, framerate, num_frames, comptype, compname = wave_obj.getparams()
        try:
            assert Fs == self.Fs #TODO dig more on assertion
        except AssertionError, e:
            self.Fs = Fs
            print "new Fs: ", self.Fs
            pass

        for channel in range(nchannels):
            channels.append(frames[:, channel])
        return channels

    def fingerprint(self, samples, song_id):
        """
            generate fingerprints of known songs
        """
        hashes = self.process_channel(samples, song_id=song_id)

        open('/home/james/python-include/numpy/fingerprint_hashes_SHA1.txt', 'w+').writelines(str(hashes))
        #np.savetxt('/home/james/python-include/numpy/fingerprint_hashes_SHA1.txt', np.array(hashes))
        print "Hashes: ", len(hashes)

    def process_channel(self, channel_samples, song_id=None):
        """
            FFT channel_samples, log transform FFT output, find local maxima,
            return locally sensitive hashes - sha1
        """
        #FFT signal extract freq/time components
        arr2d = mlab.specgram(
            channel_samples,
            NFFT=self.window_size,
            Fs=self.Fs,
            window=mlab.window_hanning,
            noverlap=self.noverlap)[0]

        #applying log transform since specgram() coz return linear arr
        arr2d = 10 * np.log10(arr2d)

        #get local maxima pts
        local_maxima = self.get_2D_peaks(arr2d, song_id, True)
        return self.generate_hashes(local_maxima, song_id=song_id)

    def get_2D_peaks(self, arr2D, song_id=None, plot=True):
        """
            get local maxima pts
            http://docs.scipy.org/doc/scipy/reference/generated/scipy.ndimage.morphology.iterate_structure.html#scipy.ndimage.morphology.iterate_structure
        """
        #TODO add tenary operator here
        if song_id is None:
            song_id = ''

        struct = generate_binary_structure(2, 1)
        neighborhood = iterate_structure(struct, Fingerprinter.PEAK_NEIGHBORHOOD_SIZE)

        #find local maxima using filter shape
        local_max = maximum_filter(arr2D, footprint=neighborhood) == arr2D
        background = (arr2D == 0)
        eroded_background = binary_erosion(background, structure=neighborhood, border_value=1)
        detected_peaks = local_max - eroded_background #Trues at peak level

        #extract peaks
        amps = arr2D[detected_peaks]
        j, i = np.where(detected_peaks)

        #filter peaks
        amps = amps.flatten()
        peaks = zip(i, j, amps) #freq, time, amp
        peaks_filtered = [x for x in peaks if x[2] > self.amp_min]

        #indices for freq & time
        frequency_idx = [x[1] for x in peaks_filtered]
        time_idx = [x[0] for x in peaks_filtered]

        if plot:
            #scatter peaks
            fig, ax = pyplot.subplots()
            ax.imshow(arr2D)
            ax.scatter(time_idx, frequency_idx)
            #pyplot.xlim([0, 1000])
            ax.set_xlabel('Time')
            ax.set_ylabel('Frequency')
            ax.autoscale(tight=True)
            #pyplot.ticklabel_format(style='sci', axis='x', scilimits=(-3,4))
            #ax.set_xticks(np.arange(0, 1000, 50))
            #ax.set_xticklabels(np.arange(0, 1000, 10))
            fig.set_size_inches(88.5, 60.5)
            ax.set_title('Spectrogram of %s' % song_id)
            pyplot.gca().invert_yaxis()
            pyplot.show()

        return zip(frequency_idx, time_idx)

    def generate_hashes(self, peaks, song_id=None):
        """
            Hash struct:
            sha1-hash[0:20] song_id, time_offset
            [(b1b3773a05c0ed0176787a4f1574ff0075f7521e),  (12, 2)), ... ]
        """
        #use sets to avoid rehashing same pairs
        fingerprinted = set()
        hashes = []

        for i in range(len(peaks)):
            for j in range(self.fan_value):
                if i+j < len(peaks) and not (i, i+j) in fingerprinted:

                    freq1 = peaks[i][Fingerprinter.IDX_FREQ_I]
                    freq2 = peaks[i+j][Fingerprinter.IDX_FREQ_I]
                    t1 = peaks[i][Fingerprinter.IDX_FREQ_J]
                    t2 = peaks[i+j][Fingerprinter.IDX_FREQ_J]
                    t_delta = t2 - t1

                    if t_delta >= Fingerprinter.MIN_HASH_TIME_DELTA:
                        h = hashlib.sha1("%s|%s|%s" % (str(freq1), str(freq2), str(t_delta)))
                        hashes.append((h.hexdigest()[0:20], (song_id, t1)))

                    #add to to fpr set
                    fingerprinted.add((i, i+j))
        return hashes