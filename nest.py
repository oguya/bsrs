__author__ = 'james'

from pydub import AudioSegment
from time import time
import os
import glob

from fingerprinter import Fingerprinter
from Logging import Logging
from Fetcher import Fetcher
from Parsers import Parsers

#AUDIO_PATH = '/home/james/python-include/songs/take_a_chance_on_me.wav'
AUDIO_PATH = '/home/james/python-include/BSRS/tmp/131912.wav'


class Nest():

    def __init__(self):
        self.fingerprinter = Fingerprinter()
        self.logger = Logging()
        self.fetcher = Fetcher()
        self.parser = Parsers()

    def control_center(self):
        channels = self.fingerprinter.extract_channels(AUDIO_PATH)
        song_id = os.path.basename(AUDIO_PATH)
        for c in range(len(channels)):
            channel = channels[c]
            t_start = time()
            str = "now fingerprinting channel %d of song %s " % (c + 1, os.path.basename(AUDIO_PATH))
            self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=str)
            print str
            self.fingerprinter.fingerprint(channel, song_id)
            t_end = time()
            str = "time taken: %d seconds" % (t_end - t_start)
            self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=str)
            print str

    def mp3_to_wav(self, src_dir, extension_list=('*.mp4', '*.flv', '*.mp3')):
        os.chdir(src_dir)
        for extension in extension_list:
            for media_file in glob.glob(extension):
                wav_file = os.path.splitext(os.path.basename(media_file))[0] + '.wav'
                str = "converting %s to %s" % (os.path.basename(media_file), wav_file)
                self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=str)
                print str
                AudioSegment.from_file(media_file).export(wav_file, format='wav')


    def fetch_stuff(self):
        self.parser.parse()


if __name__ == '__main__':
    nest = Nest()
    nest.control_center()
    #nest.fetch_stuff()
    #nest.mp3_to_wav('/home/james/python-include/BSRS/tmp/')