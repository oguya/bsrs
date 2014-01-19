__author__ = 'james'

from pydub import AudioSegment
from time import time
from threading import Thread
import os
import glob

from fingerprinter import Fingerprinter
from Logging import Logging
from Fetcher import Fetcher
from Parsers import Parsers
from Config import Configs
from databases import MySQLDatabases


class Nest:

    """
        - convert all mp3 sounds to wav sounds -> store in wavs folder
        - go to db get birdID & wavFile & fingerprint the wavfile
        - store birdID & hash in db
    """

    SOUNDS_DIR = 'BirdSounds/'
    WAV_SOUNDS_DIR = 'BirdSounds/wavSounds/'

    def __init__(self):
        self.fingerprinter = Fingerprinter()
        self.logger = Logging()
        self.fetcher = Fetcher()
        self.parser = Parsers()
        creds = Configs().get_db_creds()
        self.database = self.database = MySQLDatabases(hostname=creds['hostname'], username=creds['username'],
                                                       password=creds['passwd'], database=creds['db_name'])

    def mp3_to_wav(self, src_dir, extension_list=('*.mp4', '*.flv', '*.mp3')):
        os.chdir(src_dir)
        logs = ""
        for extension in extension_list:
            for media_file in glob.glob(extension):
                wav_file = "../"+Nest.WAV_SOUNDS_DIR + os.path.splitext(os.path.basename(media_file))[0] + '.wav'
                logs += "converting %s to %s\n" % (os.path.basename(media_file), wav_file)
                AudioSegment.from_file(media_file).export(wav_file, format='wav')
        os.chdir('../')
        print logs
        self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=logs)

    def fetch_stuff(self):
        pass
        #self.parser.parse()
        #self.parser.threading_ops()

    def fetch_images(self):
        """
        get all birds from db
            - get birdID & birdName
            - get image URLS from GAPI & store in DB
        """
        cursor = self.parser.database.get_all_birds()
        for row in cursor:
            self.parser.parse_GAPI(birdName=row['englishName'], birdID=row['birdID'])

    def fingerprint_sounds(self):
        """
            - go to db get birdID & wavFile & fingerprint the wavfile
            - store birdID & hash in db
        """
        cursor = self.database.get_sounds()
        threads = []

        for row in cursor:
            birdID = row['birdID']
            wavFile = "%s%s.wav" % (Nest.WAV_SOUNDS_DIR, row['wavFile'])

            t = Thread(target=self.fingerprint_worker, args=(wavFile, birdID))
            t.start()
            threads.append(t)

        #wait for all threads to complete
        for thread in threads:
            thread.join()

    def fingerprint_worker(self, wavFile, birdID):
        """
            fingerprint each song & store hash in db
        """
        channels = self.fingerprinter.extract_channels(wavFile)
        for c in range(len(channels)):
            channel = channels[c]
            t_start = time()
            logs = "now fingerprinting channel %d of song %s. BirdID: %s" % (c + 1, wavFile, birdID)
            self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=logs)
            print logs
            self.fingerprinter.fingerprint(channel, birdID)
            t_end = time()
            logs = "time taken: %d seconds" % (t_end - t_start)
            self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=logs)
            print logs



if __name__ == '__main__':
    nest = Nest()
    #nest.control_center()
    #nest.mp3_to_wav(Nest.SOUNDS_DIR)
    nest.fingerprint_sounds()