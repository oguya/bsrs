__author__ = 'james'

from pydub import AudioSegment
from time import time
from threading import Thread
from multiprocessing import  Process, current_process
from random import shuffle
import os
import glob
import numpy as np

from fingerprinter import Fingerprinter
from Logging import Logging
from Fetcher import Fetcher
from Parsers import Parsers
from Config import Configs
from databases import MySQLDatabases
from recognizer import Recognizer
from stats import stats


class Nest:

    """
        - convert all mp3 sounds to wav sounds -> store in wavs folder
        - go to db get birdID & wavFile & fingerprint the wavfile
        - store birdID & hash in db
    """

    SOUNDS_DIR = 'BirdSounds/'
    WAV_SOUNDS_DIR = 'BirdSounds/wavSounds/'
    MAX_PROCS = 10

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

    def chunkify(self, lst, n):
        """
        split a list into n no of parts
        """
        return [lst[i::n] for i in xrange(n)]

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
        sound_details = []

        count = 0
        print len(cursor)
        for row in cursor:
            birdID = row['birdID']
            wavFile = "%s%s.wav" % (Nest.WAV_SOUNDS_DIR, row['wavFile'])
            sound_details.append((birdID, wavFile))

        shuffle(sound_details)
        split_details = self.chunkify(sound_details, Nest.MAX_PROCS)

        #split procs
        procs = []
        #for i in range(Nest.MAX_PROCS):
        #    #create separate/non-shared connections to db
        #    creds = Configs().get_db_creds()
        #    self.database = self.database = MySQLDatabases(hostname=creds['hostname'], username=creds['username'],
        #                                                   password=creds['passwd'], database=creds['db_name'])
        #
        #    #create procs & start
        #    proc = Process(target=self.fingerprint_worker, args=([split_details[i]]))
        #    proc.start()
        #    procs.append(proc)
        #
        ##wait for all procs to finish
        #for proc in procs:
        #    proc.join()

        self.fingerprint_worker(sound_details)

    def fingerprint_worker(self, sound_details):
        """
            fingerprint each song & store hash in db
        """

        for birdID, wavFile in sound_details:
            print "birdID: ", birdID, "wavFile: ", wavFile

            channels = self.fingerprinter.extract_channels(wavFile)
            for c in range(len(channels)):
                channel = channels[c]
                t_start = time()
                logs = "now fingerprinting channel %d of song %s. BirdID: %s" % (c + 1, wavFile, birdID)
                self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=logs)
                print logs
                self.fingerprinter.fingerprint(channel, birdID)
                logs = "time taken: %d seconds" % (time() - t_start)
                self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=logs)
                print logs

            #update song as fingerprinted
            self.database.update_fingerprinted_songs(birdID=birdID)


if __name__ == '__main__':
    nest = Nest()
    #nest.control_center()
    #nest.mp3_to_wav(Nest.SOUNDS_DIR)
    #nest.fingerprint_sounds()
    recognizer = Recognizer()
    #bird = recognizer.recognize_file(filename="BirdSounds/wavSounds/Clarke's_Weaver_602.wav")
    bird = recognizer.recognize_file(filename="BirdSounds/testing/Blue-spotted_noise.wav")
    #bird = recognizer.recognize_file(filename="341.wav")
    #bird = recognizer.listen(seconds=10, verbose=True)
    print "bird details: ", bird

    stats = stats()
    stats.stereo_noise()

