__author__ = 'james'

from pydub import AudioSegment
from time import time
from threading import Thread
from multiprocessing import Process, current_process
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
import optparse


class Nest:
    """
        - convert all mp3 sounds to wav sounds -> store in wavs folder
        - go to db get birdID & wavFile & fingerprint the wavfile
        - store birdID & hash in db
    """

    SOUNDS_DIR = 'BirdSounds/'
    WAV_SOUNDS_DIR = 'BirdSounds/wavSounds/'
    MAX_PROCS = 10

    def __init__(self, **kwargs):
        if kwargs.get('cd'):
            print os.getcwd()
            os.chdir('../')

        self.fingerprinter = Fingerprinter()
        self.logger = Logging()
        self.fetcher = Fetcher()
        self.parser = Parsers()
        self.config = Configs()
        creds = self.config.get_db_creds()
        self.recognizer = Recognizer()
        self.database = self.database = MySQLDatabases(hostname=creds['hostname'], username=creds['username'],
                                                       password=creds['passwd'], database=creds['db_name'])

    def mp3_to_wav(self, src_dir, extension_list=('*.mp4', '*.flv', '*.mp3')):
        os.chdir(src_dir)
        logs = ""
        for extension in extension_list:
            for media_file in glob.glob(extension):
                wav_file = "../" + Nest.WAV_SOUNDS_DIR + os.path.splitext(os.path.basename(media_file))[0] + '.wav'
                logs += "converting %s to %s\n" % (os.path.basename(media_file), wav_file)
                AudioSegment.from_file(media_file).export(wav_file, format='wav')
        os.chdir('../')
        print logs
        self.logger.write_log(log_file='fingerprint', log_tag='i', log_msg=logs)

    def reload_creds(self):
        self.database = None

        creds = self.config.get_db_creds()
        self.database = self.database = MySQLDatabases(hostname=creds['hostname'], username=creds['username'],
                                                       password=creds['passwd'], database=creds['db_name'])

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

    def process_requests(self, request_id):
        """
            get wavfile from inbound request, match &
        """
        cursor = self.database.get_inbound_request(request_id)
        if cursor is None:
            print "cursor is None!"
            self.reload_creds()
            cursor = self.database.get_inbound_request(request_id)
        else:
            print "cursor is not None!"

        wavfile = cursor['wavFile']

        bird_details = self.recognizer.recognize_file(filename=wavfile, verbose=False)
        self.database.update_processed_requests(request_id)

        match_result = 0 if bird_details['bird_id'] == 0 else 1
        outbound_id = self.database.insert_outbound_match(request_id=request_id, birdID=bird_details['bird_id'],
                                                          matchResults=match_result)
        # print "outboundID: %s" % outbound_id
        return outbound_id

    def get_outbound_birdID(self, outboundID):
        """
            return outboundId from outbound_matches tbl
        """
        cursor = self.database.get_outbound_bird_id(outboundID)
        return cursor['birdID']

    def get_match_results(self, outboundID):
        """
            return matchResults from outbound_matches tbl
        """
        cursor = self.database.get_match_results(outboundID)
        return cursor['matchResults']

    def add_request(self, wavfile, deviceID):
        """
            add new unmatched request in db
        """
        request_id = self.database.insert_inbound_request(wavfile, deviceID)
        return request_id

    def get_bird_details(self, birdID):
        """
            get bird details from db
        """
        cursor = self.database.get_bird_by_id(birdID)
        return cursor

    def get_sound_details(self, birdID):
        """
            get sounds from db for a given birdID
            birdID, soundType, wavFile, soundURL
        """
        cursor = self.database.get_sound_by_id(birdID)
        return {"soundType": cursor['soundType'], "soundURL": cursor['soundURL']}

    def get_thumbnail_pic(self, birdID):
        """
            get thumbnail img from db for a given birdID
        """
        cursor = self.database.get_thumbnail_pic(birdID)
        return cursor['imageURL']

    def get_images(self, birdID):
        """
            return a list of images from db for a given birdID
        """
        cursors = self.database.get_images(birdID)
        pics = []
        for cursor in cursors:
            img = {"imageURL": cursor['imageURL'], "siteURL": cursor['siteURL']}
            pics.append(img)
        return pics


def main():
    """
        check for args, run fingerprinter
    """
    p = optparse.OptionParser()
    p.add_option('--requestid', '-r', dest='requestid')

    options, args = p.parse_args()

    if options.requestid:
        Nest().process_requests(request_id=options.requestid)
    else:
        print "No args. Exiting..."
        show_help()


def show_help():
    """
        cmd usage
    """
    help_msg = "\nusage: python nest.py -r requestid\n" \
               "example: python nest.py -r 5\n" \
               "python nest.py -r 5\n"
    print help_msg


if __name__ == '__main__':
    main()
    # nest = Nest()
    #nest.control_center()
    #nest.mp3_to_wav(Nest.SOUNDS_DIR)

    # try:
    #     nest.fingerprint_sounds()
    # except Exception, ex:
    #     logs = "error in fingerprint_sounds() ", ex.message
    #     nest.logger.write_log(log_file='fingerprint', log_tag='e', log_msg=logs)

    #recognizer = Recognizer()
    #bird = recognizer.recognize_file(filename="BirdSounds/testing/white_noise_stereo.wav")
    #bird = recognizer.recognize_file(filename="BirdSounds/testing/Grey_olive_noise.wav")
    #bird = recognizer.recognize_file(filename="BirdSounds/testing/Olive_Sunbird_546.wav")
    #bird = recognizer.recognize_file(filename="BirdSounds/testing/African_Barred_Owlet_83.wav")
    #bird = recognizer.recognize_file(filename="BirdSounds/wavSounds/Clarke's_Weaver_602.wav")
    #bird = recognizer.listen(seconds=10, verbose=True)
    #print "bird details: ", bird

    #stats = stats()
    #stats.stereo_noise()

