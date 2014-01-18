__author__ = 'james'

from Fetcher import Fetcher
from Logging import Logging
from databases import MySQLDatabases
from ConfigParser import ConfigParser

from time import time
import json
import urllib
from Queue import *
from threading import Thread, Lock, current_thread

class Parsers:
    """
        parse json file, store important stuff

        get no of pages-> pages
        page_counter <- 1
        while(page_counter < pages):
            get_page(page_counter)
            for recording in recordings:
                get_recording_details()
                get_audio_file()
            page_counter++
    """

    MAX_IMAGES_URL = 4
    MAX_NO_THREADS = 100
    CONFIG_FILE = 'bsrs.cfg'
    BIRD_SOUNDS_DIR = 'BirdSounds/'

    def __init__(self):
        self.fetcher = Fetcher()
        self.logger = Logging()
        self.config_file = Parsers.CONFIG_FILE
        self.config = ConfigParser()

        self.load_config_file()
        creds = self.get_db_creds()
        self.database = MySQLDatabases(hostname=creds['hostname'], username=creds['username'],
                                       password=creds['passwd'], database=creds['db_name'])

        #queues
        self.birdID_queue = self.wavFile_queue = Queue()
        self.soundtype_queue = self.soundURL_queue = Queue()

    def get_db_creds(self):
        """
            load db creds from config file
        """
        hostname = self.config.get('database', 'db_hostname')
        username = self.config.get('database', 'db_username')
        passwd = self.config.get('database', 'db_password')
        db_name = self.config.get('database', 'db_dbname')
        return {'hostname': hostname, 'username': username,
                'passwd': passwd, 'db_name': db_name}

    def load_config_file(self):
        """
            Load config file
        """
        try:
            self.config.read(self.config_file)
            info_msg = "Loaded config file %s" % self.config_file
            self.logger.write_log('fetcher', 'i', info_msg)
        except Exception, e:
            info_msg = "config file %s missing" % self.config_file
            self.logger.write_log('fetcher', 'e', info_msg)
            raise Exception(info_msg)

    def get_json_data(self, url):
        """
            get url handle from fetcher
        """
        return self.fetcher.url_ops(url)

    def error_handling(self):
        pass

    def parse(self):
        url = self.fetcher.BASE_API_URL+'?query=cnt:kenya'
        API_URL = url
        json_data = json.load(self.get_json_data(url=url))

        page_counter = json_data['page']
        max_pages = json_data['numPages']

        info_log = "About to start parsing URL: %s " % url
        info_log += "no of species: %s " % json_data['numSpecies']
        info_log += "no of recordings: %s " % json_data['numRecordings']
        self.logger.write_log(log_file='parsers', log_tag='i', log_msg=info_log)
        t_start = time()

        while page_counter <= max_pages:
            url = "%s&page=%s" % (API_URL, page_counter)
            print "Now parsing Page: %d URL:%s" % (page_counter, url)

            json_data = json.load(self.get_json_data(url=url))

            recordings = json_data['recordings']

            for record in recordings:
                print "xeno-canto_ID: %s" % record['id']
                print "generic name: %s" % record['gen']
                print "specific name: %s" % record['sp']
                print "english name: %s" % record['en']
                print "country: %s" % record['cnt']
                print "location: %s" % record['loc']
                print "lat: %s" % record['lat']
                print "lng: %s" % record['lng']
                print "type: %s" % record['type']
                print "audio file %s" % record['file']
                print "src url %s" % record['url']
                print "\n\n"

                #TODO store records in db
                #store bird info in db
                birdID = self.database.insert_birds(english_name=record['en'], generic_name=record['gen'],
                                           specific_name=record['sp'], recorder=record['rec'], location=record['loc'],
                                           country=record['cnt'], lat_lng="%s,%s" % (record['lat'], record['lng']),
                                           xeno_cantoURL=record['url'])

                self.birdID = birdID
                self.birdName = record['en']

                info_log = "xeno-canto_ID:%s " % record['id']
                info_log += "generic name:%s " % record['en']
                info_log += "specific name:%s " % record['sp']
                info_log += "english_name:%s " % record['en']
                info_log += "country:%s " % record['cnt']
                info_log += "location:%s " % record['loc']
                info_log += "lat:%s " % record['lat']
                info_log += "lng:%s " % record['lng']
                info_log += "type:%s " % record['type']
                info_log += "audio_file:%s " % record['file']
                info_log += "src_url: %s " % record['url']
                self.logger.write_log(log_file='parsers', log_tag='i', log_msg=info_log)

                wavFile = "%s_%s" % (str(record['en']).replace(' ', '_'), str(birdID))

                #store sound details temporarily in db
                self.database.insert_tmp_sounds(birdID=birdID, soundType=record['type'], wavFile=wavFile,
                                                soundURL=record['file'])

            page_counter += 1

        t_delta = time() - t_start
        info_log = "Finished parsing URL: %s Time: %d" % (url, t_delta)
        self.logger.write_log(log_file='parsers', log_tag='i', log_msg=info_log)

        #download all sounds
        self.threading_ops()

    def threading_ops(self):
        """
            create n threads & download sound files
        """
        num_rows = self.database.get_no_tmp_sounds()
        wavFile_queue = soundURL_queue = Queue(maxsize=num_rows)
        cursor = self.database.get_tmp_sounds()

        for row in cursor:
            wavFile_queue.put(str.format("%s%s.mp3" % (Parsers.BIRD_SOUNDS_DIR, row['wavFile'])))
            soundURL_queue.put(row['soundURL'])

            self.database.insert_sounds(birdID=row['birdID'], soundType=row['soundType'],
                                        wavFile=row['wavFile'], soundURL=row['soundURL'])

            wav_file = str.format("%s%s.mp3" % (Parsers.BIRD_SOUNDS_DIR, row['wavFile']))
            t = Thread(target=self.fetcher.dl_sound_file_v2, args=(row['soundURL'], wav_file))
            t.setDaemon(True)
            t.start()
            t.join()

    def parse_GAPI(self, birdName, birdID, verbose=True):
        """
            download & parse json file containing bird images from
            google url
        """
        birdName = str(birdName).replace('-', ' ')

        query = urllib.urlencode({'q': birdName})
        url = self.fetcher.BASE_GAPI_URL+'?v=1.0&'+query
        print "URL: %s" % url
        json_data = json.load(self.get_json_data(url=url))

        if verbose:
            print "GAPI URL: %s\n BirdName: %s BirdID: %d " % (url, birdName, birdID)
        response_status = json_data['responseStatus']
        if int(response_status) is not 200:
            raise Exception("response from GAPI: %s" % response_status)
        else:
            print "GAPI: OK %s" % response_status

        results = json_data['responseData']['results']

        for result in results:
            imageURL = result['unescapedUrl']
            siteURL = result['originalContextUrl']

            #TODO store results in db
            self.database.insert_images(birdID=birdID, imageURL=imageURL, siteURL=siteURL)

            if verbose:
                logs = "imageURL: %s\n siteURL: %s" % (imageURL, siteURL)
                self.logger.write_log(log_file='parsers', log_tag='i', log_msg=logs)
                print logs

