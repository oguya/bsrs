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
        self.results_queue = self.soundtype_queue = self.soundURL_queue = Queue()

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
        json_data = json.load(self.get_json_data(url=url))

        page_counter = json_data['page']
        max_pages = json_data['numPages']

        info_log = "About to start parsing URL: %s " % url
        info_log += "no of species: %s " % json_data['numSpecies']
        info_log += "no of recordings: %s " % json_data['numRecordings']
        self.logger.write_log(log_file='parsers', log_tag='i', log_msg=info_log)
        t_start = time()

        while page_counter <= max_pages:
            url += '&page=%s' % page_counter
            print "Now parsing Page: %s" % page_counter

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

                #fetch audio file
                #TODO use threads

                #create queues to hold results, birdID & wavFile
                #self.birdID_queue = self.wavFile_queue = self.results_queue = Queue()

                wavFile = "%s_%s" % (str(record['en']).replace(' ', '_'), str(birdID))

                #store values in queues
                self.birdID_queue.put(birdID)
                self.wavFile_queue.put(wavFile)
                self.soundtype_queue.put(record['type'])
                self.soundURL_queue.put(record['file'])

                #status = self.fetcher.dl_sound_file(soundURL=record['file'], wavFile=wavFile)

                #if status:
                #    #store sound details in db
                #    self.database.insert_sounds(birdID=birdID, soundType=record['type'],
                #                                wavFile=wavFile, soundURL=record['file'])

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
        #TODO not mem. efficient => soln??

        self.tmp_sndURL_queue = self.soundURL_queue
        self.tmp_wavfile_queue = self.wavFile_queue

        #no of threads = qsize
        Parsers.MAX_NO_THREADS = self.birdID_queue.qsize() if self.birdID_queue.qsize() > Parsers.MAX_NO_THREADS else Parsers.MAX_NO_THREADS

        #create n workers
        workers = [Thread(target=self.fetcher.dl_sound_file,
                          args=(self.soundURL_queue, self.wavFile_queue, self.results_queue))
                   for i in range(Parsers.MAX_NO_THREADS)]

        #start threads
        for worker in workers:
            worker.start()

        #get the results & store in db
        for i in xrange(self.results_queue.qsize()):
            try:
                status = self.results_queue.get()
                birdID = self.birdID_queue.get()
                sound_type = self.soundtype_queue.get()
                wav_file = self.tmp_wavfile_queue.get()
                sound_url = self.tmp_sndURL_queue.get()

                print "status %s" % str(status)

                if status:
                    #store sound details in db
                    self.database.insert_sounds(birdID=birdID, soundType=sound_type,
                                                wavFile=wav_file, soundURL=sound_url)
            except Empty, e:
                self.logger.write_log(log_file='parsers', log_tag='e', log_msg="empty queue: %s" % e.message)

        #wait for all threads to complete
        for worker in workers: worker.join()
        self.logger.write_log(log_file='parsers', log_tag='i', log_msg="all %d threads done!" % Parsers.MAX_NO_THREADS)

    def parse_GAPI(self, birdName, birdID, verbose=False):
        """
            download & parse json file containing bird images from
            google url
        """
        query = urllib.urlencode({'q': birdName})
        url = self.fetcher.BASE_API_URL+'?v=1.0&'+query
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
