__author__ = 'james'

from Fetcher import Fetcher
from Logging import Logging
from databases import MySQLDatabases
from time import time
import json
import thread
import urllib

class Parsers():
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

    def __init__(self):
        self.fetcher = Fetcher()
        self.logger = Logging()

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
                print "generic name: %s" % record['en']
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
                thread.start_new_thread(self.fetcher.dl_sound_file, (record['file'], record['id']+".mp3"))
                #self.fetcher.dl_sound_file(record['file'], record['id']+".mp3")

            break
            #page_counter += 1

        t_delta = time() - t_start
        info_log = "Finished parsing URL: %s Time: %d" % (url, t_delta)
        self.logger.write_log(log_file='parsers', log_tag='i', log_msg=info_log)

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

        results = json_data['responseData']['results']

        for result in results:
            imageURL = result['unescapedUrl']
            siteURL = result['originalContextUrl']

            #TODO store results in db

            if verbose:
                logs = "imageURL: %s\n siteURL: %s" % (imageURL, siteURL)
                self.logger.write_log(log_file='parsers', log_tag='i', log_msg=logs)
                print logs
