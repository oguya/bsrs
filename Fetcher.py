__author__ = 'james'

from Logging import Logging
from BaseHTTPServer import BaseHTTPRequestHandler
from Queue import Empty
import urllib2 as url2
import os.path
import os
import urllib

import ConfigParser

class Fetcher:
    """
        download json file from xeno-canto.org
        - parse it to get bird info
        - dl bird sound
    """

    #BIRD_SOUNDS_DIR = '/home/james/python-include/BSRS/'
    BIRD_SOUNDS_DIR = 'BirdSounds/'
    CONFIG_FILE = 'bsrs.cfg'
    BASE_API_URL = 'http://www.xeno-canto.org/api/recordings.php'
    BASE_GAPI_URL = 'https://ajax.googleapis.com/ajax/services/search/images'

    Logging = Logging()
    ErrorCodes = BaseHTTPRequestHandler.responses

    def __init__(self,
                 sounds_dir=BIRD_SOUNDS_DIR,
                 logging=Logging,
                 config_file=CONFIG_FILE):

        self.sounds_dir = sounds_dir
        self.logging = logging
        self.config_file = config_file

        self.check_dir()
        #os.chdir(self.sounds_dir)

        self.config = ConfigParser.ConfigParser()
        self.get_configs()
        self.proxied_nets()

    def get_configs(self):
        try:
            self.config.read(self.config_file)
            info_msg = "Loaded config file %s" % self.config_file
            self.logging.write_log('fetcher', 'i', info_msg)
        except Exception, e:
            info_msg = "config file %s missing" % self.config_file
            self.logging.write_log('fetcher', 'e', info_msg)
            raise Exception(info_msg)

    def check_dir(self):
        if not os.path.exists(self.sounds_dir) and not os.path.isdir(self.sounds_dir):
            error_msg = "sounds dir: %s doesn't exists! Creating one" % self.sounds_dir
            self.logging.write_log('fetcher', 'w', error_msg)
        else:
            error_msg = "sounds dir: %s exists!Proceed..." % self.sounds_dir
            self.logging.write_log('fetcher', 'i', error_msg)

    def parse_json(self):
        url = "http://www.xeno-canto.org/api/recordings.php?query=cnt:kenya"
        response = self.url_ops(url)

    #def dl_sound_file(self, soundURL, wavFile):
    def dl_sound_file(self, soundURL_queue, wavFile_queue, results):
        """
            download bird sound audio for later fingerprinting
        """
        soundURL = wavFile = None

        try:
            #get birdID & soundURL
            soundURL = soundURL_queue.get()
            wavFile = wavFile_queue.get()
        except Empty, e:
            self.logging.write_log('fetcher', 'i', "Queue empty: %s" % e.message)
            return False

        wavFile = self.BIRD_SOUNDS_DIR + wavFile + ".mp3"
        print "URL: %s dest: %s" % (soundURL, wavFile)

        try:
            urllib.urlretrieve(soundURL, filename=wavFile)
            info_msg = "downloaded %s saved as: %s" % (soundURL, wavFile)
            self.logging.write_log('fetcher', 'i', info_msg)

            #put results in a queue => {'res': True}
            results.put(True)
        except Exception, e:
            #results.put(False)
            error_msg = "Unable to download file: %s. Stopping! Execption: %s" % (soundURL, e.args)
            self.logging.write_log('fetcher', 'e', error_msg)
            raise Exception(error_msg)

    def proxied_nets(self):
        use_proxy = self.config.getboolean('Proxies', 'use_http_proxy')
        print 'proxy: ', use_proxy
        if use_proxy:
            proxy = url2.ProxyHandler({'http': self.encode_proxy_creds()})
            auth = url2.HTTPBasicAuthHandler()
            opener = url2.build_opener(proxy, auth, url2.HTTPHandler)
            url2.install_opener(opener=opener)
        else:
            os.environ['http_proxy'] = os.environ['HTTP_PROXY'] = ''

    def encode_proxy_creds(self):
        """
            get proxy creds from config file
            http://username:password@proxy_host:proxy_port/
            ret -> http://P15%2F35231%2F2010%40students:rzd%40uon@proxy.uonbi.ac.ke:80/
        """
        try:
            username = urllib.quote(self.config.get('Proxies', 'http_proxy_username'), '')
            password = urllib.quote(self.config.get('Proxies', 'http_proxy_password'), '')
            host = urllib.quote(self.config.get('Proxies', 'http_proxy_host'), '')
            port = urllib.quote(self.config.get('Proxies', 'http_proxy_port'), '')

            proxy_auth_str = "http://%s:%s@%s:%s" % (username, password, host, port)

            info_msg = "using proxy auth: %s" % proxy_auth_str
            self.logging.write_log(log_file='fetcher', log_tag='i', log_msg=info_msg)
            return proxy_auth_str
        except BaseException, e:
            info_msg = "Error in config file: %s" % e.message
            self.logging.write_log(log_file='fetcher', log_tag='e', log_msg=info_msg)
            raise Exception(info_msg)

    def url_ops(self, url):
        """
            open urls -> ret url2.urlopen obj
        """
        headers = {'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36\
                                 (KHTML, like Gecko) Chrome/29.0.1547.65 Safari/537.36"}
        req = url2.Request(url, data=None, headers=headers)

        try:
            response = url2.urlopen(req)
            #assert isinstance(response, object)
            if response.code is not 200:
                error_log = "Didn't expect code: %d. Quitting" % response.code
                self.logging.write_log(log_file='fetcher', log_tag='e', log_msg=error_log)
                raise Exception(error_log)
            info_log = "code: %d. URL: %s" % (response.code, response.url)
            self.logging.write_log(log_file='fetcher', log_tag='i', log_msg=info_log)
            return response
        except url2.HTTPError, e:
            error_log = "HTTP Error: %s " % e.code
            error_log += "Error Details: ", self.ErrorCodes[e.code][0], ":", self.ErrorCodes[e.code][1]
            print error_log
            self.logging.write_log(log_file='fetcher', log_tag='e', log_msg=error_log)
        except url2.URLError, e:
            if hasattr(e, 'reason') and hasattr(e, 'code'):
                error_log = "URL Error. Reason: %s. Code: %s" % (e.reason, e.code)
                print error_log
                self.logging.write_log(log_file='fetcher', log_tag='e', log_msg=error_log)
            else:
                error_log = "URL Error: %s" % e.reason
                print error_log
                self.logging.write_log(log_file='fetcher', log_tag='e', log_msg=error_log)
        return False