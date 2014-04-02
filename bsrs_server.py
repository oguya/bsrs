__author__ = 'james'

from flask import Flask, jsonify, request
from nest import Nest
import time
import os
import pprint

app = Flask(__name__)
#flask svr run configs
# HOST = '127.0.0.1'
#TODO get current ip
HOST = '192.168.43.100'
# HOST = '0.0.0.0'
PORT = 5000
DEBUG = True
WAVFILEUPLOAD_DIR = 'wavFileUploads/'
NEST = Nest()


@app.route('/')
def hello_world():
    return 'Hello world'


@app.route('/recognizer/<int:request_id>')
def recognizer(request_id):
    NEST.process_requests(request_id=request_id)
    return "request_id: %d" % request_id


@app.route('/match', methods=['POST', 'GET'])
def match():
    """
        handle wavfile uploads
    """
    pprint.pprint(request.form)
    if request.method == 'POST':
        wavfile = request.files['wavFile']
        deviceID = request.form['deviceID']
        str = "deviceID: %s file: %s " % (deviceID, wavfile.filename)
        new_name = "%s%s_%s" % (WAVFILEUPLOAD_DIR, wavfile.filename, timestamp())
        print "New Request. wavfile: %s deviceID: %s" % (new_name, deviceID)
        wavfile.save(new_name)
        request_id = NEST.add_request(new_name, deviceID)
        result = start_recognizer(request_id)
        return result
    else:
        wavfile = request.files['wavFile']
        deviceID = request.form['deviceID']
        str = "deviceID: %s file: %s " % (deviceID, wavfile.filename)
        new_name = "%s%s_%s" % (WAVFILEUPLOAD_DIR, wavfile.filename, timestamp())
        print "New Request. wavfile: %s deviceID: %s" % (new_name, deviceID)
        error = {"tagResults": 0, "birdID": 0, "errorDetails": "Use HTTP POST"}

        return jsonify(error)


@app.route('/match2/<int:request_id>')
def match2(request_id):
    pprint.pprint(request)
    result = start_recognizer(request_id)
    return result

def start_recognizer(request_id):
    """
        start recognizing sounds
    """
    #TODO get birdID..get birdDetails, soundDetails, pics, thumbnailPic
    outbound_id = NEST.process_requests(request_id=request_id)
    result = process_results(outbound_id)
    return jsonify(result)


def process_results(outbound_id):
    """
        process results return json
        TODO get birdID..get birdDetails, soundDetails, pics, thumbnailPic
    """
    birdID = NEST.get_outbound_birdID(outbound_id)
    match_results = NEST.get_match_results(outbound_id)

    if birdID == 0 or match_results == 0:
        #TODO add error handling
        return error_result()

    cursor = NEST.get_bird_details(birdID)
    sound_details = NEST.get_sound_details(birdID)
    pics = NEST.get_images(birdID)
    thumbnail_pic = NEST.get_thumbnail_pic(birdID)
    results = {
        "tagResults": match_results,
        "birdID": cursor['birdID'],
        "englishName": cursor['englishName'],
        "genericName": cursor['genericName'],
        "specificName": cursor['specificName'],
        "Recorder": cursor['Recorder'],
        "Location": cursor['Location'],
        "Country": cursor['Country'],
        "lat": str(cursor['lat_lng']).split(',')[0],
        "lng": str(cursor['lat_lng']).split(',')[1],
        "xenoCantoURL": cursor['xenoCantoURL'],
        "soundType": sound_details['soundType'],
        "soundURL": sound_details['soundURL'],
        "thumbnailURL": thumbnail_pic,
        "images": pics
    }
    return results


def error_result():
    result = {
        "tagResults": 0,
        "birdID": 0,
    }
    return result

@app.route('/test')
def test_json():
    images = []
    for i in "hello world":
        x = {"imageURL": i, "siteURL":  "site url hapa"}
        images.append(x)

    json = {
        "tagResults": "1",
        "birdID": "33",
        "englishName": "Crested Guineafowl",
        "genericName": "Guttera",
        "specificName": "pucherani",
        "Recorder": "Rory Nefdt",
        "Location": "Kakamega Forest Reserve",
        "Country": "Kenya",
        "lat": "0.352",
        "lng": "34.861",
        "xenoCantoURL": "http://xeno-canto.org/98676",
        "soundType": "Call",
        "soundURL": "http://www.xeno-canto.org/download.php?XC=98676",
        "thumbnailURL": "http://ibc.lynxeds.com/files/pictures/Crested_Guineafowl_LHarding_med.JPG",
        "images": images
    }
    return jsonify(json)


def timestamp():
    """
        return current_timestamp -> YYYY-mm-dd HH:MM:SS
    """
    c = time.localtime()
    str = "%s-%s-%s_%s:%s:%s" % (c.tm_year, c.tm_mon, c.tm_mday, c.tm_hour, c.tm_min, c.tm_sec)
    return str


def checkUploadDir():
    """
    create wavfile upload dir if not existent
    """
    if not os.path.exists(WAVFILEUPLOAD_DIR) and not os.path.isdir(WAVFILEUPLOAD_DIR):
        print "wavfile uploads dir %s doesn't exist. Creating it" % WAVFILEUPLOAD_DIR
        os.mkdir(WAVFILEUPLOAD_DIR, 0755)
    else:
        print "uploads dir %s exists. proceed..." % WAVFILEUPLOAD_DIR
        pass


#run server
if __name__ == '__main__':
    checkUploadDir()
    app.run(HOST, PORT, debug=DEBUG)