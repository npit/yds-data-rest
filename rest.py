from flask import Flask, jsonify
from flask_restful import Resource, Api

import json
from datetime import date
import logging, os, time
import mysql.connector

# logging #############################
logging_level=logging.DEBUG
log_directory = os.getcwd()

def get_datetime_str():
    #return time.strftime("[%d|%m|%y]_[%H:%M:%S]")
    return time.strftime("%d%m%y_%H%M%S")

def configure_logging():
    logfile = log_directory + os.path.sep + get_datetime_str() + ".log"
    print("Using logfile: %s" % logfile)
    if not os.path.exists(log_directory):
        try:
            os.makedirs(log_directory)
        except Exception as ex:
            print(ex)
            exit(1)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging_level)

    formatter = logging.Formatter('%(asctime)s| %(levelname)7s - %(filename)15s - line %(lineno)4d - %(message)s')

    # file handler
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging_level)
    handler.setFormatter(formatter)
    # console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    logger.addHandler(consoleHandler)
    return logger

def read_creds_file(credsfile):
    if not os.path.exists(credsfile):
        logger.error("Credentials file %s does not exist!" % credsfile)
    lines = []
    with open(credsfile,'r') as f:
        for line in f:
            line = line.strip()
            lines.append(line)
    return tuple(lines)

logger = configure_logging()
########################################

credsfile="creds.txt"
user,passw, host, db = read_creds_file(credsfile)

# start the connection #############################
try:
    cnx = mysql.connector.connect(user=user, password=passw, host=host,  database=db, charset="utf8")
    cursor = cnx.cursor()
except Exception as ex:
    logger.error("Unable to connect to the database.")
    logger.error(ex)
    exit(1)

####################################################

class tweet:
    post_id = "post_id"
    created_at = "created_at"
    coordinates = "coordinates"
    place = "place"
    retweet_count = "retweet_count"
    followers_when_published = "followers_when_published"
    text = "text"
    language = "language"
    url = "url"
    twitter_user_id = "twitter_user_id"
    engine_type = "engine_type"
    engine_id = "engine_id"
    def toJSON(qres, fields):

        jsonarr = [

                {fields[i] : qres[j][i] for i in range(len(fields)) }

            for j in range(len(qres))
        ]
        return jsonarr


app = Flask(__name__)
api = Api(app)
app.config['JSON_AS_ASCII'] = False
@app.route("/tweets/<timestamp>")
def get_tweets(timestamp):
    logger.info("Fetching tweets with timestamp argument %s " % str(timestamp))
    try:
        datestr = str(date.fromtimestamp(int(timestamp)))
    except Exception as ex:
        return [{"status" : "400", "message" : str(ex)}]

    fields = [ tweet.post_id, tweet.url, tweet.text , tweet.created_at]
    query = ("SELECT " + ",".join(fields) + " from twitter_post where created_at > " + datestr + " limit 5")

    cursor.execute(query)
    res = cursor.fetchall()
    logger.info("Fetched %d tweets with timestamp argument %s " % (len(res), str(timestamp)))

    return json.dumps(tweet.toJSON(res, fields),ensure_ascii=False)


if __name__ == '__main__':
    app.run(debug=True)
    # h = HelloWorld()
    # h.get()