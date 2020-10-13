from confluent_kafka import Producer

import argparse
import glob
import time
import sys
import csv
import json
import re
import datetime
import random

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('username', type=str, help='Cloudkafka API username')
    parser.add_argument('password', type=str, help='Cloudkafka API password')
    parser.add_argument('topic', type=str, help='Cloudkafka API password')

    args = parser.parse_args()

    topic = args.topic
    username = args.username
    password = args.password

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': "rocket-01.srvs.cloudkafka.com:9094,"
                             "rocket-02.srvs.cloudkafka.com:9094,"
                             "rocket-03.srvs.cloudkafka.com:9094",
        'session.timeout.ms': 1000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': username,
        'sasl.password': password
    }
    p = Producer(**conf)


    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' % (msg.topic(), msg.partition()))


    articleFiles = list(glob.glob('./all_the_news/articles*.csv'))
    articleFiles.sort()

    for file in articleFiles:
        print(file)
        with open(file) as fp:
            next(fp)
            for line in fp:
                regex = r"^\d+,(?P<id>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<title>(\"(?:[^\"\\]|\\.)*\")|([^,]*))," \
                        r"(?P<publication>(\"(?:[^\"\\]|\\.)*\")|([^,]+)),(?P<author>(\"(?:[^\"\\]|\\.)*\")|([^,]*))," \
                        r"(?P<date>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<year>(\"(?:[^\"\\]|\\.)*\")|([^,]*))," \
                        r"(?P<month>(\"(?:[^\"\\]|\\.)*\")|([^,]*)),(?P<url>(\"(?:[^\"\\]|\\.)*\")|([^,]*))," \
                        r"(?P<content>(\"(?:[^\"\\]|\\.)*\")|([^,]*))$ "

                match = re.match(regex, line)
                if match is None:
                    print("Unparsable: ", line)
                    continue

                matchGroups = match.groupdict()
                timeRetrieved = datetime.datetime.now() - datetime.timedelta(seconds=random.randint(0, 3600))

                try:
                    data = {
                        "id": matchGroups["id"],
                        "title": matchGroups["title"],
                        "publication": matchGroups["publication"],
                        "author": matchGroups["author"],
                        "date": matchGroups["date"],
                        "year": matchGroups["year"],
                        "month": matchGroups["month"],
                        "url": matchGroups["url"],
                        "content": (matchGroups["content"][:500] + '..') if len(matchGroups["content"]) > 500 else
                        matchGroups["content"],
                        "retrieved": str(timeRetrieved)
                    }

                    line = json.dumps(data).encode("utf8")
                    p.produce(topic, line, callback=delivery_callback)
                    p.flush()
                except BufferError as e:
                    sys.stderr.write(
                        '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
                p.poll(0)
                time.sleep(1.0)

    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
