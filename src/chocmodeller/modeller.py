# This file is part of chocolatine
# 
# Copyright (C) 2021 The Regents of the University of California
# All Rights Reserved
#
# Permission to copy, modify, and distribute this software and its
# documentation for academic research and education purposes, without fee, and
# without a written agreement is hereby granted, provided that
# the above copyright notice, this paragraph and the following paragraphs
# appear in all copies.
#
# Permission to make use of this software for other than academic research and
# education purposes may be obtained by contacting:
#
# Office of Innovation and Commercialization
# 9500 Gilman Drive, Mail Code 0910
# University of California
# La Jolla, CA 92093-0910
# (858) 534-5815
# invent@ucsd.edu
#
# This software program and documentation are copyrighted by The Regents of the
# University of California. The software program and documentation are supplied
# "as is", without any accompanying services from The Regents. The Regents does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO
# OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.
#
####
#
# Portions of this source code are Copyright (c) 2023 Georgia Tech Research
# Corporation. All Rights Reserved. Permission to copy, modify, and distribute
# this software and its documentation for academic research and education
# purposes, without fee, and without a written agreement is hereby granted,
# provided that the above copyright notice, this paragraph and the following
# three paragraphs appear in all copies. Permission to make use of this
# software for other than academic research and education purposes may be
# obtained by contacting:
#
#  Office of Technology Licensing
#  Georgia Institute of Technology
#  926 Dalney Street, NW
#  Atlanta, GA 30318
#  404.385.8066
#  techlicensing@gtrc.gatech.edu
#
# This software program and documentation are copyrighted by Georgia Tech
# Research Corporation (GTRC). The software program and documentation are
# supplied "as is", without any accompanying services from GTRC. GTRC does
# not warrant that the operation of the program will be uninterrupted or
# error-free. The end-user understands that the program was developed for
# research purposes and is advised not to rely exclusively on the program for
# any reason.
#
# IN NO EVENT SHALL GEORGIA TECH RESEARCH CORPORATION BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
# EVEN IF GEORGIA TECH RESEARCH CORPORATION HAS BEEN ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE. GEORGIA TECH RESEARCH CORPORATION SPECIFICALLY DISCLAIMS ANY
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED
# HEREUNDER IS ON AN "AS IS" BASIS, AND  GEORGIA TECH RESEARCH CORPORATION HAS
# NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
# MODIFICATIONS.
#
#
# =================================================

import time, sys, json, os
import pandas as pd
import logging
import argparse
import psycopg2

from confluent_kafka import Consumer, KafkaException, Producer
from libchocolatine.asyncfetcher import fetchIodaMeta, fetchIodaHistoricBlocking
from libchocolatine.arimabuilder import ChocArimaPool, ChocArimaJob

class ChocModeller(object):

    def __init__(self, iodaapiurl, arimaworkers, arimahistory):

        self.iodaapiurl = iodaapiurl
        self.outstanding_arima = set()
        self.arimapool = ChocArimaPool(arimaworkers)
        self.arimahistory = arimahistory
        self.consumer = None
        self.producer = None
        self.producer_topic = "chocolatine.model.generated"
        self.dbsession = None
        self.dbcursor = None

    def connectDatabase(self):

        dbname = "models"
        dbpword = os.getenv("PSQL_PASSWORD")

        if dbpword is None:
            print("Unable to determine PSQL password")
            return None

        self.dbsession = psycopg2.connect(database=dbname, user='postgres', password=dbpword, host="martignano.cc.gatech.edu", port=5432)
        self.dbcursor = self.dbsession.cursor()
        return self.dbcursor

    def insertDatabaseRow(self, reply):

        if self.dbsession is None:
            if self.connectDatabase() is None:
                print("Unable to connect to database to write model estimate")
                return

        if reply["ar_param"] is None:
            reply["ar_param"] = 0
        if reply["ma_param"] is None:
            reply["ma_param"] = 0

        insert = """INSERT INTO public.arma_models values (
                      %u, %u, %u, '%s', '%s', '%s', '%s', %u, %u, %.2f,""" % \
                      (reply['ar_param'], reply['ma_param'],
                       reply['param_limit'], reply['datasource'],
                       reply['entitytype'], reply['entitycode'],
                       reply['fqid'], reply['generated_at'],
                       reply['training_start'], reply['time_to_generate'])

        if len(reply['mads']) > 0:
            insert += " ARRAY ["""

            for i in range(0, len(reply['mads'])):
                if i < len(reply['mads']) - 1:
                   insert += "%.6f, " % (reply['mads'][i])
                else:
                   insert += "%.6f], " % (reply['mads'][i])
        else:
            insert += " ARRAY[]::real[], "

        insert += "'%s');" % (reply["model_type"])


        self.dbcursor.execute(insert)
        self.dbsession.commit()

    def setupKafkaConsumer(self, broker, group, topics):
        conf = {'bootstrap.servers': broker, 'group.id': group,
                'session.timeout.ms': 6000,
                'auto.offset.reset': "earliest"
        }
        logger = logging.getLogger("kafka-consumer")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
        logger.addHandler(handler)


        self.consumer = Consumer(conf, logger=logger)
        self.consumer.subscribe(topics)

    def setupKafkaProducer(self, broker, topic=None):
        conf = {'bootstrap.servers': broker}

        self.producer = Producer(**conf)
        if topic is not None:
            self.producer_topic = topic

    def deriveBestModel(self, fetched, received, stepsize):
        expectedpoints = int(self.arimahistory / stepsize)
        if fetched.empty:
            return None

        zeroes = (fetched["signalValue"] == 0).sum()

        if fetched.shape[0] < 0.75 * expectedpoints:
            print("Series '%s' has too many missing values to derive a model" % (received['fqid']))
            return None

        lastts = fetched.last_valid_index()
        firstts = fetched.first_valid_index()

        if lastts - firstts < pd.Timedelta(self.arimahistory, units="seconds") * 0.95:
            print("Fetched history for series '%s' does not cover full time period" % (received['fqid']))
            return None

        if zeroes / fetched.shape[0] >= 0.5:
            print("Series '%s' is mostly zeroes -- suggesting zero-based model" % (received["fqid"]))

            # insert zero model into db
            return 0

        # Try derive a good ARMA model for this
        job = ChocArimaJob(received['fqid'], received['requestedby'],
                received['arma_limit'], fetched['signalValue'], stepsize)
        self.arimapool.addJob(job)
        return 1


    def _fetchData(self, injob):
        tofetch = self.arimahistory
        startts = injob['ts'] - self.arimahistory

        finalmeta = {}

        res = []
        while tofetch > 0:
            amount = min(tofetch, 90 * 24 * 60 * 60)

            fetched, meta = fetchIodaHistoricBlocking(self.iodaapiurl,
                    injob['fqid'], startts + amount, amount, 1800)
            if fetched is not None:

                # Convert the data into a usable pandas dataframe
                t = fetched['from']
                step = fetched['step']
                serieskey = injob['fqid']

                for v in fetched['values']:
                    res.append({"timestamp": pd.Timestamp(t, unit='s'),
                                "signalValue": v})
                    t += step


                if finalmeta == {}:
                    finalmeta = meta
                    finalmeta['step'] = fetched['step']
                else:
                    finalmeta['duration'] += meta['duration'] 
                    if meta['endtime'] > finalmeta['endtime']:
                        finalmeta['endtime'] = meta['endtime']

            tofetch -= amount
            startts += amount

        if len(res) == 0:
            return None, None

        pdseries = pd.DataFrame.from_records(res, index="timestamp")
        pdseries.name = injob['fqid']
        return pdseries, finalmeta

    def _actionJob(self, injobmsg, store_db):
        if injobmsg.error():
            raise KafkaException(injobmsg.error())

        injob = json.loads(injobmsg.value())
        if injob['ts'] == 0:
             injob['ts'] = time.time()
             injob['ts'] -= (injob['ts'] % 300)

        if injob['fqid'].startswith('gtr.'):
            # TEMPORARY - I'm doing some testing with the gtr. series
            if injob['fqid'] in self.waiting:
                del(self.waiting[injob['fqid']])
            return

        if injob['fqid'] in self.waiting:
            meta = fetchIodaMeta(injob['fqid'], injob['ts'], self.arimahistory)

            injob['meta'] = meta
            self.waiting[injob['fqid']][injob['requestedby']] = injob
        else:

            fetched, meta = self._fetchData(injob)
            if fetched is None:
                if injob['fqid'] in self.waiting:
                    del(self.waiting[injob['fqid']])
            else:
                r = self.deriveBestModel(fetched, injob, meta['step'])
                if r < 0:
                    return
                if r == 0:
                    zero_reply = {
                        "fqid": injob["fqid"],
                        "param_limit": injob["arma_limit"],
                        "datasource": meta["datasource"],
                        "entitycode" : meta["entitycode"],
                        "entitytype": meta["entitytype"],
                        "generated_at": int(time.time()),
                        "training_start": meta["fetchstart"],
                        "time_to_generate": 0,
                        "requestedby": injob["requestedby"],
                        "model_type": "ZERO",
                        "ar_param": 0,
                        "ma_param": 0,
                        "mads": []
                    }

                    self.producer.produce(self.producer_topic,
                                json.dumps(zero_reply))

                    if store_db:
                        self.insertDatabaseRow(zero_reply)
                    print("Generated ZERO model for series '%s'" % (zero_reply['fqid']))
                else:
                    if injob['fqid'] not in self.waiting:
                        self.waiting[injob['fqid']] = {}

                    injob['meta'] = meta
                    self.waiting[injob['fqid']][injob['requestedby']] = injob

    def run(self, store_db):
        if self.consumer is None:
            print("Error: need to call setupKafkaConsumer() before calling run()")
            return

        self.arimapool.startWorkers()

#        injob = {"serieskey": "bgp.prefix-visibility.geo.netacuity.OC.AU.v4.visibility_threshold.min_50%_ff_peer_asns.visible_slash24_cnt", "timestamp": 1657237628}

        self.waiting = {}
        try:
            while True:
                injobmsg = self.consumer.poll(timeout=1.0)
                if injobmsg is not None:
                    self._actionJob(injobmsg, store_db)

                completedArima = self.arimapool.getCompleted()
                for c in completedArima:
                    if c['fqid'] not in self.waiting:
                        continue

                    reply = {}
                    for reqfrom, reqjob in self.waiting[c['fqid']].items():
                        reply = {
                            "param_limit": reqjob["arma_limit"],
                            "datasource": reqjob["meta"]["datasource"],
                            "entitycode" : reqjob["meta"]["entitycode"],
                            "entitytype": reqjob["meta"]["entitytype"],
                            "fqid": c["fqid"],
                            "generated_at": c["eststart"],
                            "training_start": reqjob["meta"]["fetchstart"],
                            "time_to_generate": c["modeltime"],
                            "mads": c["mads"],
                            "requestedby": reqfrom,
                            "model_type": "ARMA"
                        }
                        if c['arma'] is None:
                            reply['ar_param'] = None
                            reply['ma_param'] = None
                        else:
                            reply['ar_param'] = c['arma'][0]
                            reply['ma_param'] = c["arma"][2]

                        self.producer.produce(self.producer_topic,
                                json.dumps(reply))

                    if reply != {}:
                        if store_db:
                            self.insertDatabaseRow(reply)
                        print("Generated model for series '%s'" % (reply['fqid']))

                    self.producer.flush()
                    del(self.waiting[c['fqid']])

                    print(len(self.waiting), "model estimation jobs remaining")


        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

def startModeller():

    parser = argparse.ArgumentParser()
    parser.add_argument('-b' ,'--kafka-broker', required=True, help='Location of kafka broker')
    parser.add_argument('-t', '--topic-prefix', default="chocolatine.model", help="prefix to add to kafka topics")
    parser.add_argument('-g', '--kafka-group', default="choc-testing", help="Kafka consumer group to use")
    parser.add_argument('-p', '--pool-size', default=4, type=int, help="Number of concurrent model estimation threads to use")
    parser.add_argument('-D', '--store-db', action="store_true", help="Write generated models to a database")

    opts = vars(parser.parse_args())

    model = ChocModeller(
        "http://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw",
        opts['pool_size'],
        23 * 7 * 24 * 60 * 60)

    model.setupKafkaConsumer(opts['kafka_broker'], opts['kafka_group'],
        [opts['topic_prefix'] + ".requests"])

    model.setupKafkaProducer(opts['kafka_broker'],
        opts['topic_prefix'] + ".generated")

    model.run(opts['store_db'])
    print("Exiting chocolatine modeller")

    model.arimapool.haltWorkers()
