# This source code is Copyright (c) 2023 Georgia Tech Research Corporation. All
# Rights Reserved. Permission to copy, modify, and distribute this software and
# its documentation for academic research and education purposes, without fee,
# and without a written agreement is hereby granted, provided that the above
# copyright notice, this paragraph and the following three paragraphs appear in
# all copies. Permission to make use of this software for other than academic
# research and education purposes may be obtained by contacting:
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
# This source code is part of the chocolatine software. The original
# chocolatine software is Copyright (c) 2021 The Regents of the University of
# California. All rights reserved. Permission to copy, modify, and distribute
# this software for academic research and education purposes is subject to the
# conditions and copyright notices in the source code files and in the
# included LICENSE file.


import multiprocessing, queue, signal, time, sys, socket, os, random
import json
from confluent_kafka import Consumer, KafkaException, Producer
import statistics
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
from libchocolatine.asyncfetcher import AsyncHistoryFetcher, runAsyncFetcher
from libchocolatine.arimapredictor import ArimaPredictor


if not sys.warnoptions:
    import warnings
    warnings.filterwarnings("ignore", message="Maximum Likelihood optimization failed to converge")

MODEL_LIFETIME=60 * 24 * 60 * 60

CHOC_MODEL_TYPE_ZERO = 0
CHOC_MODEL_TYPE_ARMA = 1
CHOC_MODEL_TYPE_UNKNOWN = 2

class ChocTimeSeries(object):
    def __init__(self, keystr, freq=300):
        self.keystr = keystr
        self.history = None
        self.datafreq = freq
        self.datasource = "unknown"
        self.entitytype = "unknown"
        self.entitycode = "unknown"

        self.modeltype = CHOC_MODEL_TYPE_UNKNOWN
        self.modeltime = 0
        self.arma = None
        self.arma_source = ""
        self.arma_requested = False
        self.arma_mads_scores = []

        self.backlog = []
        self.predictor = None
        self.predicted = None
        self.pred_intervals = []
        self.predict_source = ""

        self.histslots = {}

        self.stepsperhour = (3600 / self.datafreq)
        self.parseKeyString()
        self.ppw = int((3600 * 24 * 7) / self.datafreq)

        self.baseline = -1;
        self.smallesthist = []

    def addBacklog(self, timestamp, value):
        if len(self.backlog) == 0 or timestamp > self.backlog[-1][0]:
            self.backlog.append((timestamp, value))

        #self.backlog = self.backlog[ -( 6 * int(self.stepsperhour)) : ]

    def clearBacklog(self):
        self.backlog = []

    def getBacklog(self):
        for ts,val in self.backlog:
            yield (ts, val)

    def parseKeyString(self):
        return

    def getDefaultArma(self):
        return (1,1)

    def setDefaultPredIntervals(self, n):
        self.arma_mads_scores = []

    def assignDatabaseModel(self, dbmodel):
        if dbmodel["model_type"] == "ZERO":
            self.arma= (0, 0)
            self.arma_mads_scores = []
            self.modeltype = CHOC_MODEL_TYPE_ZERO
        else:
            self.arma_mads_scores = dbmodel['pred_intervals']
            self.arma = (dbmodel['ar_param'], dbmodel['ma_param'])
            self.modeltype = CHOC_MODEL_TYPE_ARMA

        self.arma_source = "db"
        self.modeltime = dbmodel["generated_at"]

class ChocGtrTimeSeries(ChocTimeSeries):
    def __init__(self, keystr):
        super().__init__(keystr, 60 * 30)
        self.datasource = "gtr"

    def getDefaultArma(self):
        return (1, 1)

    def setDefaultPredIntervals(self, n):
        vals  = [d['signalValue'] for d in self.history if d['signalValue'] is not None]
        median = np.median(vals)

        # TODO figure out some sensible values to go here...
        pdis = []
        for i in range(0, n):
            pdis.append(0.1 * median)
        self.arma_mads_scores = pdis

    def parseKeyString(self):
        x = self.keystr.split('.')
        self.entitytype = "google-service"
        self.entitycode = x[2] + "-" + x[3]

class ChocTelescopeTimeSeries(ChocTimeSeries):
    def __init__(self, keystr):
        super().__init__(keystr, 300)
        self.datasource = "darknet"

    def getDefaultArma(self):
        return (1, 1)

    def setDefaultPredIntervals(self, n):
        vals  = [d['signalValue'] for d in self.history if d['signalValue'] is not None]
        median = np.median(vals)

        # TODO figure out some sensible values to go here...
        pdis = []
        for i in range(0, n):
            pdis.append(0.1 * median)
        self.arma_mads_scores = pdis

    def parseKeyString(self):
        x = self.keystr.split('.')

        if len(x) == 8 and x[3] == "geo":
            self.entitytype = "country"
            self.entitycode = x[6]
        elif len(x) == 7 and x[3] == "geo":
            self.entitytype = "continent"
            self.entitycode = x[5]
        elif len(x) == 9 and x[3] == "geo":
            self.entitytype = "region"
            self.entitycode = x[7]
        elif len(x) == 10 and x[3] == "geo":
            self.entitytype = "county"
            self.entitycode = x[8]
        elif x[4] == "asn":
            self.entitytype = "asn"
            self.entitycode = x[5]

class ChocActiveTimeSeries(ChocTimeSeries):
    def __init__(self, keystr):
        super().__init__(keystr, 600)
        self.datasource = "ping-slash24"

    def getDefaultArma(self):

        return (1,1)

    def setDefaultPredIntervals(self, n):
        vals  = [d['signalValue'] for d in self.history if d['signalValue'] is not None]
        median = np.median(vals)

        pdis = []
        for i in range(0, n):
            pdis.append(0.1 * median)
        self.arma_mads_scores = pdis

    def parseKeyString(self):
        x = self.keystr.split('.')

        if len(x) == 11 and x[2] == "geo":
            self.entitytype = "country"
            self.entitycode = x[5]
        elif len(x) == 10 and x[2] == "geo":
            self.entitytype = "continent"
            self.entitycode = x[4]
        elif len(x) == 12 and x[2] == "geo":
            self.entitytype = "region"
            self.entitycode = x[6]
        elif len(x) == 13 and x[2] == "geo":
            self.entitytype = "county"
            self.entitycode = x[7]
        elif x[2] == "asn":
            self.entitytype = "asn"
            self.entitycode = x[3]


class ChocBgpTimeSeries(ChocTimeSeries):
    def __init__(self, keystr):
        super().__init__(keystr, 300)
        self.datasource = "bgp"

    def getDefaultArma(self):

        return (1,1)

    def setDefaultPredIntervals(self, n):
        vals  = [d['signalValue'] for d in self.history if d['signalValue'] is not None]
        median = np.median(vals)

        pdis = []
        for i in range(0, n):
            pdis.append(0.001 * median)
        self.arma_mads_scores = pdis

    def parseKeyString(self):
        x = self.keystr.split('.')

        if len(x) == 10 and x[2] == "geo":
            self.entitytype = "country"
            self.entitycode = x[5]
        elif len(x) == 9 and x[2] == "geo":
            self.entitytype = "continent"
            self.entitycode = x[4]
        elif len(x) == 11 and x[2] == "geo":
            self.entitytype = "region"
            self.entitycode = x[6]
        elif len(x) == 12 and x[2] == "geo":
            self.entitytype = "county"
            self.entitycode = x[7]
        elif x[2] == "asn":
            self.entitytype = "asn"
            self.entitycode = x[3]

class ChocolatineDetector(object):

    def __init__(self, name, iodaapi, kafkaconf, dbconf, maxarma=3):
        self.iodaapi = iodaapi
        self.kafkaconf = kafkaconf
        self.series = {}
        self.maxarma = maxarma
        self.name = name
        self.running = None
        self.dbconf = dbconf

        self.oob = multiprocessing.Queue()
        self.inq = multiprocessing.Queue()
        self.evqueue = multiprocessing.Queue()

        self.kafkaModelReq = None
        self.kafkaModelReply = None
        self.kafkaReqId = ""

        self.histRequest = multiprocessing.Queue()
        self.histReply = multiprocessing.Queue()

        self.asyncfetcher = None
        self.dbsession = None
        self.dbcursor = None
        self.ignorekeys = set()

    def _sendModelRequest(self, serieskey, timestamp, kafkatopic):
        req = {
            "requestedby": self.kafkaReqId,
            "ts": timestamp,
            "arma_limit": self.maxarma,
            "fqid": serieskey
        }

        data = json.dumps(req)

        try:
            self.kafkaModelReq.produce(kafkatopic + ".requests", data)
        except BufferError:
            print("buffer too full to send model request?")

        self.kafkaModelReq.poll(0)
        self.kafkaModelReq.flush()

    def connectDatabase(self):
        dbname = self.dbconf.get("name", "models")
        dbhost = self.dbconf.get("host", "localhost")
        dbport = self.dbconf.get("port", 5432)

        dbpword = os.getenv("PSQL_PASSWORD")

        if dbpword is None:
            print("Unable to determine PSQL password")
            return None

        self.dbsession = psycopg2.connect(database=dbname, user='postgres', password=dbpword, host=dbhost, port=dbport)
        self.dbcursor = self.dbsession.cursor(
                cursor_factory = psycopg2.extras.RealDictCursor)
        return self.dbcursor

    def lookupModelInDatabase(self, serieskey):

        assert(self.dbsession is not None)
        query = """SELECT * FROM public.arma_models WHERE fqid = %s
                ORDER BY generated_at DESC;"""
        self.dbcursor.execute(query, (serieskey,))

        if self.dbcursor.rowcount <= 0:
            return None
        x = self.dbcursor.fetchone()
        return x

    def start(self):
        #conf = {'bootstrap.servers': self.kafkaconf['bootstrap-model'],
        #    'debug': "topic,msg,broker" }

        #self.kafkaModelReq = Producer(**conf)


        self.kafkaReqId = "%s-%d-%u-%u" % (socket.gethostname(), os.getpid(), time.time(), random.randint(1,10000000))

        p = multiprocessing.Process(target=runChocDetector, daemon=True,
                args = (self,), name="ChocolatineDetector-%s" % (self.name))
        p.start()
        self.running = p
        return p

    def halt(self):
        if self.running is not None:
            self.oob.put(None)
            self.running.join()
            self.running = None

    def queueLiveData(self, serieskey, timestamp, value):
        self.inq.put((0, serieskey, timestamp, value))

    def createNewSeries(self, serieskey, timestamp):
        seriestype = serieskey.split('.')[0]
        if seriestype == "bgp":
            s = ChocBgpTimeSeries(serieskey)
        elif seriestype == "google_tr" or seriestype == "gtr":
            s = ChocGtrTimeSeries(serieskey)
        elif seriestype == "darknet":
            s = ChocTelescopeTimeSeries(serieskey)
        elif seriestype == "active":
            s = ChocActiveTimeSeries(serieskey)
        else:
            return None

        self.series[serieskey] = s

        # Lookup possible model in DB
        dbmodel = self.lookupModelInDatabase(serieskey)
        now = time.time()
        if dbmodel is None or now - dbmodel["generated_at"] > MODEL_LIFETIME:
            if dbmodel and dbmodel["training_start"] > timestamp:
                # only replace a model if we are processing more recent data
                # than the data that was used to generate the old model
                pass
            elif not s.arma_requested:
                kafkatopic = self.kafkaconf["modellertopic"]
                self._sendModelRequest(serieskey, timestamp, kafkatopic)
                s.arma_requested = True

                print("Sent model request for series %s to %s.requests" % \
                    (serieskey, kafkatopic))

        if dbmodel is None:
            # set a default model to use for now -- prediction intervals
            # will need to be calculated once we have historical data
            # available...
            s.arma  = s.getDefaultArma()
            s.arma_source = "default"
            s.modeltype = CHOC_MODEL_TYPE_UNKNOWN

        else:
            s.assignDatabaseModel(dbmodel)
        return s

    def sendHistoryRequest(self, s, serieskey, timestamp):
        histjob = (serieskey, timestamp, 10 * 7 * 24 * 60 * 60, s.datafreq)
        self.histRequest.put(histjob)

    def updateSeriesWithNewModel(self, modelresp):
        if modelresp['fqid'] not in self.series:
            print("Warning: received a model for '%s', but we do not appear to have requested it?" % (modelresp['fqid']))
            return

        s = self.series[modelresp['fqid']]
        s.arma_requested = False
        s.modeltime = modelresp["generated_at"]
        if modelresp['ar_param'] is None:
            # no valid model was found, just rely on default
            return
        s.arma = (modelresp['ar_param'], modelresp['ma_param'])
        s.arma_source = "modeller"
        print("Received new %s model from modeller for series %s" % (modelresp["model_type"], modelresp['fqid']))

        if modelresp["model_type"] == "ZERO":
            s.modeltype = CHOC_MODEL_TYPE_ZERO
        else:
            s.modeltype = CHOC_MODEL_TYPE_ARMA
            s.arma_mads_scores = modelresp['mads']

            if len(s.histslots) > 0:
                medians = {}
                for k,v in s.histslots.items():
                    medians[k] = statistics.median(sorted(v))
                s.predictor = ArimaPredictor(s.arma, s.datafreq)
                s.predictor.bootstrapHistory(s.history[-4 * s.ppw:], medians,
                        60 * 60 * 24 * 7 * 2)
                s.predicted = s.predictor.forecast(12)
                s.pred_intervals = s.arma_mads_scores.copy()
                s.predict_source = s.arma_source

    def getLiveDataResult(self, block=False):
        try:
            res = self.evqueue.get(block)
        except queue.Empty:
            return None

        return res

    def processLiveData(self, serieskey, timestamp, value):
        event = None

        if serieskey not in self.series:
            s = self.createNewSeries(serieskey, timestamp)
            self.sendHistoryRequest(s, serieskey, timestamp)
        else:
            s = self.series[serieskey]

        if s is None:
            return None, False

        if s.history is None:
            s.addBacklog(timestamp, value)
            return None, False

        now = time.time()
        if now - s.modeltime > MODEL_LIFETIME and s.arma_requested == False:
            # our model is getting out of date -- request an update
            # from the model generator
            kafkatopic = self.kafkaconf["modellertopic"]
            self._sendModelRequest(serieskey, timestamp, kafkatopic)
            s.arma_requested = True

            print("Sent model request for series %s to %s.requests" % \
                (serieskey, kafkatopic))

        if s.modeltype == CHOC_MODEL_TYPE_ZERO:
            event = {
                 "timestamp": timestamp,
                 "observed": value,
                 "predicted": 0,
                 "threshold": 0,
                 "norm_threshold": 0,
                 "alertable": False
            }
            return event, True

        # compare live value against predicted values (or if timestamp >
        # last prediction, force a fresh set of predictions and then do
        # the comparison)
        while s.predicted is None or len(s.predicted) == 0 or \
                timestamp > s.predicted[-1]["timestamp"]:
            # there's a big gap in the observed data, so we need to use
            # our forecasts to "fill" the gap and allow us to keep
            # predicting forward until we reach the timestamp of the
            # next actual data point
            if (s.predicted and len(s.predicted) > 0):
                for p in s.predicted:
                    s.predictor.appendHistory(p['forecast'], p['timestamp'],
                            p['forecast'], False)

            #print("Forecasting... %s" % (serieskey))
            s.predicted = s.predictor.forecast(12)
            s.pred_intervals = s.arma_mads_scores.copy()
            s.predict_source = s.arma_source
            #print(s.predicted)

        while len(s.predicted) > 0:
            p = s.predicted[0]
            if p["timestamp"] < timestamp and p["forecast"] is not None:
                s.predictor.appendHistory(p["forecast"], timestamp,
                        p["forecast"], False)
                s.predicted = s.predicted[1:]
                continue

            if p["timestamp"] > timestamp:
                break

            if p["forecast"] is None:
                s.predictor.appendHistory(value, timestamp, None, False)
                break

            if s.pred_intervals is None or p["index"] >= len(s.pred_intervals):
                break

            #print("Comparing %.1f with prediction %.1f (MAD = %.4f, src=%s) -- %s @ %u" % \
            #        (value, p["forecast"], s.pred_intervals[p["index"]],
            #        s.predict_source, serieskey, timestamp))
            s.predicted = s.predicted[1:]

            # Is the observed value anomalous?
            # Yes -- add to events (if the forecast is higher than the
            # observed), update history using "predicted" value

            # No -- update history using observed value

            event = {
                "timestamp": timestamp,
                 "observed": value,
                 "predicted": p["forecast"],
                 "threshold": p["forecast"] * s.pred_intervals[p["index"]],
                 "norm_threshold": p["forecast"] * \
                        (1 - ((1 - s.pred_intervals[p["index"]]) * 0.33)),
                 "alertable": False,
                 "baseline": s.baseline,
            }

            if event['threshold'] <= 0:
                event['threshold'] = 0

            if event["predicted"] - event["threshold"] < 1.0:
                event['threshold'] = event["predicted"] - 1.0
                event['norm_threshold'] = event['predicted']

            if value < event['threshold']:
                event['alertable'] = True
                s.predictor.appendHistory(value, timestamp, p["forecast"], True)
            elif value > p["forecast"] + s.pred_intervals[p["index"]]:
                event['alertable'] = False
                s.predictor.appendHistory(value, timestamp, p["forecast"], True)
            else:
                event['alertable'] = False
                s.predictor.appendHistory(value, timestamp, p["forecast"], False)
            break

        return event, True

    def addTestSeries(self):
        if self.name == "bgp-test":
            series = ["bgp.prefix-visibility.geo.netacuity.OC.AU.v4.visibility_threshold.min_50%_ff_peer_asns.visible_slash24_cnt"]
        elif self.name == "gtr-test":
            series = ["google_tr.NA.US.SPREADSHEETS.traffic"]
        elif self.name == "telescope-test":
            series = []
        else:
            series = []

        now = time.time()
        now -= now % 300

        for skey in series:
            s = self.createNewSeries(skey, now)
            self.sendHistoryRequest(s, skey, now)

    def processHistoryData(self, hist, serieskey):

        if serieskey not in self.series:
            print("Received unexpected history for unknown series:", serieskey)
            return

        s = self.series[serieskey]
        ts = hist['from']
        step = hist['step']
        native = hist['nativeStep']

        assert(step == native)

        lasttimestamp = 0
        res = []
        for v in hist['values']:
            res.append({"timestamp": pd.Timestamp(ts, unit='s'),
                    "signalValue": v})
            lasttimestamp = ts
            ts += step

            if v is not None:
                slot = ts % (60 * 60 * 24 * 7 * 2)

                if slot not in s.histslots:
                    s.histslots[slot] = [int(v)]
                else:
                    s.histslots[slot].append(int(v))

                if len(s.smallesthist) < 40:
                    s.smallesthist.append(v)
                    s.smallesthist = sorted(s.smallesthist)
                elif v < s.smallesthist[-1]:
                    s.smallesthist[-1] = v
                    s.smallesthist = sorted(s.smallesthist)


        s.history = res
        if s.modeltype != CHOC_MODEL_TYPE_ZERO:
            medians = {}
            for k,v in s.histslots.items():
                medians[k] = statistics.median(sorted(v))

            if s.arma_source == "default":
                s.setDefaultPredIntervals(12)

            # Now that we have historic data, we can start to introduce
            # predictions

            s.predictor = ArimaPredictor(s.arma, s.datafreq)
            s.predictor.bootstrapHistory(s.history[-4 * s.ppw:], medians,
                    60 * 60 * 24 * 7 * 2)

            s.predicted = s.predictor.forecast(12)
            s.pred_intervals = s.arma_mads_scores.copy()
            s.predict_source = s.arma_source

        s.baseline = max(1, statistics.median(s.smallesthist))
        for bl in s.getBacklog():
            if bl[0] > lasttimestamp and bl[1] is not None:
                ev, toput = self.processLiveData(serieskey, bl[0], bl[1])
                if toput:
                    self.evqueue.put((serieskey, bl[0], ev))
                #if ev is not None and ev['alertable']:
                #    print("EVENT:", serieskey, ev)
                lasttimestamp = bl[0]
        s.clearBacklog()

    def run(self):
        try:
            job = self.oob.get(False)
            return -1
        except queue.Empty:
            pass

        try:
            serieskey, histdata = self.histReply.get(False)
        except queue.Empty:
            histdata = []

        # update series with history and begin predicting
        if histdata is not None and len(histdata) > 0:
            for d in histdata[0]:
                self.processHistoryData(d, serieskey)
        elif histdata is None:
            self.ignorekeys.add(serieskey)

        # check for model responses from modeller
        try:
            recvd = 0
            while 1:
                respmsg = self.kafkaModelReply.poll(0)
                if respmsg is None:
                    break

                if respmsg.error():
                    raise KafkaException(respmsg.error())
                else:
                    resp = json.loads(respmsg.value())

                    if resp['requestedby'] == self.kafkaReqId:
                        # update series with newly acquired model
                        self.updateSeriesWithNewModel(resp)

                    recvd += 1
                    # don't spend too long receiving model responses as
                    # we also need to be reading from our live data
                    # source(s)
                    if recvd >= 100:
                        break
        except:
            return -1

        try:
            job = self.inq.get(False)
        except queue.Empty:
            return 1

        if job[0] == 0 and job[1] not in self.ignorekeys:
            ev, toput = self.processLiveData(job[1], job[2], job[3])
            if toput:
                self.evqueue.put((job[1], job[2], ev))
            #if ev is not None and ev['alertable']:
            #    print("EVENT:", job[1], ev)

        return 0


def runChocDetector(det):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    conf = {'bootstrap.servers': det.kafkaconf['bootstrap-model']}

    det.kafkaModelReq = Producer(**conf)

    cons_conf = {'bootstrap.servers': det.kafkaconf['bootstrap-model'],
            'group.id': det.kafkaconf['group'],
            'session.timeout.ms': 60000,
            'auto.offset.reset': 'earliest',
    }

    det.kafkaModelReply = Consumer(cons_conf)
    det.kafkaModelReply.subscribe([det.kafkaconf['modellertopic'] + \
            ".generated"])

    det.connectDatabase()

    # TEMPORARY, for testing purposes
    det.addTestSeries()

    while True:
        x = det.run()
        if x < 0:
            break
        elif x > 0:
            time.sleep(x)

    if det.kafkaModelReply:
        det.kafkaModelReply.close()

def filterIODAKeys(key):
    # XXX one day we should allow users to provide regexes

    comps = key.split('.')

    if comps[0] == 'bgp':
        if comps[1] != "prefix-visibility":
            return False
        if comps[2] == "overall":
            return False
        if comps[-1] != "visible_slash24_cnt":
            return False
        if comps[-2] != "min_50%_ff_peer_asns":
            return False

        # XXX temporary for testing/debug
        if len(comps) != 10 or comps[5] not in ["NZ", "AU", "DE"] \
                or comps[2] != "geo":
             return False

        return True

    if comps[0] == "darknet":
        if len(comps) != 8 or comps[-2] not in ["NZ", "IT"] or comps[3] != "geo":
            return False
        return True

    if comps[0] == "google_tr":
        if comps[-1] != "traffic":
            print(comps)
            return False

        # XXX temporary for testing/dev/debug
        if comps[-2] != "WEB_SEARCH":
            return False
        if comps[-3] != "US":
            return False

        return True

    return False

def parseGraphiteBatch(msg):

    decoded = msg.value().decode('utf-8')
    for line in decoded.split('\n'):
        l = line.split(" ")
        if len(l) != 3:
            continue
        key = l[0]
        value = int(l[1])
        timestamp = int(l[2])

        # only return keys that match series we use for IODA signals
        if filterIODAKeys(key):
            yield(key, value, timestamp)


IODAAPI = "http://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw"

def runTestInstance(detector, kafkaconf):

    # Start an asynchronous HTTP request handler
    fetcher = AsyncHistoryFetcher(IODAAPI, detector.histRequest,
            detector.histReply)

    fetcher.start()
    detector.start()

    conf = {
            'bootstrap.servers': kafkaconf['bootstrap-live'],
            'group.id': kafkaconf['group'],
            'session.timeout.ms': 6000,
            'auto.offset.reset': "earliest"
        }

    kc = Consumer(conf)
    kc.subscribe([kafkaconf['livetopic']])

    try:
        while True:
            msg = kc.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                for p in parseGraphiteBatch(msg):
                    keystr, value, ts = p

                    detector.queueLiveData(keystr, ts, value)
    except KeyboardInterrupt:
        pass
    finally:
        kc.close()

    fetcher.halt()
    detector.halt()

def runTestGtrInstance():

    kafkaconf = {"livetopic": "tsk-production.graphite.gtr",
            "modellertopic": "chocolatine.model",
            "bootstrap-live": "procida.cc.gatech.edu:9092",
            "bootstrap-model": "capri.cc.gatech.edu:9092",
            "group": "choc.testing.gtr"
    }

    detector = ChocolatineDetector("gtr-test", IODAAPI, kafkaconf, {})
    runTestInstance(detector, kafkaconf)

def runTestTelescopeInstance():

    kafkaconf = {"livetopic": "tsk-production.graphite.darknet.merit-nt.non-erratic",
            "modellertopic": "chocolatine.model",
            "bootstrap-live": "cirella.cc.gatech.edu:9092",
            "bootstrap-model": "capri.cc.gatech.edu:9092",
            "group": "choc.testing.darknet"
    }

    detector = ChocolatineDetector("telescope-test", IODAAPI, kafkaconf, {})
    runTestInstance(detector, kafkaconf)

def runTestBgpInstance():
    kafkaconf = {"livetopic": "tsk-production.bgp.5min",
            "modellertopic": "chocolatine.model",
            "bootstrap-live": "capri.cc.gatech.edu:9092",
            "bootstrap-model": "capri.cc.gatech.edu:9092",
            "group": "choc.testing.bgp"
    }

    detector = ChocolatineDetector("bgp-test", IODAAPI, kafkaconf, {})
    runTestInstance(detector, kafkaconf)

if __name__ == "__main__":
    #runTestBgpInstance()
    if len(sys.argv) == 1:
        runTestBgpInstance()
    elif sys.argv[1] == "bgp":
        runTestBgpInstance()
    elif sys.argv[1] == "darknet":
        runTestTelescopeInstance()
    elif sys.argv[1] == "gtr":
        runTestGtrInstance()
    else:
        print("Unknown test instance type:", sys.argv[1], "... exiting")
