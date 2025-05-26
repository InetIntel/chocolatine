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

import multiprocessing, queue, signal, time, json
import asyncio, aiohttp, logging, requests, threading
from aiohttp import ClientSession, ClientResponseError
import urllib.error, urllib.parse

def formActiveProbingQuery(serieskey, endtime, duration):
    keysplit = serieskey.split('.')

    if len(keysplit) < 6:
        return None, None

    dataSource = "ping-slash24"
    if keysplit[2] == "geo":
        if len(keysplit) == 10:
            entityType = "continent"
        elif len(keysplit) == 11:
            entityType = "country"
        elif len(keysplit) == 12:
            entityType = "region"
        elif len(keysplit) == 13:
            entityType = "county"
        entityCode = keysplit[-6]
    elif keysplit[2] == "asn":
        entityType = "asn"
        entityCode = keysplit[3]
    else:
        return None, None
    meta = {
        "datasource": dataSource,
        "entitytype": entityType,
        "entitycode": entityCode,
        "fetchstart": endtime - duration,
        "duration": duration,
        "endtime": endtime
    }

    return meta

def formBgpQuery(serieskey, endtime, duration):
    keysplit = serieskey.split('.')

    if len(keysplit) < 4:
        return None, None

    dataSource = "bgp"
    if keysplit[2] == "geo":
        if len(keysplit) == 9:
            entityType = "continent"
        elif len(keysplit) == 10:
            entityType = "country"
        elif len(keysplit) == 11:
            entityType = "region"
        elif len(keysplit) == 12:
            entityType = "county"
        entityCode = keysplit[-5]
    elif keysplit[2] == "asn":
        entityType = "asn"
        entityCode = keysplit[3]
    else:
        return None, None
    meta = {
        "datasource": dataSource,
        "entitytype": entityType,
        "entitycode": entityCode,
        "fetchstart": endtime - duration,
        "duration": duration,
        "endtime": endtime
    }

    return meta

def formTelescopeQuery(serieskey, endtime, duration):
    keysplit = serieskey.split('.')

    if len(keysplit) < 4:
        return None

    dataSource = keysplit[1]
    if keysplit[3] == "geo":
        if len(keysplit) == 7:
            entityType = "continent"
        elif len(keysplit) == 8:
            entityType = "country"
        elif len(keysplit) == 9:
            entityType = "region"
        elif len(keysplit) == 10:
            entityType = "county"
        else:
            entityType = "country"
        entityCode = keysplit[-2]
    elif keysplit[3] == "routing":
        entityType = "asn"
        entityCode = keysplit[-2]
    else:
        return None


    meta = {
        "datasource": dataSource,
        "entitytype": entityType,
        "entitycode": entityCode,
        "fetchstart": endtime - duration,
        "duration": duration,
        "endtime": endtime
    }

    return meta

def formGtrQuery(serieskey, endtime, duration):

    keysplit = serieskey.split('.')

    if len(keysplit) < 4:
        return None

    meta = {
        "datasource": "gtr",
        "entitytype": "country",
        "entitycode": keysplit[2],
        "sourceparams": keysplit[3],
        "fetchstart": endtime - duration,
        "duration": duration,
        "endtime": endtime
    }

    return meta

def formHistoryQuery(serieskey, endtime, duration, mininterval=60):
    seriestype = serieskey.split('.')[0]
    meta = None

    if seriestype == "bgp":
        meta = formBgpQuery(serieskey, endtime, duration)
    elif seriestype == "active":
        meta = formActiveProbingQuery(serieskey, endtime, duration)
    elif seriestype == "darknet":
        meta = formTelescopeQuery(serieskey, endtime, duration)
    elif seriestype == "google_tr" or seriestype == "gtr":
        meta = formGtrQuery(serieskey, endtime, duration)

    if meta is None:
        return None, None

    if meta['entitycode'] == '??':
        return None, meta

    if "sourceparams" in meta:
        qargs = "/%s/%s?from=%u&until=%u&datasource=%s&maxPoints=%u&sourceParams=%s" % ( \
            meta['entitytype'], meta['entitycode'],
            meta['fetchstart'], meta['endtime'],
            meta['datasource'], (meta['duration'] / mininterval) + 1,
            meta['sourceparams'])
    else:
        qargs = "/%s/%s?from=%u&until=%u&datasource=%s&maxPoints=%u" % ( \
            meta['entitytype'], meta['entitycode'],
            meta['fetchstart'], meta['endtime'],
            meta['datasource'], (meta['duration'] / mininterval) + 1)

    return qargs, meta


def fetchIodaHistoricBlocking(apiurl, serieskey, endtime, duration,
        mininterval=60):
    qargs, meta = formHistoryQuery(serieskey, endtime, duration, mininterval)

    if qargs is None:
        return None, meta

    try:
        resp = requests.get(apiurl + qargs)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        return None, meta
    except Exception as err:
        print(f'Non-HTTP error occurred {err}')
        return None, meta

    jsonresult = resp.json()['data'][0][0]
    return jsonresult, meta

def fetchIodaMeta(serieskey, endtime, duration):

    seriestype = serieskey.split('.')[0]
    meta = None

    if seriestype == "bgp":
        meta = formBgpQuery(serieskey, endtime, duration)
    elif seriestype == "active":
        meta = formActiveProbingQuery(serieskey, endtime, duration)
    elif seriestype == "darknet":
        meta = formTelescopeQuery(serieskey, endtime, duration)
    elif seriestype == "google_tr" or seriestype == "gtr":
        meta = formGtrQuery(serieskey, endtime, duration)

    return meta

async def fetch(job, session, api):

    queryargs, meta = formHistoryQuery(job[0], job[1], job[2], job[3])

    if queryargs is None:
        return None

    url = api + queryargs
    retries = 0

    while retries < 10:
        try:
            async with session.get(url, timeout=30) as response:
                resp = await response.json()
        except ClientResponseError as e:
            logging.warning(e.code)
            break
        except asyncio.TimeoutError:
            retries += 1
        except Exception as e:
            logging.warning(e)
            break
        else:
            return resp, job[0]

    if retries >= 20:
        logging.warning("Unable to fetch data from IODA API without timing out: %s" % (url))


    return None


class AsyncHistoryFetcher(object):

    def __init__(self, api, inq, outq):
        self.api = api
        self.inq = inq
        self.outq = outq
        self.pending = set()
        self.fetchThread = None
        self.internal = None
        self._stop_event = threading.Event()
        self._event_loop = None

    def _reader_thread(self):
        while not self._stop_event.is_set():
            try:
                job = self.inq.get(timeout=0.1)
                future = asyncio.run_coroutine_threadsafe(
                        self.internal.put(job), self._event_loop)
                future.result()
                if job is None:
                    break
            except queue.Empty:
                continue


    async def run(self):
        jobcount = 0
        try:
            job = await self.internal.get()

            if job is None:
                for p in self.pending:
                    p.cancel()
                await asyncio.gather(*self.pending, return_exceptions=True)
                return -1

            task = asyncio.create_task(fetch(job, self.session, self.api))
            self.pending.add(task)

        except queue.Empty:
            pass

        if len(self.pending) == 0:
            return 1

        done, active = await asyncio.wait(self.pending, timeout=1.0)
        self.pending = active

        for d in done:
            res = d.result()
            if res is not None:
                self.outq.put((res[1], res[0]['data']))
        return 0

    def halt(self):
        if self.fetchThread is not None:
            self.fetchThread.join()
            self.fetchThread = None

    def start(self):
        p = multiprocessing.Process(target=runAsyncFetcher, daemon=False,
            args = (self,), name="AsyncFetcher")
        p.start()
        self.fetchThread = p
        return p




async def asyncmain(fetch):
    # TODO fix SSL in IODA API so we don't have to do this nasty hack
    fetch.session = ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))

    while True:
        x = await fetch.run()
        if x < 0:
            break
        await asyncio.sleep(0.1)

def runAsyncFetcher(fetch):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    fetch._event_loop = loop
    fetch.internal = asyncio.Queue()

    reader = threading.Thread(target=fetch._reader_thread, daemon=True)
    reader.start()

    try:
        loop.run_until_complete(asyncmain(fetch))
    finally:
        fetch._stop_event.set()
        reader.join()
        loop.run_until_complete(fetch.session.close())
        loop.close()
        fetch.outq.close()


