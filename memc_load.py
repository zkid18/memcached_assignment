#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# protoc  --python_out=. ./appsinstalled.proto
import appsinstalled_pb2
import pymemcache
from multiprocessing import Pool, TimeoutError
from threading import Thread
from queue import Queue, Empty

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

TIMEOUT = 0.1
CONNECT_TIMEOUT = 5
SENTINEL = object()


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))

class MemcachedThread(Thread):
    """
    MemcachedTread

    Creates the Client for a single memcached server.
    """
    def __init__(self, job_queue, result_queue, memc, dry_run=False, attemps = 1):
        """
        Constructor.

        Args:
            job_queue: Queue
            result_queue: Queue
            memc: Memcache client
            dry_run: Store True
            attemps: attemps to restart
        
        Notes:

        """
        super().__init__()
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.memc = memc
        self.dry_run = dry_run
        self.attemps = attemps


    def run(self):
        '''
        Run Writer
        '''
        logging.debug("Worker [{}] starts thread {}".format(os.getpid(), self.name))
        processed = errors = 0
        while True:
            try:
                chunks = self.job_queue.get(timeout=0.1)
                if chunks == SENTINEL:
                    self.result_queue.put((processed, errors))
                    logging.info('[Worker %s] Stop thread: %s' % (os.getpid(), self.name))
                    break
                else:
                    proc_items, err_items = self.insert_appsinstalled(chunks)
                    processed += proc_items
                    errors += err_items
            except Empty:
                continue
        

    def insert_appsinstalled(self, batches):
        processed = errors = 0
        try:
            if self.dry_run:
                for key, values in batches.items():
                    logging.debug("{} - {} -> {}".format(self.memc, key, values))
                    processed += 1
            else:
                for key, value in batches.items():
                    self.memc.set(key, value)
        except Exception as e:
            logging.exception("Cannot write to memc {}: {}".format(self.memc.servers[0], e))
            errors += 1
            return processed, errors
        return processed, errors



def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def serializeapp(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # print("ser", key, packed)
    return key, packed


def file_processing(fn, options):
    '''
    Create thread_pool for every new task
    Every thread exits after completing the task. 

    To overcome the problem of so many threads, creat ehte pool of threads
    for each task to execute execute another task when it is finished (so long as there are more tasks to run).  

    When all tasks are done, we must tell the threads to exit. 
    We paass in a special SENTINEL argument that is different from any argument that the caller could pass.
    '''
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    thread_pool = {}
    job_pool = {}
    result_queue = Queue()
    chunks = {}

    for device_type, memc_addr in device_memc.items():
        memc = pymemcache.Client([memc_addr], timeout=TIMEOUT)
        job_pool[device_type] = Queue()
        worker = MemcachedThread(job_pool[device_type], 
                                result_queue,
                                memc, 
                                options.dry,
                                )
        thread_pool[device_type] = worker
        chunks[device_type] = {}
        worker.start()

    logging.info('[Worker %s] Processing %s' % (os.getpid(), fn))
    fd = gzip.open(fn)
    processed = errors = 0

    for line in fd:
        line = line.decode().strip()
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue
            
        key, packed = serializeapp(appsinstalled)
        chunk = chunks[device_type]
        chunk[key] = packed

    for dev_type, chunk in chunks.items():
        if chunk:
            job_pool[dev_type].put(chunk)

    for queue in job_pool.values():
        queue.put(SENTINEL)

    for thread in thread_pool.values():
        # to ensure that all threads have finished, call join 
        thread.join()
    
    while not result_queue.empty():
        result = result_queue.get(timeout=0.1)
        processed += result[0]
        errors += result[1]

    if not processed:
        fd.close()
        # dot_rename(fn)
        return fn

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    fd.close()
    return fn


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


def main(options):
    '''
    Create pool of precess which will carry out the task submitted to it
    '''
    with Pool(int(options.workers)) as pool:
        process_args = (
                        (file_name, options) 
                        for file_name in sorted(glob.iglob(options.pattern))
                        )
        for fn in pool.starmap(file_processing, process_args):
            file_processing(fn, options)
            logging.info('Renaming %s' % fn)
            # dot_rename(fn)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--workers", action="store", default="3")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)