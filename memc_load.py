#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
from multiprocessing import Pool, TimeoutError
from threading import Thread
from queue import Queue, Empty

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

TIMEOUT = 0.1
CONNECT_TIMEOUT = 5

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))

class MemchaedThread(Thread):
    def __init__(self, job_queue, result_queue, memc, dry_run=False, attemps = 1):
        super().__init__()
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.memc = memc
        self.dry_run = dry_run
        self.attemps = attemps


    def run(self):
        '''
        Override the run() method to specify an activity
        '''
        logging.debug("Worker [{}] starts thread {}".format(os.getpid(), self.name))
        try:
            batches = self.job_queue.get(timeout=TIMEOUT)
            processed = errors = 0
            ok = self.insert_appsinstalled(batches)
            if ok:
                processed += 1
            else:
                errors += 1
            self.result_queue.put((processed, errors))
        except Empty:
            pass
        

    def insert_appsinstalled(self, batches):
        try:
            if self.dry_run:
                for key, values in batches.items():
                    logging.debug("{} - {} -> {}".format(self.memc, key, values))
            else:
                self.memcached_set(batches)
        except Exception as e:
            logging.exception("Cannot write to memc {}: {}".format(self.memc.servers[0], e))
            return False
        return True

    def memcached_set(self, batches):
        for key, value in batches.items():
            self.memc.set(key, value)



def parse_appsinstalled(line):
    line_parts = line.decode('utf-8').strip().split("\t")
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
    return key, packed


def file_processing(fn, options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    thread_pool = {}
    job_pool = {}
    result_queue = Queue()
    batches = {}

    for device_type, memc_addr in device_memc.items():
        memc = memcache.Client([memc_addr])
        job_pool[device_type] = Queue()
        worker = MemchaedThread(job_pool[device_type], 
                                result_queue,
                                memc, 
                                options.dry,
                                )
        thread_pool[device_type] = worker
        batches[device_type] = {}
        worker.start()


    fd = gzip.open(fn)
    processed = errors = 0
    for line in fd:
        line = line.strip()
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
        batch = batches[device_type]
        batch[key] = packed
        if batch:
            job_pool[device_type].put(batch)
        for thread in thread_pool.values():
            thread.join()
        while not result_queue.empty():
            results = result_queue.get(TIMEOUT)
            print(results)
            processed += results[0]
            errors += results[1]

        if not processed:
            fd.close()
            dot_rename(fn)
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
            logging.info('Renaming %s' % fn)
            dot_rename(fn)


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