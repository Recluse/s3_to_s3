#!/usr/bin/python
# -*- coding: utf-8 -*-
""" This module works only as script.
Russian:
    Скрипт предназначен для копирования ВСЕХ объектов (или всех объектов для префикса) из одного бакета s3 в другой
    на другом сервере (или на том же самом с другими ключами).

English:
    Script puprose: copy ALL objects(or all objects with given prefix) from one s3 bucket to another
    (on another server or other pair of access keys)
"""

import logging
import threading
import time
import io
import sys
import os
import signal

import boto3
import botocore
from botocore.utils import fix_s3_host

if sys.version_info[0] == 2:
    import ConfigParser as configparser
    import Queue
else:
    import configparser
    import queue as Queue

DEFAULT_CONF = "s3_to_s3.conf"
# config sections
DST_SECTION = "Destination"
SRC_SECTION = "Source"
GLOBAL_SECTION = "Global"
DEBUG = False

queue = Queue.Queue()
logger = logging.getLogger("S3_to_S3")


def parse_conf(conf_file=DEFAULT_CONF):
    """ Parse config from file and return it"""

    def to_bool(value):
        "Converts value to True"
        if isinstance(value, bool):
            return value
        if not value:
            return False
        return value.lower() in ['true', '1', 't', 'y', 'yes']

    class ConfigParserWithDefaults(configparser.ConfigParser, object):
        """ Replace default get method with dict-like get, that can return defaults"""

        def __init__(self, *args, **kwargs):
            if 'filename' in kwargs:
                self._filename = kwargs.pop('filename')
            else:
                raise ValueError("No config filename defined!")
            self._logger = logging.getLogger("S3_to_S3")
            super(ConfigParserWithDefaults, self).__init__(*args, **kwargs)
            with io.open(self._filename, encoding="utf-8") as f:
                self.readfp(f)

        def get_with_default(self, section, option, default=None, conv=None, **kwargs):
            """Get value from super or return default"""
            try:
                result = super(ConfigParserWithDefaults, self).get(section, option, **kwargs)
            except (configparser.NoOptionError, configparser.NoSectionError) as e:
                result = default
            if conv and callable(conv):
                return conv(result)
            return result

        def get(self, section, option, default=None, **kwargs):
            return self.get_with_default(section, option, default, **kwargs)

        # pylint: disable=arguments-differ
        def getboolean(self, section, option, default=None):
            return self.get_with_default(section, option, default, conv=to_bool)

        # pylint: disable=W0221
        def getint(self, section, option, default=None):
            try:
                default = int(default)
            except (ValueError, TypeError):
                raise ValueError("Default value should be int!")
            return self.get_with_default(section, option, default, conv=int)

        def get_marker(self):
            """Marker used for s3 file_list paging, we need to store it somewhere if we want to resume
            execution from given marker"""
            marker = self.get(GLOBAL_SECTION, "marker", None)
            if not marker:
                raise ValueError("No marker in config file! Check your config or drop the \"-r\" flag")
            return marker

        def set_marker(self, value):
            """Set marker value in config"""
            self._logger.debug("Setting up marker value %s", value)
            self.set(GLOBAL_SECTION, "marker", value)
            with open(self._filename, "w") as f:
                self.write(f)

    if os.path.exists(conf_file):
        return ConfigParserWithDefaults(filename=conf_file, allow_no_value=True)


def setup_logging(filename):
    """setup logger and filehandler for it"""
    logger = logging.getLogger("S3_to_S3")
    logger.setLevel(logging.DEBUG)
    if not filename.endswith('.log'):
        filename += ".log"
    logfile = logging.FileHandler(filename)
    logfile.setLevel(logging.INFO)
    # create console handler with a higher log level
    console = logging.StreamHandler()
    console.setLevel(logging.WARN)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        '[%(relativeCreated)6d-%(threadName)10s] %(asctime)s - %(name)s - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S")
    logfile.setFormatter(formatter)
    console.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(logfile)
    logger.addHandler(console)
    return logger


# pylint: disable=R0913
def s3_copy(src, dst, src_bucket, src_key, dst_bucket=None, dst_key=None):
    """Copy from src bucket to dst bucket. For futher details refer to
    http://boto3.readthedocs.io/en/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
    For our use case with many small files default settings is good.
    But soon we need to measure threading impact on our download/upload processes"""
    if not dst_bucket:
        dst_bucket = src_bucket
    if not dst_key:
        dst_key = src_key
    buf = io.BytesIO()
    src.download_fileobj(src_bucket, src_key, buf)
    buf.seek(0)
    dst.upload_fileobj(buf, dst_bucket, dst_key)


def s3_conn(endpoint_url, aws_access_key_id, aws_secret_access_key, region_name, use_ssl=True,
            verify_ssl=True, conf=None, sign_payload=None):
    "Generate session and s3 connection"
    session = boto3.session.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    if not conf:
        conf = botocore.client.Config(s3={'addressing_style': 'path', 'payload_signing_enabled': sign_payload},
                                      signature_version='s3', max_pool_connections=20)
    return session.client('s3', endpoint_url=endpoint_url, region_name=region_name, verify=verify_ssl, use_ssl=use_ssl,
                          config=conf)


class ConsumerThread(threading.Thread):
    """Consumer thread (i.e worker) do all hard work. Boto session object is not thread safe,
    so we need it in every worker.
    Running as a daemon for easy Queue.join() - all manage ops done for us.
    For each key in Queue download file into buffer and upload it to another server (with same name)"""
    daemon = True

    def __init__(self, src_bucket, dst_bucket, src_kw, dst_kw):
        self.src_bucket = src_bucket
        self.dst_bucket = dst_bucket
        self.src = s3_conn(**src_kw)
        # s3_conn(SRC_S3_ENDPOINT, SRC_ACCESS_KEY_ID, SRC_SECRET_KEY, SRC_REGION, SRC_SSL, SRC_SSL_VERIFY)
        self.dst = s3_conn(**dst_kw)
        # s3_conn(DST_S3_ENDPOINT, DST_ACCESS_KEY_ID, DST_SECRET_KEY, DST_REGION, DST_SSL, DST_SSL_VERIFY)
        super(ConsumerThread, self).__init__()

    def run(self):
        while True:
            key = queue.get()
            try:
                s3_copy(self.src, self.dst, self.src_bucket, key, self.dst_bucket)
                logger.info(u"Moved %s:%s to \"%s:%s\"", self.src_bucket, key, self.dst_bucket, key)
            except Exception:
                logger.exception("Exception in thread with key: %s", key)
            finally:
                queue.task_done()


def main():
    """In main we populate Queue with keys from s3 src and run worker threads.
    Main concern in this approach - not more than 2k keys"""
    import argparse

    def is_valid_file(parser, arg):
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!" % arg)
        else:
            return arg

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--marker", help="marker start position of paged s3 iterator", default=None)
    parser.add_argument("-c", "--config", help="config file for use", default=DEFAULT_CONF,
                        type=lambda x: is_valid_file(parser, x))
    parser.add_argument("-t", "--threads", help="number of worker threads", type=int, default=None)
    parser.add_argument("-r", "--restore", help="restore marker from config file", action='store_true')
    parser.add_argument("-i", "--max-items", dest="max_items", help="max items to process", type=int, default=0)
    parser.add_argument("-s", "--safe-mode", dest="safe_mode", help="save marker position with each iteration",
                        action='store_true')
    parser.add_argument("--debug", dest="debug", help="enable debug output", action='store_true')
    parser.add_argument("prefix", nargs="?", type=str, default=None, )

    args = parser.parse_args()

    worker_threads = prefix = marker = safe_mode = None
    config = parse_conf(args.config)
    logger = setup_logging(config.get(GLOBAL_SECTION, "logfile", __file__))

    SIGN_PAYLOAD = config.getboolean(GLOBAL_SECTION, 'sign_payload')

    DST_S3_ENDPOINT = config.get(DST_SECTION, "endpoint_url")
    DST_ACCESS_KEY_ID = config.get(DST_SECTION, "access_key_id")
    DST_SECRET_KEY = config.get(DST_SECTION, "access_key_secret")
    DST_REGION = config.get(DST_SECTION, "region", "default")
    DST_SSL = config.getboolean(DST_SECTION, "use_ssl", True)
    DST_SSL_VERIFY = config.getboolean(DST_SECTION, "verify_ssl", True)

    SRC_S3_ENDPOINT = config.get(SRC_SECTION, "endpoint_url")
    SRC_ACCESS_KEY_ID = config.get(SRC_SECTION, "access_key_id")
    SRC_SECRET_KEY = config.get(SRC_SECTION, "access_key_secret")
    SRC_REGION = config.get(SRC_SECTION, "region", "default")
    SRC_SSL = config.getboolean(SRC_SECTION, "use_ssl", True)
    SRC_SSL_VERIFY = config.getboolean(SRC_SECTION, "verify_ssl", True)

    worker_threads = config.getint(GLOBAL_SECTION, "threads", 20)
    page_size = config.getint(GLOBAL_SECTION, "page_size", 1000)
    max_items = None

    if args.threads:
        worker_threads = int(args.threads)
    elif not worker_threads:
        worker_threads = 20  # if not set in config and via command line, set it to default
    if args.marker:
        marker = args.marker
    if args.restore:
        marker = config.get_marker()
    if args.max_items:
        max_items = args.max_items
    if args.debug:
        # set debug for all handlers
        global DEBUG
        DEBUG = True
        [handler.setLevel(logging.DEBUG) for handler in logger.handlers]
    safe_mode = args.safe_mode
    prefix = args.prefix

    # pylint: disable=unused-argument
    def exit_gracefully(*args):
        "Called if terminated with sigint or sigterm"
        if marker:
            config.set_marker(marker)

    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    bucket_to_copy = src_bucket = config.get(SRC_SECTION, "bucket", None)
    bucket_dst = dst_bucket = config.get(DST_SECTION, "bucket")

    # Validate values
    not_none = ('SRC_ACCESS_KEY_ID', 'SRC_S3_ENDPOINT', 'SRC_SECRET_KEY', 'DST_SECRET_KEY',
                'DST_ACCESS_KEY_ID', 'DST_S3_ENDPOINT', 'page_size', 'worker_threads')
    for valname in not_none:
        val = globals().get(valname, None)
        if not val:
            val = locals().get(valname, None)
        if not val:
            parser.error("Value %s is not defined" % valname)

    src = s3_conn(SRC_S3_ENDPOINT, SRC_ACCESS_KEY_ID, SRC_SECRET_KEY, SRC_REGION, SRC_SSL, SRC_SSL_VERIFY)
    dst = s3_conn(DST_S3_ENDPOINT, DST_ACCESS_KEY_ID, DST_SECRET_KEY, DST_REGION, DST_SSL, DST_SSL_VERIFY)

    # look somewhere in boto3 bugtracker. Basically, it's for working with custom s3, and probably not needed
    # anymore. In worker thread it already eliminated
    src.meta.events.unregister('before-sign.s3', fix_s3_host)
    dst.meta.events.unregister('before-sign.s3', fix_s3_host)

    logger.debug("Before start. Safe mode: %s, PageSize: %d, MaxItems: %s, StartingToken(Marker): %s, src: %s, dst: %s",
                 safe_mode, page_size, max_items or "all", marker or "", SRC_S3_ENDPOINT + ":" + bucket_to_copy,
                 DST_S3_ENDPOINT + ":" + bucket_dst
                 )

    src_kw = dict(endpoint_url=SRC_S3_ENDPOINT, aws_access_key_id=SRC_ACCESS_KEY_ID,
                  aws_secret_access_key=SRC_SECRET_KEY, region_name=SRC_REGION, use_ssl=SRC_SSL,
                  verify_ssl=SRC_SSL_VERIFY, sign_payload=SIGN_PAYLOAD)
    dst_kw = dict(endpoint_url=DST_S3_ENDPOINT, aws_access_key_id=DST_ACCESS_KEY_ID,
                  aws_secret_access_key=DST_SECRET_KEY, region_name=DST_REGION, use_ssl=DST_SSL,
                  verify_ssl=DST_SSL_VERIFY, sign_payload=SIGN_PAYLOAD)
    for _ in range(worker_threads):
        ConsumerThread(src_bucket=src_bucket, dst_bucket=dst_bucket, src_kw=src_kw, dst_kw=dst_kw).start()

    pager = src.get_paginator('list_objects')
    page_num = 0
    page_iter = pager.paginate(Bucket=bucket_to_copy, PaginationConfig={
        'PageSize': page_size,
        'Prefix': prefix,
        'MaxItems': max_items,
        'StartingToken': marker})
    items_total = 0
    try:
        for page in page_iter:
            while queue.qsize() >= 2000:
                logger.warn('Queue has more that 2000 objects. sleeping...')
                time.sleep(5)
            keys = page.get("Contents", [{}])
            for key in keys:
                dst_obj = key.get("Key")
                queue.put(dst_obj)
                items_total += 1

            page_num += 1
            logger.info("Processing %d page (total items: %d)", page_num, items_total)
            marker = page.get("Marker")
            if safe_mode:
                config.set_marker(marker)
        logger.debug("Finished iterator consuming")
        logger.info("Finished consuming, waiting for pending jobs")

        queue.join()
        logger.info("Done! Last marker: %s", marker)
    except KeyboardInterrupt:
        logger.warn("Interrupted on page marker: %s", marker)
        # store marker in marker_file
        config.set_marker(marker)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error("Exception in main: %s", e, exc_info=DEBUG)
