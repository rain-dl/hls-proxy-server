#-------------------------------------------------------------------------------
# Name:        hls-proxy-server
# Purpose:
#
# Author:      RAiN
#
# Created:     05-03-2020
# Copyright:   (c) RAiN 2020
# Licence:     GPL
#-------------------------------------------------------------------------------

import sys
from http.server import SimpleHTTPRequestHandler
from http.server import HTTPServer
from socketserver import ThreadingMixIn
import functools
import subprocess
from threading import Timer
import json
import argparse
import os
import logging
import time

logger = logging.getLogger("HLS Downloader")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

class HlsProxyProcess:
    def __init__(self, process_map, path, url, m3u8dir, m3u8file, cleanup_time):
        self.process_map = process_map
        self.path = path
        self.url = url
        self.m3u8dir = m3u8dir
        self.m3u8file = m3u8file
        self.cleanup_time = cleanup_time
        script = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'hls-downloader.py')
        cmd = ['python3', script, '-d', self.m3u8dir, '-m', self.m3u8file, '-s', '6', '-r', '3', self.url]
        self.process = subprocess.Popen(cmd, shell=False)
        logger.info('Launched hls-downloader to proxy %s.' % (self.url))
        self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
        self.cleanup_timer.start()

    def cleanup(self):
        self.process.terminate()
        if self.path in self.process_map.keys():
            self.process_map.pop(self.path)
        logger.info('hls-downloader for %s terminated.' % (self.url))

    def reset_cleanup_timer(self):
        self.cleanup_timer.cancel()
        self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
        self.cleanup_timer.start()

class HLSProxyHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, process_map=None, hls_proxy_config=None, **kwargs):
        self.process_map = process_map
        self.hls_proxy_config = hls_proxy_config
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            content = json.dumps(list(map(lambda p: {'path': p.path, 'url': p.url}, self.process_map.values())))
            self.wfile.write(bytes(content, 'UTF-8'))
            return

        if self.path in self.hls_proxy_config['hls_proxies']:
            hls_proxy = self.hls_proxy_config['hls_proxies'][self.path]
            if self.path not in self.process_map.keys():
                m3u8dir = os.path.join(self.directory, os.path.dirname(self.path)[1:])
                m3u8file = os.path.basename(self.path)
                self.process_map[self.path] = HlsProxyProcess(self.process_map, self.path, hls_proxy['url'], m3u8dir, m3u8file, hls_proxy['cleanup'])
                logger.info("Hls proxy for path %s launched" % (str(self.path)))

                time.sleep(1)
                m3u8fullname = os.path.join(m3u8dir, m3u8file)
                retry = 10
                while retry > 0 and not os.path.exists(m3u8fullname):
                    retry -= 1
                    time.sleep(0.5)
            else:
                self.process_map[self.path].reset_cleanup_timer()
                logger.info("Cleanup time for hls proxy %s reseted." % (str(self.path)))
        super().do_GET()

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawl a HLS Playlist')
    parser.add_argument('-p', '--port', type=int, required=True, help='Binding port of HTTP server.')
    parser.add_argument('-d', '--directory', type=str, required=True, help='HTTP server base directory.')
    parser.add_argument('-c', '--conf', type=str, required=True, help='HLS proxy path mapping config.')
    args = parser.parse_args()

    try:
        with open(args.conf, 'r') as fp:
            hls_proxy_config = json.load(fp)
    except Exception as ex:
        logger.error("Failed to load hls proxy path mapping config file. Error: %s" %(str(ex)))
        exit(2)

    process_map = {}

    HandlerClass = functools.partial(HLSProxyHTTPRequestHandler, directory=args.directory, process_map=process_map, hls_proxy_config=hls_proxy_config)
    ServerClass  = ThreadingHTTPServer
    #Protocol     = "HTTP/1.0"

    server_address = ('0.0.0.0', args.port)

    #HandlerClass.protocol_version = Protocol
    httpd = ServerClass(server_address, HandlerClass)

    sa = httpd.socket.getsockname()
    logger.info("Serving HTTP on %s:%s..." % (sa[0], sa[1]))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
    logger.info("Server stopped.")
