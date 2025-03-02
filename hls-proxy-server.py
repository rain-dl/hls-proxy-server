#-------------------------------------------------------------------------------
# Name:        hls-proxy-server
# Purpose:
#
# Author:      RAiN
#
# Created:     05-03-2020
# Copyright:   (c) RAiN 2023
# Licence:     GPL
#-------------------------------------------------------------------------------

from http.server import SimpleHTTPRequestHandler
from http.server import HTTPServer
from socketserver import ThreadingMixIn
import functools
import subprocess
from threading import Timer
import json
import argparse
import os
import shutil
import logging
import logging.handlers
import time
import hashlib
import pycurl
from io import BytesIO
import certifi
import gevent
import re
import importlib
hls_downloader = importlib.import_module('hls-downloader')

logger = logging.getLogger("HLS Downloader")
logger.setLevel(logging.INFO)

class HlsProxyProcess:
    def __init__(self, process_map, path, url, m3u8dir, m3u8file, cleanup_time, verbose, log_file):
        self.process_map = process_map
        self.path = path
        self.url = url
        self.m3u8dir = m3u8dir
        self.m3u8file = m3u8file
        self.cleanup_time = cleanup_time
        script = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'hls-downloader.py')
        cmd = ['python', script, '-d', self.m3u8dir, '-m', self.m3u8file, '-s', '6', '-r', '10', '--auto_refresh', '3600', self.url]
        if verbose:
            cmd.append('-v')
        if log_file is not None:
            cmd.append('--log')
            cmd.append(log_file)
        self.process = subprocess.Popen(cmd, shell=False)
        logger.info('Launched hls-downloader to proxy %s.' % (self.url))
        self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
        self.cleanup_timer.start()

    def cleanup(self):
        logger.info('Terminating hls-downloader for %s...' % (self.url))
        if self.path in self.process_map.keys():
            self.process_map.pop(self.path)
        self.process.terminate()
        self.process.wait()
        shutil.rmtree(self.m3u8dir)
        logger.info('hls-downloader for %s terminated.' % (self.url))

    def reset_cleanup_timer(self):
        self.cleanup_timer.cancel()
        self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
        self.cleanup_timer.start()

def request_url(url, timeout = 5, retry = 3, retry_delay = 1, header = None, cookie = None):
    retry_by_resume = True

    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.WRITEDATA, buffer)
    c.setopt(pycurl.CAINFO, certifi.where())
    c.setopt(pycurl.FOLLOWLOCATION, False)
    c.setopt(pycurl.FAILONERROR, True)
    if header is not None:
        c.setopt(pycurl.HTTPHEADER, header)
    if header is None or len([x for x in header if str.lower(x).startswith('user-agent')]) == 0:
        c.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0')
    if cookie is not None:
        c.setopt(pycurl.COOKIE, cookie)

    redirect_url = None
    content_type = None
    done = False
    for i in range(retry):
        try:
            if retry_by_resume:
                c.setopt(pycurl.RESUME_FROM, buffer.tell())
            else:
                buffer.truncate(0)
            c.setopt(pycurl.TIMEOUT_MS, int(timeout * 1000) if retry_by_resume else int(timeout * 2000))
            c.perform()
            response_code = c.getinfo(pycurl.RESPONSE_CODE)
            if response_code == 301 or response_code == 302:
                redirect_url = c.getinfo(pycurl.REDIRECT_URL)
            else:
                content_type = c.getinfo(pycurl.CONTENT_TYPE)
            c.close()
            done = True
            break
        except pycurl.error as e:
            if e.args[0] == pycurl.E_OPERATION_TIMEDOUT:
                if retry_by_resume:
                    logger.warning('Download %s timeout, retry and resume from %d' % (url, buffer.tell()))
                else:
                    logger.warning('Download %s timeout, retrying' % (url))
            elif e.args[0] == pycurl.E_HTTP_NOT_FOUND:
                c.close()
                return 404, 'text/html; charset=utf-8', "Not found.", None
            elif e.args[0] == pycurl.E_RANGE_ERROR:
                logger.warning('Server doesn''t support byte ranges, retry with full download.')
                retry_by_resume = False
            else:
                logger.warning('Download %s failed, error: %s, retrying' % (url, str(e)))
                gevent.sleep(retry_delay)

    if done:
        return response_code, redirect_url, content_type, buffer.getvalue()
    else:
        return 504, None, 'text/html; charset=utf-8', "Gateway time-out.", None

class HLSProxyHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, process_map=None, cleanup_default=120,
                 base_uri="http://127.0.0.1:8090", verbose=False, hls_log=None, **kwargs):
        self.process_map = process_map
        self.base_uri = base_uri
        self.verbose = verbose
        self.hls_log = hls_log
        self.cleanup_default = cleanup_default
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            content = json.dumps(list(map(lambda p: {'path': p.path, 'url': p.url}, self.process_map.values())))
            self.wfile.write(bytes(content, 'UTF-8'))
            return

        if self.path.startswith('/proxy/'):
            url, base_url, file_name = self.get_proxy_url(self.path)

            stream_uri = hls_downloader.get_stream_uri(url, None, None)
            if stream_uri != url:
                self.send_response(301)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Access-Control-Allow-Methods', 'GET')
                self.send_header('Access-Control-Allow-Headers', 'Content-Type, Content-Length, Authorization')
                redirect_url = re.sub(r'(https?://)', self.base_uri + r'/proxy/\1', stream_uri)
                self.send_header('Location', redirect_url)

            hash = self.get_url_hash(base_url)
            if hash not in self.process_map.keys():
                m3u8dir = os.path.join(self.directory, hash)
                m3u8file = file_name
                url, cleanup_time = self.extract_cleanup_time(url)
                self.process_map[hash] = HlsProxyProcess(self.process_map, hash, url, m3u8dir, m3u8file, cleanup_time, self.verbose, self.hls_log)
                logger.info("Hls proxy for path %s launched" % (url))

                launch_time = time.time()
                time.sleep(1)
                m3u8fullname = os.path.join(m3u8dir, m3u8file)
                retry = 20
                while retry > 0 and (not os.path.exists(m3u8fullname) or os.path.getmtime(m3u8fullname) < launch_time):
                    retry -= 1
                    time.sleep(0.5)
            else:
                self.process_map[hash].reset_cleanup_timer()
                logger.debug("Cleanup time for hls proxy %s reseted." % (str(self.path)))

        if self.path.startswith('/fetch/'):
            url = self.path[7:]
            response_code, redirect_url, content_type, content = request_url(url)
            self.send_response(response_code)
            if response_code == 301 or response_code == 302:
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Access-Control-Allow-Methods', 'GET')
                self.send_header('Access-Control-Allow-Headers', 'Content-Type, Content-Length, Authorization')
                redirect_url = re.sub(r'(https?://)', self.base_uri + r'/fetch/\1', redirect_url)
                self.send_header('Location', redirect_url)
            if content_type is not None:
                self.send_header('Content-type', content_type)
            self.end_headers()

            try:
                content = content.decode('utf8')
                content = re.sub(r'(https?://)', self.base_uri + r'/fetch/\1', content)
                self.wfile.write(bytes(content, 'UTF-8'))
            except:
                self.wfile.write(content)

            return

        try:
            super().do_GET()
        except:
            pass

    def translate_path(self, path):
        if path.startswith('/proxy/'):
            _, base_url, file_name = self.get_proxy_url(path)
            hash = self.get_url_hash(base_url)
            return os.path.join(os.path.join(self.directory, hash), file_name)
        return super().translate_path(path)

    def get_proxy_url(self, path):
        url = path[7:]

        # abandon query parameters
        base_url = url.split('?',1)[0]
        base_url = base_url.split('#',1)[0]
        base_url, file_name = base_url.rsplit('/', 1)
        return url, base_url, file_name

    def get_url_hash(self, url):
        md5 = hashlib.md5()
        md5.update(url.encode('utf-8'))
        return md5.hexdigest()

    def extract_cleanup_time(self, url):
        ss = url.split('?',1)
        if len(ss) == 1:
            return url, self.cleanup_default
        url = ss[0]
        params = ss[1].split('&')
        cleanup_time = self.cleanup_default
        param_str = ''
        for param in params:
            p = param.split('=')
            if p[0] == 'cleanup':
                try:
                    cleanup_time = int(p[1])
                except ValueError:
                    cleanup_time = self.cleanup_default
            else:
                if len(param_str) > 0:
                    param_str += '&'
                param_str += param
        if len(param_str) > 0:
            url = url + '?' + param_str
        return url, cleanup_time

    def log_request(self, code='-', size='-'):
        if self.verbose:
            SimpleHTTPRequestHandler.log_request(self, code, size)

    def log_message(self, format, *args):
        logger.debug(format % args)

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawl a HLS Playlist')
    parser.add_argument('-u', '--base_uri', type=str, default="http://127.0.0.1:8090", help='Host name of HTTP server.')
    parser.add_argument('-b', '--binding', type=str, default="127.0.0.1", help='Binding address of HTTP server.')
    parser.add_argument('-p', '--port', type=int, required=True, help='Binding port of HTTP server.')
    parser.add_argument('-d', '--directory', type=str, required=True, help='HTTP server base directory.')
    parser.add_argument('-e', '--cleanup', type=int, default=120, help='The default cleanup time.')
    parser.add_argument('--cert', type=str, default=None, help='Https cert file.')
    parser.add_argument('--key', type=str, default=None, help='Https key file.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode.')
    parser.add_argument('--log', type=str, default=None, help='Log file path name.')
    parser.add_argument('--hls_log', type=str, default=None, help='Hls downloader Log file path name.')
    args = parser.parse_args()

    # if not args.base_uri.endswith(f':{args.port}'):
    #     args.base_uri += f':{args.port}'

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.log is not None:
        log_handler = logging.handlers.TimedRotatingFileHandler(filename=args.log, when='D', backupCount=10, encoding='utf-8')
    else:
        log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"))
    logger.addHandler(log_handler)

    process_map = {}

    HandlerClass = functools.partial(HLSProxyHTTPRequestHandler, directory=args.directory, process_map=process_map,
                                     cleanup_default=args.cleanup, base_uri=args.base_uri,
                                     verbose=args.verbose, hls_log=args.hls_log)
    ServerClass  = ThreadingHTTPServer
    #Protocol     = "HTTP/1.0"

    server_address = (args.binding, args.port)

    #HandlerClass.protocol_version = Protocol
    httpd = ServerClass(server_address, HandlerClass)

    server_type = "HTTP"
    if args.cert is not None and args.key is not None:
        import ssl
        httpd.socket = ssl.wrap_socket(httpd.socket, certfile=args.cert, keyfile=args.key, server_side=True)
        server_type = "HTTPS"

    sa = httpd.socket.getsockname()
    logger.info("Serving %s on %s:%s..." % (server_type, sa[0], sa[1]))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
    logger.info("Server stopped.")
