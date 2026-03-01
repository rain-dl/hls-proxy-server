#-------------------------------------------------------------------------------
# Name:        hls-proxy-server
# Purpose:
#
# Author:      RAiN
#
# Created:     05-03-2020
# Copyright:   (c) RAiN 2026
# Licence:     GPL
#-------------------------------------------------------------------------------

from http.server import SimpleHTTPRequestHandler
from http.server import HTTPServer
from socketserver import ThreadingMixIn
import functools
import subprocess
from threading import Timer, Lock
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
        cmd = ['python', script, '-d', self.m3u8dir, '-m', self.m3u8file, '-s', '6', '-t', '10', '--auto_refresh', '3600', self.url]
        if verbose:
            cmd.append('-v')
        if log_file is not None:
            cmd.append('--log')
            cmd.append(log_file)
        self.process = subprocess.Popen(cmd, shell=False)
        logger.info('Launched hls-downloader to proxy %s.' % (self.url))
        self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
        self.cleanup_timer.start()

        self.last_reset_time = time.time()

    def cleanup(self):
        logger.info('Terminating hls-downloader for %s...' % (self.url))
        if self.path in self.process_map.keys():
            self.process_map.pop(self.path)
        self.process.terminate()
        self.process.wait()
        shutil.rmtree(self.m3u8dir)
        logger.info('hls-downloader for %s terminated.' % (self.url))

    def reset_cleanup_timer(self):
        if self.process is None or not self.process.poll() is not None:
            logger.info('hls-downloader for %s has terminated unexpectedly.' % (self.url))
            if self.path in self.process_map.keys():
                self.process_map.pop(self.path)
        else:
            self.cleanup_timer.cancel()
            self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
            self.cleanup_timer.start()
            self.last_reset_time = time.time()

class HlsTranscodeProcess:
    def __init__(self, process_map, path, url, m3u8dir, m3u8file, cleanup_time, params: dict):
        self.process_map = process_map
        self.path = path
        self.url = url
        self.m3u8dir = m3u8dir
        self.m3u8file = m3u8file
        self.cleanup_time = cleanup_time

        resolution = params.get('s', None)
        preset = params.get('p', 'fast')
        v_bitrate = params.get('vb', '1M')
        v_max_bitrate = params.get('vmax', None)
        a_bitrate = params.get('ab', '128K')

        os.makedirs(self.m3u8dir, exist_ok=True)
        cmd = ['ffmpeg', '-loglevel', 'error', '-i', self.url]
        if resolution is not None:
            cmd.extend(['-s', resolution])
        cmd.extend(['-c:v', 'libx264', '-preset', preset, '-g', '90', '-b:v', v_bitrate])
        if v_max_bitrate is not None:
            cmd.extend(['-maxrate:v', v_max_bitrate, '-minrate:v', '0', '-bufsize:v', v_bitrate])
        cmd.extend(['-c:a', 'aac', '-b:a', a_bitrate])
        cmd.extend(['-f', 'hls', '-hls_time', '3', '-segment_wrap', '10', '-hls_list_size', '6', '-hls_flags', 'delete_segments'])
        cmd.append(os.path.join(self.m3u8dir, self.m3u8file))
        self.process = subprocess.Popen(cmd, shell=False)
        logger.info('Launched ffmpeg to transcode %s.' % (self.url))
        self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
        self.cleanup_timer.start()

        self.last_reset_time = time.time()

    def cleanup(self):
        logger.info('Terminating ffmpeg transcoder for %s...' % (self.url))
        if self.path in self.process_map.keys():
            self.process_map.pop(self.path)
        self.process.send_signal(15)
        self.process.wait()
        shutil.rmtree(self.m3u8dir)
        logger.info('ffmpeg transcoder for %s terminated.' % (self.url))

    def reset_cleanup_timer(self):
        exit_code = self.process.poll()
        if exit_code is not None:
            logger.info('ffmpeg transcoder for %s has terminated unexpectedly.' % (self.url))
            if self.path in self.process_map.keys():
                self.process_map.pop(self.path)
        else:
            self.cleanup_timer.cancel()
            self.cleanup_timer = Timer(self.cleanup_time, self.cleanup)
            self.cleanup_timer.start()
            self.last_reset_time = time.time()

def request_url(url, request_range = None, timeout = 5, retry = 3, retry_delay = 1, header = None, cookie = None):
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
    if request_range:
        start, end = request_range
        if start is not None:
            if end is not None:
                range_header = f"{start}-{end}"
            else:
                range_header = f"{start}-"
        else:
            if end is not None:
                range_header = f"-{end}"
        c.setopt(pycurl.RANGE, range_header)

    response_header = {}
    def header_callback(x):
        kv = x.decode('ascii').split(':')
        if len(kv) < 2:
            return
        response_header[kv[0].strip()] = kv[1].strip()
    c.setopt(pycurl.HEADERFUNCTION, header_callback)

    redirect_url = None
    content_type = None
    content_range = None
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
                content_range = response_header.get('Content-Range')
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
        return response_code, redirect_url, content_type, content_range, buffer.getvalue()
    else:
        return 504, None, 'text/html; charset=utf-8', None, "Gateway time-out."

def parse_range_header(header):
    if not header.startswith('bytes='):
        raise ValueError('Invalid Range header')
    ranges = header[6:].split(',')
    if len(ranges) > 1:
        raise ValueError('Multiple ranges not supported')
    start, end = ranges[0].split('-')
    try:
        if start:
            start = int(start)
        else:
            start = None
        if end:
            end = int(end)
        else:
            end = None
        if start is None and end is None:
            raise ValueError('Invalid Range header')
        return (start, end)
    except ValueError:
        raise ValueError('Invalid Range header')

def copy_byte_range(infile, outfile, start=None, stop=None, bufsize=16*1024):
    """Like shutil.copyfileobj, but only copy a range of the streams.

    Both start and stop are inclusive.
    """
    if start is not None: infile.seek(start)
    while 1:
        to_read = min(bufsize, stop + 1 - infile.tell() if stop else bufsize)
        buf = infile.read(to_read)
        if not buf:
            break
        outfile.write(buf)

class HLSProxyHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, process_map: dict, cleanup_default=120, max_transcoder_instances=3,
                 protocol="HTTP", verbose=False, hls_log=None, lock: Lock = None, **kwargs):
        self.process_map = process_map
        self.protocol = protocol.lower()
        self.verbose = verbose
        self.hls_log = hls_log
        self.cleanup_default = cleanup_default
        self.max_transcoder_instances = max_transcoder_instances
        self.lock = lock if lock is not None else Lock()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if 'Range' in self.headers:
            try:
                request_range = parse_range_header(self.headers['Range'])
            except ValueError:
                self.send_error(400, 'Invalid Range header')
                return
        else:
            request_range = None

        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            content = json.dumps(list(map(lambda p: {'path': p.path, 'url': p.url}, self.process_map.values())))
            self.wfile.write(bytes(content, 'UTF-8'))
            return

        if self.path.startswith('/fetch/'):
            url = self.path[7:]

            response_code, redirect_url, content_type, content_range, content = request_url(url, request_range=request_range)
            self.send_response(response_code)
            if response_code == 301 or response_code == 302:
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Access-Control-Allow-Methods', 'GET')
                self.send_header('Access-Control-Allow-Headers', 'Content-Type, Content-Length, Authorization')
                redirect_url = re.sub(r'(?<!/)(https?://)', self.headers.get("X-Forwarded-Proto", self.protocol) + "://" + self.headers['Host'] + r'/fetch/\1', redirect_url)
                self.send_header('Location', redirect_url)
            if content_type is not None:
                self.send_header('Content-type', content_type)
            if content_range is not None:
                self.send_header('Content-Range', content_range)
                self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()

            try:
                content = content.decode('utf8')
                content = re.sub(r'(https?://)', self.headers.get("X-Forwarded-Proto", self.protocol) + "://" + self.headers['Host'] + r'/fetch/\1', content)
                self.wfile.write(bytes(content, 'UTF-8'))
            except:
                self.wfile.write(content)
            return

        if self.path.startswith('/proxy/'):
            url = self.path[7:]
            hash = self.get_url_hash(url)
            url, params = self.extract_url_and_parameters(url)

            desired_resolution = int(params.get('sr', 0))
            stream_uri = hls_downloader.get_stream_uri(url, None, None, desired_resolution)
            proxy_prefix = self.headers.get("X-Forwarded-Proto", self.protocol) + "://" + self.headers['Host'] + f'/p/{hash}/'

            self.send_response(301)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type, Content-Length, Authorization')
            redirect_url = proxy_prefix + stream_uri
            self.send_header('Location', redirect_url)
            self.end_headers()
            return

        if self.path.startswith('/transcode/'):
            url = self.path[11:]
            hash = self.get_url_hash(url)
            url, params = self.extract_url_and_parameters(url)

            desired_resolution = int(params.get('sr', 0))
            stream_uri = hls_downloader.get_stream_uri(url, None, None, desired_resolution)
            proxy_prefix = self.headers.get("X-Forwarded-Proto", self.protocol) + "://" + self.headers['Host'] + f'/t/{hash}/'

            self.send_response(301)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type, Content-Length, Authorization')
            params_str = self.get_parameters_str(params)
            if len(params_str) > 0:
                redirect_url = proxy_prefix + params_str + '/' + stream_uri
            else:
                redirect_url = proxy_prefix + stream_uri
            self.send_header('Location', redirect_url)
            self.end_headers()
            return

        if self.path.startswith('/t/'):
            hash, url, params, base_url, file_name = self.get_transcode_url_and_parameters(self.path)

            with self.lock:
                if hash not in self.process_map.keys():
                    m3u8dir = os.path.join(self.directory, hash)
                    m3u8file = file_name
                    url, cleanup_time = self.extract_cleanup_time(url)
                    self.process_map[hash] = HlsTranscodeProcess(self.process_map, hash, url, m3u8dir, m3u8file, cleanup_time, params)
                    logger.info("Hls transcoder %s for path %s launched" % (hash, url))
                    self.check_transcoder_instance_count()

                    launch_time = time.time()
                    time.sleep(1)
                    m3u8fullname = os.path.join(m3u8dir, m3u8file)
                    retry = 20
                    while retry > 0 and (not os.path.exists(m3u8fullname) or os.path.getmtime(m3u8fullname) < launch_time):
                        retry -= 1
                        time.sleep(0.5)
                else:
                    self.process_map[hash].reset_cleanup_timer()
                    logger.debug("Cleanup time for hls transcoder %s reseted." % (str(self.path)))

        if self.path.startswith('/p/'):
            hash, url, base_url, file_name = self.get_proxy_url(self.path)

            with self.lock:
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

        try:
            # path = self.translate_path(self.path)
            path = os.path.join(os.path.join(self.directory, hash), file_name)
            f = None
            try:
                f = open(path, 'rb')
            except OSError:
                self.send_error(404, "File not found")
                return None

            try:
                fs = os.fstat(f.fileno())
                file_len = fs[6]

                if request_range:
                    start, end = request_range
                    if start is None:
                        if end > file_len:
                            self.send_error(416, 'Requested range not satisfiable')
                            return None
                        start = file_len - end
                        end = file_len - 1
                    else:
                        if start >= file_len:
                            self.send_error(416, 'Requested range not satisfiable')
                            return None
                        if end is None or end >= file_len:
                            end = file_len - 1
                    content_length = end - start + 1

                    self.send_response(206)
                    self.send_header('Content-Range', 'bytes %d-%d/%d' % (start, end, file_len))
                else:
                    start = None
                    end = None
                    content_length = file_len
                    self.send_response(200)

                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Content-Length', str(content_length))
                self.send_header('Content-type', self.guess_type(path))
                self.end_headers()

                copy_byte_range(f, self.wfile, start, end)
            finally:
                f.close()
        except Exception as e:
            logger.error(repr(e))

    def translate_path(self, path):
        if path.startswith('/p/'):
            hash, _, _, file_name = self.get_proxy_url(path)
            return os.path.join(os.path.join(self.directory, hash), file_name)
        elif path.startswith('/t/'):
            hash, _, _, _, file_name = self.get_transcode_url_and_parameters(path)
            return os.path.join(os.path.join(self.directory, hash), file_name)
        return super().translate_path(path)

    def get_proxy_url(self, path):
        ss = path[3:].split('/', 1)
        url_hash = ss[0]
        url = ss[1]

        # abandon query parameters
        base_url = url.split('?',1)[0]
        base_url = base_url.split('#',1)[0]
        base_url, file_name = base_url.rsplit('/', 1)
        return url_hash, url, base_url, file_name

    def extract_url_and_parameters(self, path: str):
        if path.startswith('http'):
            return path, {}
        ss = path.split('/', 1)
        if len(ss) == 1:
            return path, {}
        path = ss[1]
        parameters = {}
        try:
            pairs = ss[0].split(':')
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    parameters[key] = value
            return path, parameters
        except ValueError:
            return path, {}

    def get_parameters_str(self, parameters: dict):
        return ':'.join([key + '=' + value for key, value in parameters.items()])

    def get_transcode_url_and_parameters(self, path):
        ss = path[3:].split('/', 1)
        url_hash = ss[0]
        url, parameters = self.extract_url_and_parameters(ss[1])

        # abandon query parameters
        base_url = url.split('?',1)[0]
        base_url = base_url.split('#',1)[0]
        base_url, file_name = base_url.rsplit('/', 1)
        return url_hash, url, parameters, base_url, file_name

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

    def check_transcoder_instance_count(self):
        transcoder_hashs = [hash for hash, instance in self.process_map.items() if isinstance(instance, HlsTranscodeProcess)]
        if len(transcoder_hashs) > self.max_transcoder_instances:
            transcoder_hashs = sorted(transcoder_hashs, key=lambda x: self.process_map[x].last_reset_time)
            while len(transcoder_hashs) > self.max_transcoder_instances:
                hash = transcoder_hashs.pop(0)
                self.process_map[hash].cleanup()

    def log_request(self, code='-', size='-'):
        if self.verbose:
            SimpleHTTPRequestHandler.log_request(self, code, size)

    def log_message(self, format, *args):
        logger.debug(format % args)

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawl a HLS Playlist')
    parser.add_argument('-b', '--binding', type=str, default="127.0.0.1", help='Binding address of HTTP server.')
    parser.add_argument('-p', '--port', type=int, required=True, help='Binding port of HTTP server.')
    parser.add_argument('-d', '--directory', type=str, required=True, help='HTTP server base directory.')
    parser.add_argument('-e', '--cleanup', type=int, default=120, help='The default cleanup time.')
    parser.add_argument('-t', '--max_transcoder', type=int, default=3, help='The max transcoder instance number.')
    parser.add_argument('--cert', type=str, default=None, help='Https cert file.')
    parser.add_argument('--key', type=str, default=None, help='Https key file.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode.')
    parser.add_argument('--log', type=str, default=None, help='Log file path name.')
    parser.add_argument('--hls_log', type=str, default=None, help='Hls downloader Log file path name.')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.log is not None:
        log_handler = logging.handlers.TimedRotatingFileHandler(filename=args.log, when='D', backupCount=10, encoding='utf-8')
    else:
        log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"))
    logger.addHandler(log_handler)

    server_type = "HTTP"
    if args.cert is not None and args.key is not None:
        server_type = "HTTPS"

    process_map = {}
    lock = Lock()

    HandlerClass = functools.partial(HLSProxyHTTPRequestHandler, directory=args.directory, process_map=process_map,
                                     cleanup_default=args.cleanup, max_transcoder_instances=args.max_transcoder, protocol=server_type,
                                     verbose=args.verbose, hls_log=args.hls_log, lock=lock)
    ServerClass  = ThreadingHTTPServer
    #Protocol     = "HTTP/1.0"

    server_address = (args.binding, args.port)

    #HandlerClass.protocol_version = Protocol
    httpd = ServerClass(server_address, HandlerClass)

    if server_type == "HTTPS":
        import ssl
        httpd.socket = ssl.wrap_socket(httpd.socket, certfile=args.cert, keyfile=args.key, server_side=True)

    sa = httpd.socket.getsockname()
    logger.info("Serving %s on %s:%s..." % (server_type, sa[0], sa[1]))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
    logger.info("Server stopped.")
