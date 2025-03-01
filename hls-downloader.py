#!/usr/bin/python

import logging
import logging.handlers
import argparse
import m3u8
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from m3u8_generator import PlaylistGenerator
import os
import pycurl
from io import BytesIO
import certifi
import gevent
import signal
from datetime import datetime

# logger用于配置和发送日志消息。可以通过logging.getLogger(name)获取logger对象，如果不指定name则返回root对象
logger = logging.getLogger('HLS Downloader')
logger.setLevel(logging.DEBUG)

playlist_download_timeout = 6
playlist_max_retry = 10  # 网络请求重试次数上限
playlist_retry_delay = 1   # 每一次请求失败后需等待多少秒后再进行重试

thread_pool_size = 8   # ts视频文件块多协程下载时使用的协程池大小
segment_max_retry = 10
segment_retry_delay = 1     #单个ts视频文件下载超时后，需要再等待多少秒再进行重试

retry_by_resume = True

def request_url(url, timeout = 30, retry = 3, retry_delay = 1, header = None, cookie = None):
    global retry_by_resume

    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.WRITEDATA, buffer)
    c.setopt(pycurl.CAINFO, certifi.where())
    c.setopt(pycurl.FOLLOWLOCATION, True)
    c.setopt(pycurl.TIMEOUT_MS, int(timeout * 1000) if retry_by_resume else int(timeout * 2000))
    c.setopt(pycurl.FAILONERROR, True)
    if header is not None:
        c.setopt(pycurl.HTTPHEADER, header)
    if header is None or len([x for x in header if str.lower(x).startswith('user-agent')]) == 0:
        c.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0')
    if cookie is not None:
        c.setopt(pycurl.COOKIE, cookie)

    effective_url = None
    done = False
    for i in range(retry):
        try:
            if retry_by_resume:
                c.setopt(pycurl.RESUME_FROM, buffer.tell())
            else:
                buffer.truncate(0)
            c.perform()
            effective_url = c.getinfo(pycurl.EFFECTIVE_URL)
            c.close()
            done = True
            break
        except pycurl.error as e:
            if e.args[0] == pycurl.E_OPERATION_TIMEDOUT:
                if retry_by_resume:
                    logger.warning('Download %s timeout, retry and resume from %d' % (url, buffer.tell()))
                else:
                    logger.warning('Download %s timeout, retrying' % (url))
            elif e.args[0] == pycurl.E_RANGE_ERROR:
                logger.warning('Server doesn''t support byte ranges, retry with full download.')
                retry_by_resume = False
            else:
                logger.warning('Download %s failed, error: %s, retrying' % (url, str(e)))
                gevent.sleep(retry_delay)

    if done:
        return buffer.getvalue(), effective_url
    raise RuntimeError('Download %s failed.' % (url))

def urljoin(base, url):
    bscheme, bnetloc, bpath, bparams, bquery, bfragment = \
            urlparse(base, '', True)
    scheme, netloc, path, params, query, fragment = \
            urlparse(url, bscheme, True)
    if scheme != bscheme or netloc:
        return url

    base_parts = bpath.split('/')
    if base_parts[-1] != '':
        del base_parts[-1]

    if path[:1] == '/':
        segments = path.split('/')
    else:
        segments = base_parts + path.split('/')

    resolved_path = []

    for seg in segments:
        if seg == '..':
            try:
                resolved_path.pop()
            except IndexError:
                pass
        elif seg == '.':
            continue
        else:
            resolved_path.append(seg)

    if segments[-1] in ('.', '..'):
        resolved_path.append('')

    return urlunparse((bscheme, bnetloc, '/'.join(resolved_path) or '/', params, query, fragment))

playlist_retry_count = 0     # 网络请求重试计数器

out_f_lock = Lock()     # 获取文件锁
fetching_segments = []
fetched_segments = dict()   # fetched_data[seq]对应着索引为seq的ts文件的文件内容
segment_durations = dict()
last_segment_sequence = -1

def update_playlist():
    global fetching_segments
    global fetched_segments
    global segment_durations
    global output_playlist
    global output_playlist_entries

    out_f_lock.acquire()    # 因为有可能要进行文件写操作，因此要先上锁
    while len(fetching_segments) > 0:
        seq = fetching_segments[0]
        if seq in fetched_segments:
            if fetched_segments[seq]:
                logger.debug('Writing %d to %s' % (seq, out_m3u8))
                filename = '%012d' % (seq) + '.ts'
                duration = segment_durations[seq]
                output_playlist_entries.append({ 'name' : filename, 'duration' : duration })
                if not output_playlist.end_playlist:
                    while len(output_playlist_entries) > out_m3u8_size:
                        output_playlist_files_obsoleted.append(output_playlist_entries[0]['name'])
                        del output_playlist_entries[0]
                    while len(output_playlist_files_obsoleted) > 0:
                        try:
                            os.remove(os.path.join(args.directory, output_playlist_files_obsoleted[0]))
                        except Exception as e:
                            logger.error(str(e))
                        del output_playlist_files_obsoleted[0]
                    output_playlist.sequence = max(seq - out_m3u8_size, 0)
                op = output_playlist.generate()
                m3u8_f = open(out_m3u8, 'w')
                m3u8_f.write(op)
                m3u8_f.close()
            else:
                logger.debug('Skip writing %d to %s' % (seq, out_m3u8));
            fetching_segments.remove(seq)
            del fetched_segments[seq]
            del segment_durations[seq]
        else:
            break
    out_f_lock.release()

def decode_and_write(content, seq, duration):
    global fetched_segments

    filename = os.path.join(args.directory, '%012d'%(seq) + '.ts')
    logger.debug('Write to %s' % (filename))
    video_f = open(filename, 'wb')
    video_f.write(content)
    video_f.close()

    out_f_lock.acquire()
    fetched_segments[seq] = content    # 更新已下载ts文件的索引信息
    out_f_lock.release()

    update_playlist()

# 下载一个ts文件，seq为其下标索引
def get_one(seq, url, duration):
    global fetched_segments

    logger.debug('Processing segment #%d, url: %s' % (seq, url))

    while True:
        try:
            content, _ = request_url(url, duration, segment_max_retry)
            decode_and_write(content, seq, duration)
            break
        except Exception as e:
            logger.error(str(e))
            logger.error('Failed to download segment %d.' % (seq))

            out_f_lock.acquire()
            fetched_segments[seq] = None
            out_f_lock.release()
            update_playlist()
            break

def get_stream_uri(playlist_url, header, cookie):
    content, effective_url = request_url(playlist_url, header=header, cookie=cookie)

    if effective_url is not None:
        playlist_url = effective_url

    #--------------------------根据分辨率或者比特率，解析二级m3u8（如果有的话）---------------------------
    content = content.decode('utf8')
    variant_m3u8 = m3u8.loads(content)
    streams_uri = dict()

    # 对于每一个ts视频列表，转化为分辨率-->uri或者比特率-->uri的映射关系，并保存在streams_uri中
    for playlist in variant_m3u8.playlists:
        if playlist.stream_info.resolution :
            resolution = int(playlist.stream_info.resolution[1])
            logger.info('Stream at %dp detected!' % resolution)
        else:
            resolution = int(playlist.stream_info.bandwidth)
            logger.info('Stream with bandwidth %d detected!' % resolution)
        streams_uri[resolution] = urljoin(playlist_url, playlist.uri)
    #------------------------------------------------------------------------------------------------------

    #-----------------------------选取最终要下载的ts视频列表对应的m3u8链接地址-----------------------------
    auto_highest = True # 是否自动选取最高画质的那个ts视频列表
    stream_res = 0

    if auto_highest and len(variant_m3u8.playlists) > 0:    # 如果存在二级m3u8索引
        stream_res = max(streams_uri)
        logger.info('Stream picked: %dp' % stream_res)
        stream_uri = streams_uri[stream_res]
    else: # 如果没有二级m3u8索引，那么就选一级m3u8索引即可（说明没有可选的其他画质）
        stream_uri = playlist_url
    logger.info('Chunk list: %s' % (stream_uri))
    #------------------------------------------------------------------------------------------------------
    return stream_uri

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawl a HLS Playlist')
    parser.add_argument('url', type=str, help='Playlist URL')   # 必须参数，指定m3u8文件的下载链接
    parser.add_argument('-d', '--directory', type=str, help='Output directory for m3u8 and ts file')
    parser.add_argument('-m', '--m3u8', type=str, help='Output m3u8 File')   # 输出的m3u8文件
    parser.add_argument('-s', '--m3u8size', type=int, help='Output m3u8 list size')   # 输出的m3u8文件的条目数量
    parser.add_argument('-r', '--retry', type=int, default=segment_max_retry, help='Retry count')   # 下载ts片段的最大重试次数
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')
    parser.add_argument('--log', type=str, default=None, help='Log file path name.')
    parser.add_argument('--auto_refresh', type=int, default=0, help='Interval for refreshing the original url.')
    parser.add_argument('--header', action='append', type=str, help='HTTP header')
    parser.add_argument('--cookie', default=None, type=str, help='HTTP Cookie header')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.log is not None:
        log_handler = logging.handlers.TimedRotatingFileHandler(filename=args.log, when='D', backupCount=10, encoding='utf-8')
    else:
        log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"))
    logger.addHandler(log_handler)

    playlist_url = args.url
    logger.info('Playlist URL: ' + playlist_url)

    segment_max_retry = args.retry

    thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
    output_playlist_entries = []
    output_playlist = PlaylistGenerator(playlist_entries=output_playlist_entries, version=3)   # 输出Playlist
    output_playlist_files_obsoleted = []

    os.makedirs(args.directory, exist_ok=True)

    out_m3u8 = args.m3u8
    if out_m3u8 == None or len(out_m3u8) == 0:
        out_m3u8 = 'index.m3u8'
    out_m3u8 = os.path.join(args.directory, out_m3u8)
    out_m3u8_size = args.m3u8size
    if out_m3u8_size == None or out_m3u8_size <= 0:
        out_m3u8_size = 6
    if os.path.exists(out_m3u8):
        try:
            chunklist = m3u8.load(out_m3u8)
            for segment in chunklist.segments:
                #output_playlist_entries.append({ 'name' : segment.uri, 'duration' : segment.duration})
                try:
                    os.remove(os.path.join(args.directory, segment.uri))
                except Exception as e:
                    pass
        except:
            pass

    try:
        stream_uri = get_stream_uri(playlist_url, args.header, args.cookie)
    except Exception as e:
        logger.error(str(e))
        logger.error('Failed to access main playlist.')
        exit(-1)

    last_playlist_url_refresh_time = datetime.now()

    quit = False
    def shutdown():
        global quit
        if not quit:
            logger.info('Shutting down...')
            quit = True

    gevent.signal_handler(signal.SIGTERM, shutdown)
    gevent.signal_handler(signal.SIGINT, shutdown)

    while not quit:
        try:
            content, _ = request_url(stream_uri, playlist_download_timeout, playlist_max_retry, header=args.header, cookie=args.cookie)
        except Exception as e:
            logger.error('Failed to download chunk list.')
            continue

        content = content.decode('utf8')
        chunklist = m3u8.loads(content)

        if chunklist.media_sequence == None:
            logger.warning('Incorrect chunk list')
            gevent.sleep(playlist_retry_delay)
            continue

        target_dur = chunklist.target_duration

        list_end = chunklist.is_endlist # 对于直播等场景，还会有下一个m3u8文件
        output_playlist.end_playlist = list_end
        output_playlist.duration = chunklist.target_duration

        seq = chunklist.media_sequence
        out_f_lock.acquire()
        for segment in chunklist.segments:
            if seq > last_segment_sequence:
                url = urljoin(stream_uri, segment.uri)
                fetching_segments.append(seq)
                segment_durations[seq] = segment.duration
                logger.debug('Segment %d, %s' % (seq, url))
                last_segment_sequence = seq
                thread_pool.submit(get_one, seq, url, segment.duration)    # 启动协程池进行下载
            seq += 1
        out_f_lock.release()

        sleep_dur = target_dur / 2 # 休眠一段时间再获取下一个m3u8文件

        if list_end or quit:
            break

        if args.auto_refresh > 0 and stream_uri != playlist_url and (datetime.now() - last_playlist_url_refresh_time).total_seconds() > args.auto_refresh:
            try:
                stream_uri = get_stream_uri(playlist_url, args.header, args.cookie)
                last_playlist_url_refresh_time = datetime.now()
                logger.info('Refreshed playlist URL: ' + stream_uri)
            except Exception as e:
                logger.error(str(e))
                logger.error('Failed to refresh playlist url.')

        logger.debug('Sleep for %d secs before reloading' % (sleep_dur))
        gevent.sleep(sleep_dur)

    thread_pool.shutdown()
    logger.info('Ended.')
