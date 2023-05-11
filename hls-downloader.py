#!/usr/bin/python

import logging
import argparse
import m3u8
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import json
from m3u8_generator import PlaylistGenerator
import os
from time import time, sleep
import requests

try:
    import pycurl
    from io import BytesIO
    import certifi
    use_pycurl = True
except ImportError:
    use_pycurl = False

connect_timeout = 2

playlist_max_retry = 10  # 网络请求重试次数上限
playlist_retry_delay = 1   # 每一次请求失败后需等待多少秒后再进行重试

thread_pool_size = 8   # ts视频文件块多协程下载时使用的协程池大小
segment_download_timeout = 6    # 单个ts视频文件下载的超时时间
segment_max_retry = 10
segment_retry_delay = 1     #单个ts视频文件下载超时后，需要再等待多少秒再进行重试

def request_url(url, connect_timeout = 10, read_timeout = 30):
    if use_pycurl:
        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.CAINFO, certifi.where())
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(pycurl.TIMEOUT_MS, read_timeout * 1000)
        c.perform()
        status_code = c.getinfo(c.RESPONSE_CODE)
        c.close()

        if status_code != 200:
            raise RuntimeError('Failed to get content, status code: %d.' % (status_code))
        return buffer.getvalue()

    resp = requests.get(url, timeout=(connect_timeout, read_timeout))
    if resp.status_code != 200:
        raise RuntimeError('Failed to get content, status code: %d.' % (resp.status_code))
    return resp.content

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
                logger.debug("Writing %d to %s" % (seq, out_m3u8))
                filename = "%012d" % (seq) + ".ts"
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
                m3u8_f = open(out_m3u8, "w")
                m3u8_f.write(op)
                m3u8_f.close()
            else:
                logger.debug("Skip writing %d to %s" % (seq, out_m3u8));
            fetching_segments.remove(seq)
            del fetched_segments[seq]
            del segment_durations[seq]
        else:
            break
    out_f_lock.release()

def decode_and_write(content, seq, duration):
    global fetched_segments

    filename = os.path.join(args.directory, "%012d"%(seq) + ".ts")
    logger.debug("Write to %s" % (filename))
    video_f = open(filename, "wb")
    video_f.write(content)
    video_f.close()

    out_f_lock.acquire()
    fetched_segments[seq] = content    # 更新已下载ts文件的索引信息
    out_f_lock.release()

    update_playlist()

# 下载一个ts文件，seq为其下标索引
def get_one(seq, url, duration):
    global fetched_segments

    logger.debug("Processing Segment #%d, Url: %s" % (seq, url))

    retry_count = 0
    while True:
        try:
            content = request_url(url, connect_timeout, segment_download_timeout)
            decode_and_write(content, seq, duration)
            break
        except Exception as e:
            logger.error(str(e))
            logger.warning("Content Problem, Retrying for %d" % (seq))
            retry_count = retry_count + 1
            if retry_count > segment_max_retry:
                logger.error("Seq %d Failed" % (seq))

                out_f_lock.acquire()
                fetched_segments[seq] = None
                out_f_lock.release()
                update_playlist()
                break
            sleep(segment_retry_delay)

if __name__ == '__main__':
    # logger用于配置和发送日志消息。可以通过logging.getLogger(name)获取logger对象，如果不指定name则返回root对象
    logger = logging.getLogger("HLS Downloader")
    logger.setLevel(logging.INFO)

    # handler用于将日志记录发送到合适的目的地
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # formatter用于指定日志记录输出的具体格式
    formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    ch.setFormatter(formatter)

    logger.addHandler(ch)   # 一个logger对象可以通过addHandler方法添加多个handler，每个handler又可以定义不同日志级别，以实现日志分级过滤显示

    #-----------------------------创建文件Handler并将其绑定到logger-------------------------
    #logf = logging.FileHandler("hls.log")
    #logf.setLevel(logging.DEBUG)
    #logf.setFormatter(formatter)
    #logger.addHandler(logf)
    #---------------------------------------------------------------------------------------

    parser = argparse.ArgumentParser(description='Crawl a HLS Playlist')
    parser.add_argument('url', type=str, help='Playlist URL')   # 必须参数，指定m3u8文件的下载链接
    parser.add_argument('-d', '--directory', type=str, help='Output directory for m3u8 and ts file')
    parser.add_argument('-m', '--m3u8', type=str, help='Output m3u8 File')   # 输出的m3u8文件
    parser.add_argument('-s', '--m3u8size', type=int, help='Output m3u8 list size')   # 输出的m3u8文件的条目数量
    parser.add_argument('-r', '--retry', type=int, default=segment_max_retry, help='Retry count')   # 下载ts片段的最大重试次数
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')
    parser.add_argument('--header', default="", type=str, help='Header (JSON)')     # 网络请求头键值对的JSON对象序列化得到的二进制文件
    parser.add_argument('--cookie', default="", type=str, help='Cookie (JSON)')     # cookie键值对的JSON对象序列化得到的二进制文件
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    playlist_url = args.url
    logger.info("Playlist URL: " + playlist_url)

    segment_max_retry = args.retry

    control = requests.Session()    # m3u8文件下载会话
    thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
    output_playlist_entries = []
    output_playlist = PlaylistGenerator(playlist_entries=output_playlist_entries, version=3)   # 输出Playlist
    output_playlist_files_obsoleted = []

    os.makedirs(args.directory, exist_ok=True)

    out_m3u8 = args.m3u8
    if out_m3u8 == None or len(out_m3u8) == 0:
        out_m3u8 = "index.m3u8"
    out_m3u8 = os.path.join(args.directory, out_m3u8)
    out_m3u8_size = args.m3u8size
    if out_m3u8_size == None or out_m3u8_size <= 0:
        out_m3u8_size = 6
    if os.path.exists(out_m3u8):
        try:
            chunklist = m3u8.load(out_m3u8)
            for segment in chunklist.segments:
                output_playlist_entries.append({ 'name' : segment.uri, 'duration' : segment.duration})
        except:
            pass

    #------------------------读取请求头和Cookie配置文件-----------------------
    cookie_file = args.cookie
    header_file = args.header
    cookie_dict = dict()
    header_dict = dict()
    try:
        if len(cookie_file) > 0:
            with open(cookie_file, "rb") as cookie_f:
                cookie_json = json.load(cookie_f)
                for ele in cookie_json:
                    key = ele["name"]
                    val = ele["value"]
                    cookie_dict[key] = val

        if len(header_file) > 0:
            with open(header_file, "rb") as header_f:
                header_json = json.load(header_f)
                for ele in header_json:
                    header_dict.update(ele)
    except IOError as e:
        print(e)
    logger.debug(cookie_dict)
    logger.debug(header_dict)
    #------------------------------------------------------------------------

    mpl_res = control.get(playlist_url, cookies=cookie_dict, headers=header_dict)
    content = mpl_res.content
    playlist_url = mpl_res.url
    logger.info("Main Playlist %s ST %d" % (playlist_url, mpl_res.status_code))

    #--------------------------根据分辨率或者比特率，解析二级m3u8（如果有的话）---------------------------
    content = content.decode('utf8')
    variant_m3u8 = m3u8.loads(content)
    streams_uri = dict()

    # 对于每一个ts视频列表，转化为分辨率-->uri或者比特率-->uri的映射关系，并保存在streams_uri中
    for playlist in variant_m3u8.playlists:
        if playlist.stream_info.resolution :
            resolution = int(playlist.stream_info.resolution[1])
            logger.info("Stream at %dp detected!" % resolution)
        else:
            resolution = int(playlist.stream_info.bandwidth)
            logger.info("Stream with bandwidth %d detected!" % resolution)
        streams_uri[resolution] = urljoin(playlist_url, playlist.uri)
    #------------------------------------------------------------------------------------------------------

    #-----------------------------选取最终要下载的ts视频列表对应的m3u8链接地址-----------------------------
    auto_highest = True # 是否自动选取最高画质的那个ts视频列表
    stream_res = 0

    if auto_highest and len(variant_m3u8.playlists) > 0:    # 如果存在二级m3u8索引
        stream_res = max(streams_uri)
        logger.info("Stream Picked: %dp" % stream_res)
        stream_uri = streams_uri[stream_res]
    else: # 如果没有二级m3u8索引，那么就选一级m3u8索引即可（说明没有可选的其他画质）
        stream_uri = playlist_url
    logger.info("Chunk List: %s" % (stream_uri))
    #------------------------------------------------------------------------------------------------------


    while True:
        try:
            pl_res = control.get(stream_uri, timeout=(2, 2), cookies=cookie_dict, headers=header_dict)
        #------------------------------------如果出错则进行重试------------------------------------
        except Exception as e:
            logger.error("Cannot Get Chunklist")
            if playlist_retry_count < playlist_max_retry:
                sleep(playlist_retry_delay)
                playlist_retry_count += 1
                continue
            else:
                break
        if not pl_res.status_code == requests.codes.ok:
            logger.error("Cannot Get Chunklist")
            if playlist_retry_count < playlist_max_retry:
                sleep(playlist_retry_delay)
                playlist_retry_count += 1
                continue
            else:
                break
        #---------------------------------------------------------------------------------------------
        playlist_retry_count = 0 # 重置网络请求重试计数器
        content = pl_res.content
        content = content.decode('utf8')
        chunklist = m3u8.loads(content)

        if chunklist.media_sequence == None:
            logger.warning("Incorrect Chunklist")
            sleep(playlist_retry_delay)
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

        if list_end:    # 已经加载完了所有ts文件的下载地址
            break

        logger.debug("Sleep for %d secs before reloading" % (sleep_dur))
        sleep(sleep_dur)

    logger.info("Stream Ended")
    thread_pool.shutdown()
