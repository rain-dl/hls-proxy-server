# hls-proxy-server
A simple http server to automatic proxy hls stream.

**Usage:** python3 hls-proxy-server.py [-h] -p PORT -d DIRECTORY -c CONF</br>
&ensp;&ensp;PORT: The server will listen on 0.0.0.0:PORT.</br>
&ensp;&ensp;DIRECTORY: The root directory of the http server. All m3u8 and ts files downloaded from original hls url will stored in sub directories of this directory.</br>
&ensp;&ensp;CONF: A file in json format to store the proxy http path, original hls url and clean up time of the proxy that no client accessed.</br>

The format of proxy configuration file:
```
{
	"hls_proxies": {
		"/stream1.m3u8": {
			"url": "http://xxx.xxx.xxx.xxx/xxx/1.m3u8",
			"cleanup": 180
		},
		"/stream2.m3u8": {
			"url": "http://xxx.xxx.xxx.xxx/xxx/2.m3u8",
			"cleanup": 180
		}
}
```

File hls-downloader.py is the modified version of project <a href="https://github.com/gh877916059/hls-downloader-annotated" target="_blank">hls-downloader-annotated</a>. Added support for regenerate the m3u8 file.
