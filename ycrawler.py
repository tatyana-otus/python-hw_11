import logging
import asyncio
from html.parser import HTMLParser
import os
from concurrent import futures
import collections
from optparse import OptionParser
import configparser

import aiohttp


URL = 'http://news.ycombinator.com/'
COMMENT_URL = 'https://news.ycombinator.com/item?id={}'

HTTP_OK = 200
HTTP_SERVER_CURRENTLY_UNABLE = 503

LinkInfo = collections.namedtuple("LinkInfo", ["id", "url"])

DEF_CONFIG_FILE_NAME = 'ycomb.cfg'
DEF_CONFIG = {
    'net': {'retries': 5, 'retry_timeout': 1},
    'common': {'data_dir': 'ycombinator', 'host_max_conn': 3,
               'polling_cycle': 5},
    'log': {'filename': None}
}


class StoryLinkParser(HTMLParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.link_id = None
        self.link = None
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a' and ('class', 'storylink') in attrs:
            for k, v in attrs:
                if k == 'href':
                    self.link = v
                    return
        if tag == 'tr' and ('class', 'athing') in attrs:
            for k, v in attrs:
                if k == 'id':
                    self.link_id = v
                    return

    def handle_endtag(self, tag):
        if self.link_id is not None and self.link is not None:
            self.links.append(LinkInfo(self.link_id, self.link))
            self.link_id = None
            self.link = None


class CommentLinkParser(HTMLParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inside_comment = False
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'div' and ('class', 'comment') in attrs:
            self.inside_comment = True
            return
        if self.inside_comment and tag == 'a' and ('rel', 'nofollow') in attrs:
            for k, v in attrs:
                if k == 'href':
                    self.links.append(v)
                    return

    def handle_endtag(self, tag):
        if tag == 'div':
            self.inside_comment = False


def get_comment_links(page):
    parser = CommentLinkParser()
    parser.feed(page)
    return parser.links


def get_story_links(page):
    parser = StoryLinkParser()
    parser.feed(page)
    links = []
    for item in parser.links:
        if not item.url.startswith('http'):
            links.append(LinkInfo(item.id, URL + item.url))
        else:
            links.append(item)
    return links


def url_to_fn(url):
    if url[-1] == '/':
        return url[:-1].split('/')[-1]
    return url.split('/')[-1]


def write_to_file(dir_path, link, data):
    file_name = url_to_fn(link)
    try:
        path = os.path.join(dir_path, file_name)
        if not os.path.exists(path):
            with open(path, 'wb') as f:
                f.write(data)
                logging.debug("File saved {}".format(file_name))
        else:
            logging.debug("File alredy exist {}".format(path))
    except Exception as e:
        logging.error("Error saving to {}: {}".format(path, e))


async def fetch_url(session, url, retries, retry_timeout):
    attempts = retries
    while True:
        try:
            async with session.get(url) as resp:
                if resp.status == HTTP_OK:
                    return await resp.content.read()
                elif resp.status == HTTP_SERVER_CURRENTLY_UNABLE:
                    raise aiohttp.ClientError('HTTP error: {}'.format(resp.status))
                else:
                    logging.error("Error geting link {}: {}".format(url, resp.status))
                    return None
        except aiohttp.ClientError as e:
            attempts -= 1
            if attempts <= 0:
                logging.error("Error geting link {}: {}".format(url, e))
                return None
            await asyncio.sleep(retry_timeout)


async def save_link(session, url, dir_path, net_cfg):
    page = await fetch_url(session, url, **net_cfg)
    if page is not None:
        with futures.ProcessPoolExecutor(max_workers=1) as pool:
            pool.submit(write_to_file, dir_path, url, page)


async def save_story(session, base_dir, link_info, net_cfg):
    dir_path = os.path.join(base_dir, link_info.id + '_' + url_to_fn(link_info.url))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    await save_link(session, link_info.url, dir_path, net_cfg)
    page = await fetch_url(session, COMMENT_URL.format(link_info.id), **net_cfg)
    if page is not None:
        links = get_comment_links(page.decode("utf-8"))
        for url in links:
            await save_link(session, url, dir_path, net_cfg)


async def crawler(net_cfg, data_dir, host_max_conn, polling_cycle):
    links = []
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.connector.TCPConnector(limit=0, limit_per_host=host_max_conn)
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as stories_session:
            while True:
                page = await fetch_url(session, URL, **net_cfg)
                if page is not None:
                    new_links = get_story_links(page.decode("utf-8"))
                    add_links = [i for i in new_links if i not in links]
                    for item in add_links:
                        loop.create_task(save_story(stories_session, data_dir, item, net_cfg))
                    links = new_links
                await asyncio.sleep(polling_cycle)


def get_config(cfg_file):
    cfg = DEF_CONFIG
    if os.path.isfile(cfg_file):
        parser = configparser.ConfigParser()
        parser.read(cfg_file, encoding='utf-8')
        for sec in parser.sections():
            for opt in parser.options(sec):
                value = parser.get(sec, opt, fallback=cfg[sec][opt])
                if isinstance(cfg[sec][opt], (int, float)):
                    cfg[sec][opt] = float(value)
                else:
                    cfg[sec][opt] = value
    return cfg


if __name__ == '__main__':
    op = OptionParser()
    op.add_option('--cfg', action="store", default=DEF_CONFIG_FILE_NAME,
                  help='Configuration file name, default is {}'.format(DEF_CONFIG_FILE_NAME))
    (opts, _) = op.parse_args()
    cfg = get_config(opts.cfg)
    logging.basicConfig(filename=cfg['log']['filename'],
                        level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    com_cfg = cfg['common']
    net_cfg = cfg['net']
    try:
        if not os.path.exists(com_cfg['data_dir']):
            os.makedirs(com_cfg['data_dir'])
        loop = asyncio.get_event_loop()
        loop.run_until_complete(crawler(net_cfg, **com_cfg))
    except KeyboardInterrupt as e:
        pass
    except Exception as e:
        logging.exception("Unexpected error: {}".format(e))
