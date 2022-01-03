# coding: utf-8
# author: jwxie - xiejiawei000@gmail.com
import json
import logging
import os
import random
import time
from urllib import parse

import requests
from bs4 import BeautifulSoup


def get_logger(with_file_log=True):
    logger = logging.getLogger('CDN_CRAWLER')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    if with_file_log:
        os.makedirs('logs', exist_ok=True)
        ct = time.localtime()
        fh = logging.FileHandler(f'logs/{ct.tm_mon}-{ct.tm_mday}__{ct.tm_hour}_{ct.tm_min}_{ct.tm_sec}.log',
                                 encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


LOGGER = get_logger()
DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' \
             '(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62'
BASE_URL = 'https://cdn.chinaz.com'
DEFAULT_PROXY_PROFILE = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}


def make_request(url, *, ua=None, proxies=None):
    response = requests.get(
        url=url,
        proxies=proxies if isinstance(proxies, dict) else DEFAULT_PROXY_PROFILE,
        headers={
            'user_agent': ua if isinstance(ua, str) else DEFAULT_UA,
        }
    )
    return BeautifulSoup(response.text.encode(response.encoding), features="lxml")


def sleep_module():
    p = logging.getLogger('CDN_CRAWLER')

    delay = random.randint(1, 5) + random.random()
    p.info(f'休息{delay}秒.')
    time.sleep(delay)
    if random.random() < 0.2:
        delay2 = random.random() * 5
        p.info(f'命中红心！喝口水，多休息休息一下~，大约{delay2}秒')
        time.sleep(delay2)

    p.info('休息结束，开始工作！')


get_cdn_ip = lambda ck, page_index: requests.get(
    url=f'https://cdn.chinaz.com/ajax/AreaIP?cdnkey={ck}&area=&net=&pageindex={page_index}&cnt=0',
    headers={
        'user_agent': DEFAULT_UA,
        'refer': 'https://cdn.chinaz.com/server/%E7%99%BE%E5%BA%A6%E4%BA%91%E5%8A%A0%E9%80%9F',
        'cookie': 'ucvalidate=38e8a8ad-b0e4-eebb-69e7-2080b0c859cd, '
                  'chinaz_topuser=de1a1dec-f728-2a7e-f7b4-ca4c1a6fdb89',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9'
    },
    proxies=DEFAULT_PROXY_PROFILE,
)

main_page_soup = make_request(url=BASE_URL)

cdn_top_list = main_page_soup.find_all(attrs={'class': 'toplist-main'})
if cdn_top_list:
    cdn_top_list = cdn_top_list[0]
else:
    raise ValueError("没有在main_page中找到'toplist-main'字段")

cdn_companies = cdn_top_list.find_all(attrs={'class': 'ulcont w15-0 companyname'})
for cdn_provider_tag in cdn_companies:
    cdn_provider_name = cdn_provider_tag.text
    cdn_provider_ref_link = cdn_provider_tag.next.get('href', None)
    if not cdn_provider_ref_link:
        LOGGER.info(f'对于{cdn_provider_name}没有找到对应的cdn页面链接，将跳过.')
        continue

    cdn_provider_main_page_link = BASE_URL + parse.unquote(cdn_provider_ref_link)
    LOGGER.info(f'正在处理{cdn_provider_name}的子页面数据，链接为：{cdn_provider_main_page_link}')
    cdn_provider_main_page = make_request(cdn_provider_main_page_link)

    cdn_key = cdn_provider_main_page.find_all(attrs={'id': 'cdnkey'})[0].get('value')
    rep = get_cdn_ip(ck=parse.quote(cdn_key), page_index=1)
    provider_cdn_data = []
    count = 0
    if rep or rep.status_code != 200:
        _rep_json = rep.json()
        count = _rep_json.get('count', None)
        provider_cdn_data += _rep_json.get('data', None)
        LOGGER.info(f"初始化请求成功，一共有{count}条数据，当前进度{len(provider_cdn_data)}/{count}")
    else:
        if rep:
            LOGGER.error(f'请求成功，但是发生了一些奇怪的错误，错误为：{rep.text}')
        else:
            raise ValueError('初始化请求失败，请进行一些检查')
    for i in range(2, count // 20):
        try:
            rep = get_cdn_ip(ck=parse.quote(cdn_key), page_index=i)
            if rep:
                _rep_json = rep.json()
                if hasattr(_rep_json, 'data'):
                    provider_cdn_data += _rep_json.get('data', {'page_index' + str(i): 'None'})
                elif hasattr(_rep_json, 'status'):
                    LOGGER.warning('请求成功但是被ban了...')

                LOGGER.info(f"请求成功，一共有{count}条数据，当前进度{len(provider_cdn_data)}/{count}, pages={i}/{count // 20}")
            else:
                if rep:
                    LOGGER.error(f'请求成功，但是发生了一些奇怪的错误，错误为：{rep.text}')
                else:
                    LOGGER.error(f'请求失败，请进行一些检查，本次将跳过')
            sleep_module()

            if i % 200 == 0:
                os.makedirs(f'results', exist_ok=True)
                with open(f'results/cdn_{cdn_provider_name}.json', 'w', encoding='utf-8') as fp:
                    json.dump(provider_cdn_data, fp, ensure_ascii=False)
        except Exception as e:
            LOGGER.warning(f'当前{i}遇到错误，将会跳过。')
    LOGGER.info(f'{cdn_provider_name}已经处理完成，休息一个大的。')
    time.sleep(60 + random.random() * 10)
