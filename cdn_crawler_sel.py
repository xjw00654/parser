# coding: utf-8
# author: jwxie - xiejiawei000@gmail.com
import logging
import os
import random
import re
import time

import selenium.webdriver.common.by
from selenium import webdriver

driver = webdriver.Chrome()


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


def sleep_module():
    p = logging.getLogger('CDN_CRAWLER')

    delay = random.randint(1, 3) + random.random()
    p.info(f'休息{delay}秒.')
    time.sleep(delay)
    if random.random() < 0.2:
        delay2 = random.random() * 3
        p.info(f'命中红心！喝口水，多休息休息一下~，大约{delay2}秒')
        time.sleep(delay2)

    p.info('休息结束，开始工作！')


LOGGER = get_logger()
DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' \
             '(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62'
BASE_URL = 'https://cdn.chinaz.com'
DEFAULT_PROXY_PROFILE = {
    # 'http': 'http://127.0.0.1:7890',
    # 'https': 'http://127.0.0.1:7890',
}

driver.get(url=BASE_URL)
len_cookies = len(driver.get_cookies())

LOGGER.info('请进行登录!!!')
while True:
    if len(driver.get_cookies()) == len_cookies:
        time.sleep(0.5)
    else:
        LOGGER.info('检测到已登录，将会继续...')
        break

toplist = driver.find_element('class name', 'toplist-main')

cdn_list = toplist.find_elements('class name', 'ullist')
_T = '\n'.join([e.text.split('\n')[0] for e in cdn_list])
LOGGER.info(f"在首页，找到{len(cdn_list)}个cdn服务商，分别为:\n {_T}")

cdn_list_href = [e.find_element_by_tag_name('a').get_attribute('href') for e in cdn_list]
cdn_list_name = [e.find_element_by_tag_name('a').text for e in cdn_list]

provider_cdn_data = {k: [] for k in cdn_list_name}
for i, (name, href) in enumerate(zip(cdn_list_name, cdn_list_href)):
    if i == 0:
        continue
    driver.get(href)

    total_ip_counts = int([e for e in re.findall(r'\d*', driver.find_element('id', 'areaipcount').text) if e][0])
    total_pages = int(total_ip_counts // 20)
    LOGGER.info(f'{name}一共有{total_pages}页数据，共计{total_ip_counts}条CDN ip数据')

    if os.path.exists(f'results/cdn_{name}.txt'):
        LOGGER.info(f'在results目录中找到cdn_{name}.txt文件，表示已经有过去的爬虫爬过部分记录')
        have_done_records = len(
            [e for e in open(f'results/cdn_{name}.txt', 'r', encoding='utf-8').readlines() if e != '\n'])
        start_page = int(have_done_records // 20) + 1
        LOGGER.info(f'当前已爬取记录为{have_done_records}，大约为{start_page - 1}页面，将会从{start_page}开始')

        LOGGER.info(f'正在进行页面跳转...')
        driver.find_element(webdriver.common.by.By.ID, 'pn').send_keys(start_page)
        driver.find_element(webdriver.common.by.By.ID, 'pageok').click()
        LOGGER.info(f'跳转完成')
    else:
        start_page = 2

    for page in range(start_page, total_pages + 1):
        LOGGER.info(f'正在处理{name}的第{page}/{total_pages}页...')
        box_data = driver.find_element('class name', 'box')
        ips = [e.text for e in box_data.find_elements('tag name', 'li')]
        provider_cdn_data[name].extend(ips)

        try:
            next_page_bottom = [e for e in driver.find_element(webdriver.common.by.By.ID, 'pagelist').find_elements(
                webdriver.common.by.By.TAG_NAME, 'a') if e.text == '>'][0]
            next_page_bottom.click()
            sleep_module()
        except Exception as e:
            LOGGER.warning(f'遇到错误{e}，跳过这个page...')
            sleep_module()
            continue
        if page % 100 == 0:
            os.makedirs(f'results', exist_ok=True)
            with open(f'results/cdn_{name}.txt', 'w', encoding='utf-8') as fp:
                for line in provider_cdn_data[name]:
                    fp.write(line.replace('\n', '\t') + '\n')
    LOGGER.info(f'{name}供应商爬取结束，共计{len(provider_cdn_data[name])}条数据.')
