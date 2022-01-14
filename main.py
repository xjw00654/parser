# coding: utf-8
# author: jwxie - xiejiawei000@gmail.com
import multiprocessing as mp
import os
import socket
import time
from collections import defaultdict

import dpkt
import numpy as np
import pydblite

from file_checker import file_checker
from pcap_parser import pcap_parser_generator

_sn = defaultdict(None)
_sn = {
    'com': True, 'edu': True, 'org': True, 'gov': True, 'net': True, 'wrok': True, 'vip': True,
    'club': True, 'site': True, 'vip': True, 'top': True
}


def pcap2csv(
        path: str,
        file_name: str
):
    data = []
    files = file_checker(path, file_name=file_name)
    for file in files:
        pcap_g = pcap_parser_generator(file)
        for i, (ts, (eth, ip, udp, dns)) in enumerate(pcap_g):
            if dns.qr != dpkt.dns.DNS_R:  # 请求不要，只拿响应数据
                continue
            if len(dns.an) < 1:  # 回答数据不足的，也直接不管了
                continue

            ip_packet = [  # 拿到IP数据
                socket.inet_ntoa(ip.src),
                socket.inet_ntoa(ip.dst),
                udp.sport,
                udp.dport,
            ]

            data.append((ip_packet, dns.an))
        ss = 0
    np.save('a.npy', data)


def get_cdn_ip():
    import struct
    cdn_ip = [e.strip().split(',')[-1]
              for e in open('cdn_ip_202112231856.csv', 'r', encoding='utf-8').readlines()][1:]
    s = time.time()
    print('开始创建CDN IP内存数据库')
    pydb = pydblite.Base(':memory:')
    pydb.create('cdnIP')
    pydb.create_index('cdnIP')
    for elem in cdn_ip:
        pydb.insert(cdnIP=socket.inet_ntoa(struct.pack('I', socket.htonl(int(elem)))))
    print(f'cdnIP数据库创建完成，耗时{time.time() - s}')
    return pydb


def get_wl_db():
    ts = sorted(os.listdir('top1m'))
    _p = os.path.join('top1m', ts[-1])
    full_data = []
    for tp in os.listdir(_p):
        _p_csv = os.path.join(_p, tp, 'top-1m.csv')
        full_data += [e.strip().split(',')[1] for e in open(_p_csv, 'r').readlines()]

    wl = []
    for dn in full_data:
        spl = dn.split('.')
        if len(spl) < 2:
            continue
        if spl[-2] == 'com':
            if len(spl) == 2:
                continue
            else:
                wl.append(spl[-3])
        else:
            wl.append(spl[-2])
    wl = sorted(list(set(wl)))
    s = time.time()
    print('开始创建白名单内存数据库')
    pydb = pydblite.Base(':memory:')
    pydb.create('domain_name')
    pydb.create_index('domain_name')
    for elem in wl:
        pydb.insert(domain_name=elem)
    print(f'白名单数据库创建完成，耗时{time.time() - s}')
    return pydb


def sent_data(
        path: str,
        q: mp.Queue,
        num_processes=8,
):
    """
    在path目录里监测文件变化，并将数据送入到q队列里面

    :param path: 需要监测的文件夹
    :param q: 数据队列
    :param num_processes: 用来终止进程
    :return: None
    """

    def time2strap(tm):
        return str(int(time.mktime(time.strptime(tm, '%Y_%m%d_%H%M_%S'))))

    processed = []
    do_continue_times = 0
    while True:
        files = [e for e in os.listdir(path) if 'wl' not in e and e.endswith('.pcap')]  # 带wl的是有处理完成的
        if len(set(files) - set(processed)) < 5:
            time.sleep(10)
            do_continue_times += 1
            if do_continue_times >= 360:  # 超过3600秒没有新数据产生，直接break掉
                break
            continue
        else:
            do_continue_times = 0
            ll = list(set(files) - set(processed))
            lld = {
                e: time2strap(e.replace('.pcap', '')) for e in ll
            }
            lld_r = {
                v: k for k, v in lld.items()
            }
            ll_time_strap_sorted = sorted(list(lld.values()))[:-1]  # 最新的一个暂时不弄

            for _f in ll_time_strap_sorted:
                _f = os.path.join(path, lld_r[_f])  # 把完成路径给出来
                try:
                    if _f not in processed:
                        processed.append(_f)
                        q.put_nowait(_f)
                    else:
                        continue
                except Exception:
                    print(_f, '在传输数据到队列时遇到错误，请人工处理')

    for _f in list(set(files) - set(processed)):  # 剩下一些东西，也要跑一下
        _f = os.path.join(path, _f)  # 把完成路径给出来
        try:
            if _f not in processed:
                processed.append(_f)
                q.put_nowait(_f)
            else:
                continue
        except Exception:
            print(_f, '在传输数据到队列时遇到错误，请人工处理')
    for i in range(num_processes):
        q.put_nowait('STOP')


def filter_wl(
        q: mp.Queue,
        pydb_wl: pydblite.Base,
        pydb_ip: pydblite.Base
):
    """
    从队列q里面取数据，取数据做处理并保存

    :param q: 数据队列
    :param pydb_wl: 白名单数据库对象
    :param pydb_ip: cdn ip白名单对象
    :return: None
    """
    while True:
        try:
            _f = q.get_nowait()
            if _f is None:
                time.sleep(1)
                continue
        except Exception as e:
            time.sleep(1)
            continue
        if _f == 'STOP':
            print('GET STOP !!')
            break

        print('GET: ', _f)
        pcap = pcap_parser_generator(_f)
        fw = open(_f.replace('.pcap', 'wl.pcap'), 'wb')
        writer = dpkt.pcap.Writer(fw)

        num_writes = 0
        for ts, (eth, _, _, dns) in pcap:
            # if dns.qr != dpkt.dns.DNS_R:  # 请求不要，只拿响应数据
            #     continue
            # if len(dns.an) < 1:  # 回答数据不足的，也直接不管了
            #     continue

            in_wl_nums = 0
            for qd in dns.qd:
                dn = qd.name
                dn_spl = dn.split('.')
                dn_sld = ""
                if len(dn_spl) < 2:
                    continue
                if _sn[dn_spl[-2]]:
                    if len(dn_spl) == 2:
                        continue
                    dn_sld = dn_spl[-3]
                else:
                    dn_sld = dn_spl[-2]
                if pydb_wl(domain_name=dn_sld):
                    in_wl_nums += 1

            ttl_ok_nums = 0  # 似乎不顶啥用
            for an in dns.an:
                if an.ttl > 1800:
                    ttl_ok_nums += 1

            cdn_ip_ok_nums = 0
            for an in dns.an:
                if hasattr(an, 'ip'):
                    try:
                        real_ip = socket.inet_ntoa(an.ip)
                    except Exception:
                        continue
                    if pydb_ip(cdnIP=real_ip):
                        cdn_ip_ok_nums += 1

            # 有不在白名单的qd；
            # 是响应包：
            # 1、有不在白名单的ip；
            # 2、有小于1800的ttl记录（不管A还是CNAME还是其他）
            if len(dns.qd) == in_wl_nums:  # 在白名单里
                continue
            else:  # 不在白名单里
                if dns.qr == dpkt.dns.DNS_R:  # 保证是响应，因为请求没有TTL和IP
                    if len(dns.an) != ttl_ok_nums or len(dns.an) != cdn_ip_ok_nums:  # 有异常TTL的记录 or 有非CDN记录
                        writer.writepkt(eth, ts=ts)
                        num_writes += 1
                else:
                    writer.writepkt(eth, ts=ts)
                    num_writes += 1

        print(f'{_f}一共剩下{num_writes}个数据包，将会删除原始包')
        os.remove(_f)


if __name__ == '__main__':
    # pcap_path = 'c:\\Users\\JiaweiXie\\Desktop\\dns_pcap'
    pydb_ip = get_cdn_ip()
    pydb_wl = get_wl_db()
    q = mp.Queue()
    q.put('c:\\Users\\JiaweiXie\\Desktop\\dns_pcap\\2022_0107_1833_37wl.pcap')

    # n = 8
    # p_set = []
    # for i in range(n):
    #     p = mp.Process(target=filter_wl, args=(q, pydb_wl, pydb_ip))
    #     p.start()
    #     p_set.append(p)
    # s_p = mp.Process(target=sent_data, args=(pcap_path, q, n))
    # s_p.start()
    # for p in p_set:
    #     p.join()

    filter_wl(q, pydb_wl, pydb_ip)
    print('DONE')
