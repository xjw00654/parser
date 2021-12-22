# coding: utf-8
# author: jwxie - xiejiawei000@gmail.com

from file_checker import file_checker
from parser import pcap_parser_generator


def pcap2csv(
        path: str,
):
    files = file_checker(path, file_name='')
    for file in files:
        pcap_g = pcap_parser_generator(file)
        for i, (ts, (eth, ip, proto, dns)) in enumerate(pcap_g):
            pass
