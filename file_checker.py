# coding: utf-8
# author: jwxie - xiejiawei000@gmail.com

import logging
import os
import time
import typing

logger = logging.getLogger('DNS')


def file_checker(
        path: str,
        *,
        file_name: str,
        delay: int = 500,
        wait: int = 5
) -> typing.Generator:
    """
    文件检查器件，在path目录下找到对应的file_name的pcap文件，
    并考虑tcpdump保存的同名文件会以不同的数字后缀为结尾，尝试自动读取，
    在没有找到对应文件的时候将，间隔wait秒进行查询，最多支持延迟等待delay秒，
    在dealy秒后仍未检测到，将会终止。

    :param path: 需要检查的文件所在目录
    :param file_name: 需要检查的文件名
    :param delay: 支持最多等待多久
    :param wait: 支持当前未找到名字时，等待多久进行再次查询
    :return: 文件名完整路径生成器
    """
    if not os.path.exists(os.path.join(path, file_name)):
        msg = f'文件错误，没有在{path}目录下找到{file_name}文件'
        logger.info(msg)
        raise Exception(msg)

    try:
        for idx in range(999999999):
            if idx == 0:
                current_file_name = file_name
            else:
                current_file_name = f'{file_name}{idx}'

            for _ in range(0, delay, wait):
                if os.path.exists(os.path.join(path, current_file_name)):
                    logger.info(f'找到当前pcap包的名字为{current_file_name}')
                    break
                else:
                    logger.info(f'暂时未找到{current_file_name}包，等待{wait}s后重新检查.')
                    time.sleep(wait)

            yield os.path.join(path, current_file_name)
    except KeyboardInterrupt:
        logger.info('检测到用户手动停止，运行终止。')
