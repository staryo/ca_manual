import json
import sys
from collections import defaultdict
from contextlib import closing
from xml.etree.ElementTree import XMLParser, parse, ParseError

import paramiko
from tqdm import tqdm

from utils.xml_tools import get_text_value, get_float_value_with_dot


class KKFtpReader:
    def __init__(self, last_path, ftp_session):
        self.last_path = last_path
        self.ftp_client = ftp_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...
        # with open('last_path.json', 'w', encoding='utf-8') as input_file:
        #     json.dump(
        #         self.last_path,
        #         input_file
        #     )

    def __enter__(self):
        return self

    def read_from_ftp(self, path_to_iter):
        report = []
        for i in sorted(
                self.ftp_client.listdir(
                    path=path_to_iter
                )
        ):
            lstatout = str(self.ftp_client.lstat(
                '{}/{}'.format(path_to_iter, i)
            )).split()[0]
            if 'd' in lstatout:
                report.append('{}/{}'.format(path_to_iter, i))
        return report

    def read_tech_from_ftp(self, path_to_iter):
        iter1 = tqdm(
            sorted(self.read_from_ftp(path_to_iter)),
            desc=path_to_iter,
            file=sys.stdout,
            position=0
        )
        for each_path in iter1:
            if each_path < self.last_path[:len(each_path)]:
                continue
            if self.read_from_ftp(each_path):
                look_for_files = self.read_tech_from_ftp(each_path)
                for i in look_for_files:
                    yield i
                continue
            self.last_path = each_path
            report = self.read_file_from_ftp(each_path)
            if report:
                yield {' '.join(each_path.split('/')[-3:-1]): report}
                # report[' '.join(each_path.split('/')[-3:-1])] = report

    def read_file_from_ftp(self, path):
        try:
            with closing(self.ftp_client.open('{}/body'.format(path))) as f:
                # парсим xml
                try:
                    # ставим utf-8 хардкодом, чтоб
                    # никаких неожиданностей не было
                    xmlp = XMLParser(encoding="utf-8")
                    tree = parse(f, parser=xmlp)
                    root = tree.getroot()
                except ParseError:
                    tqdm.write('Ошибка чтения файла -- не распознан корень')
                    return
                header = root.find('Header')
                if header is None:
                    tqdm.write(
                        'Ошибка чтения файла -- не распознан Header')
                    return
                items = root.findall('Items')
                if items is None:
                    tqdm.write(
                        'Ошибка чтения файла -- не распознан Items')
                    return
                report = {
                    'DOCUMENT': get_text_value(header, 'MBLNR'),
                    'RECEIVED': defaultdict(float),
                    'DONE': defaultdict(float),
                    'DELIVERED': defaultdict(float)
                }
                check = False
                for item in items:
                    # если вид движения 301/311 и адресат -- 1072,
                    # то считаем сумму и запускаем партии
                    is_receive = get_text_value(item, 'BWART') in ['301', '311']
                    is_recepient = get_text_value(item, 'UMLGO') == '1072'
                    if is_receive and is_recepient:
                        report['DEPARTMENT'] = get_text_value(item, 'LGORT')
                        report['RECEIVED'][get_text_value(item, 'UMMAT')] += \
                            get_float_value_with_dot(item, 'ERFMG')
                        check = True

                    # если вид движения 301/311 и отправитель -- 1072,
                    # то считаем сумму и закрываем партии, которые в статусе "обработка завершена"
                    is_send = get_text_value(item, 'BWART') in ['301', '311']
                    is_sender = get_text_value(item, 'LGORT') == '1072'
                    if is_send and is_sender:
                        report['DELIVERED'][get_text_value(item, 'MATNR')] += \
                            get_float_value_with_dot(item, 'ERFMG')
                        check = True

                    # если вид движения 101 и исполнитель -- 1072,
                    # то считаем сумму и закрываем "обработка завершена" по партии
                    is_done = get_text_value(item, 'BWART') == '101'
                    is_executor = get_text_value(item, 'UMLGO') == '1072'
                    if is_done and is_executor:
                        report['DONE'][get_text_value(item, 'UMMAT')] += \
                            get_float_value_with_dot(item, 'ERFMG')
                        check = True

        except FileNotFoundError:
            tqdm.write(f'Файл не найден {path}/body')
        if check:
            return report


if __name__ == '__main__':

    try:
        with open('last_path.json', 'r', encoding='utf-8') as input_file:
            last_path = json.load(
                input_file
            )
    except FileNotFoundError:
        last_path = ''

    sftpURL = 'kk-srv-bfg2.npo.izhmash'
    sftpUser = 'a.a.stolov'
    sftpPass = 'Yunku_Kk2021kK'
    sftpPath = '/home/http_request_collector/app/data/dop/input/ca/POST'
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        sftpURL,
        username=sftpUser,
        password=sftpPass
    )

    with closing(client) as ssh:
        with closing(ssh.open_sftp()) as ftp:
            with KKFtpReader(last_path, ftp) as ftp_session:
                path_list = ftp_session.read_from_ftp(sftpPath)
                for path in path_list:
                    if path < last_path[:len(path)]:
                        continue
                    ftp_session.read_tech_from_ftp(path)
