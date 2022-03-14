import csv
import os
from _csv import writer
from os.path import exists, dirname, join
#from dotenv import load_dotenv

import logging

import sniffer as sniffer

sniffer = csv.Sniffer()


def get_dialect(file_name):
    with open(file_name, 'r', newline='') as fn:
        snip = fn.read(2048)
        fn.seek(0)
        return sniffer.sniff(snip)


def writer(header, data, filename, dialect, ctr):
    with open(filename, "a+", newline="") as csvfile:
        write = csv.DictWriter(csvfile, fieldnames=header, dialect=dialect)
        if ctr == 1:
            write.writeheader()
        write.writerows(data)


def updater(filename, last_id, dialect, ctr):
    print('updater' + filename + ' ' + str(last_id))
    value = 0
    with open(filename, 'r', newline="") as file:
        data = [row for row in csv.DictReader(file, dialect=dialect)]
        for row in data:
            if ctr == 1:
                value = float(row['Capacity/mA.h/g'])
            else:
                value = last_id + float(row['Capacity/mA.h/g'])
            row['Capacity/mA.h/g'] = value
            ewe = float(row['Ewe/V'])
            row['Ewe/V'] = ewe

    header = data[0].keys()
    writer(header, data, 'E:\dylan\\new.csv', dialect, ctr)
    last_id = value
    return last_id


def log():
    log_format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename="process.log", level=logging.DEBUG, format=log_format, filemode='w')
    logger = logging.getLogger()
    return logger

def set_vars():
    print(dirname(__file__))
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    # global dish_auth_key
    # dish_auth_key = os.environ.get('dish_auth_key')
    # global dish_auth_url
    # dish_auth_url = os.environ.get('dish_auth_url')
    # global dish_payload_url
    # dish_payload_url = os.environ.get('dish_payload_url')
    # global centree_url
    # centree_url = os.environ.get('centree_url')

def process(logger):
    count_files = 7
    if exists('E:\dylan\\new.csv'):
        os.remove('E:\dylan\\new.csv')
    for i in range(1, count_files):
        filename = 'E:\dylan\File' + str(i) + '.csv'
        if i == 1:
            dialect = get_dialect(filename)
            last_id = updater(filename, 0, dialect, i)
        else:
            last_id = updater(filename, last_id, dialect, i)


def main():
    logger = log()
    set_vars()
    process(logger)


if __name__ == "__main__":
    main()
