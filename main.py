import csv
import os
from _csv import writer
from os.path import exists, dirname, join
import logging
import sniffer as sniffer
from dotenv import load_dotenv

global input_file_path
global input_file_name
global output_file_path
global stage_file_path
sniffer = csv.Sniffer()


def get_dialect(file_name):
    with open(file_name, 'r', newline='') as fn:
        snip = fn.read(2048)
        fn.seek(0)
        return sniffer.sniff(snip)


def writer(header, data, filename, dialect, ctr):
    with open(filename, "a+", newline="") as csvfile:
        write = csv.DictWriter(csvfile, fieldnames=header, dialect=dialect)
        if ctr == 0:
            write.writeheader()
        write.writerows(data)


def updater(filename, last_id, dialect, ctr, logger):
    logger.info('updater: ' + filename + ' Last Id ' + str(last_id))
    with open(filename, 'r', newline="") as file:
        data = [row for row in csv.DictReader(file, dialect=dialect)]
        for row in data:
            value = last_id + float(row['Capacity/mA.h/g'])
            row['Capacity/mA.h/g'] = value
            ewe = float(row['Ewe/V'])
            row['Ewe/V'] = ewe

    header = data[0].keys()
    writer(header, data, output_file_path, dialect, ctr)
    last_id = value
    return last_id


def log():
    log_format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename="process.log", level=logging.DEBUG, format=log_format, filemode='w')
    logger = logging.getLogger()
    return logger


def set_vars(logger):
    logger.info(dirname(__file__))
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    global input_file_path
    input_file_path = os.environ.get('input_file_path')
    global output_file_path
    output_file_path = os.environ.get('output_file_path')
    global input_file_name
    input_file_name = os.environ.get('input_file_name')
    global stage_file_path
    stage_file_path = os.environ.get('stage_file_path')


def get_file_count(file_path, logger):
    file_count = len([name for name in os.listdir(file_path)])
    logger.info('File Count: ' + str(file_count))
    return file_count


def file_writer(data, filename):
    with open(filename, "a+", newline="") as csvfile:
        csvfile.write(data)


def stage_cleanup(logger):
    path = stage_file_path
    for f in os.listdir(path):
        logger.info("stage cleanup : " + f)
        os.remove(os.path.join(path, f))


def file_split(logger):
    stage_cleanup(logger)
    filename = input_file_path + input_file_name
    counter = 0
    with open(filename, "r") as input_file:
        for line in input_file:
            if line.startswith('C:') or line.startswith('D:'):
                stage_file_name = stage_file_path + 'File' + str(counter) + '.csv'
                logger.info("File Split : " + stage_file_name)
                counter = counter + 1
                continue
            else:
                if line != "\n":
                    file_writer(line, stage_file_name)


def process(logger):
    if exists(output_file_path):
        os.remove(output_file_path)
    file_count = get_file_count(stage_file_path, logger)
    for i in range(0, file_count):
        filename = stage_file_path + 'File' + str(i) + '.csv'
        if i == 0:
            dialect = get_dialect(filename)
            last_id = updater(filename, 0, dialect, i, logger)
        else:
            last_id = updater(filename, last_id, dialect, i, logger)


def main():
    logger = log()
    set_vars(logger)
    file_split(logger)
    process(logger)


if __name__ == "__main__":
    main()