import csv
import os
from _csv import writer

import sniffer as sniffer

sniffer = csv.Sniffer()


def get_dialect(file_name):
    with open(file_name, 'r', newline='') as fn:
        snip = fn.read(2048)
        fn.seek(0)
        return sniffer.sniff(snip)


def writer(header, data, filename, dialect):
    with open(filename, "a+", newline="") as csvfile:
        write = csv.DictWriter(csvfile, fieldnames=header, dialect=dialect)
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
    writer(header, data, 'E:\dylan\\new.csv', dialect)
    last_id = value
    return last_id


def main():
    count_files = 7
    os.remove('E:\dylan\\new.csv')
    for i in range(1, count_files):
        filename = 'E:\dylan\File' + str(i) + '.csv'
        if i == 1:
            dialect = get_dialect(filename)
            last_id = updater(filename, 0, dialect, i)
        else:
            last_id = updater(filename, last_id, dialect, i)


if __name__ == "__main__":
    main()
