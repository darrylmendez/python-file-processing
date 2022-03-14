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


def get_last_val(file_name):
    with open(file_name, 'r', newline='') as file:
        rw = csv.reader(file, get_dialect(file_name))
        last_val = float(list(rw)[-1][0])
    return last_val


def writer(header, data, filename, dialect):
    with open(filename, "a+", newline="") as csvfile:
        write = csv.DictWriter(csvfile, fieldnames=header, dialect=dialect)
        write.writeheader()
        print(filename)
        # print(data)
        write.writerows(data)


def updater(filename, last_id, dialect, ctr):
    print('updater' + filename + ' ' + str(last_id))
    value = 0
    with open(filename, 'r', newline="") as file:
        data = [row for row in csv.DictReader(file, dialect=dialect)]
        if ctr == 1:
            for row in data:
                last_id = float(row['Capacity/mA.h/g'])
                ewe = float(row['Ewe/V'])
                row['Capacity/mA.h/g'] = last_id
                row['Ewe/V'] = ewe
                # print(row['Capacity/mA.h/g'])
        else:
            for row in data:
                value = last_id + float(row['Capacity/mA.h/g'])
                row['Capacity/mA.h/g'] = value
                # last_id = float(row['Capacity/mA.h/g'])
                # row['Capacity/mA.h/g'] = last_id
                ewe = float(row['Ewe/V'])
                row['Ewe/V'] = ewe
    # print(data)
    header = data[0].keys()
    # os.remove('E:\dylan\\new.csv')
    writer(header, data, 'E:\dylan\\new.csv', dialect)
    last_id = value
    return last_id


def main():
    count_files = 7
    for i in range(1, count_files):
        filename = 'E:\dylan\File' + str(i) + '.csv'
        if i == 1:
            dialect = get_dialect(filename)
            updater(filename, 0, dialect, i)
            last_id = get_last_val(filename)
        else:
            last_id = updater(filename, last_id, dialect, i)
        # print(filename)
        # print('last_id')
        # print(last_id)


if __name__ == "__main__":
    main()
