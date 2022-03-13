import csv
import sniffer as sniffer

count_files = 3
i = 2
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


last_id = get_last_val('E:\dylan\File1.csv')
while i < count_files:
    filename = 'E:\dylan\File' + str(i) + '.csv'
    with open(filename, 'r', newline='') as f:
        snippet = f.read(2048)
        #go back to first row
        f.seek(0)
        dialect = sniffer.sniff(snippet)
        reader = csv.reader(f, dialect)
        if sniffer.has_header(snippet):
            header_row = next(reader)

        # for row in reader:
        #     cap = float(row[0]) + last_id
        #     print(cap)
        #     writer = csv.writer(filename, delimiter='\t')
        #     writer.writerow(cap, float(row[1]))
    with open(filename, 'r+', newline="") as file:
        readData = [row for row in csv.DictReader(file, delimiter='\t')]
        for row in readData:
            print(row['Capacity'])
            last_id += float(row['Capacity'])
            row['Capacity'] = last_id
        #print(readData)
        #readData[0]['Capacity'] = last_id + float(readData[0]['Capacity'])
        # print(readData)
        #readHeader = readData[0].keys()
        #writer(readHeader, readData, filename, "update")
    i += 1
