import csv, codecs

def unicode_csv_reader(f, encoding="utf-8"):
        # csv.py doesn't do Unicode; encode temporarily as UTF-8:

    csv_reader = csv.reader(f)
    for row in csv_reader:
    # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('ascii', 'ignore')

def split_csv(line):
    cols = line.strip("\n\r ").split('"')
    for i in range(1,len(cols),2):
        cols[i] = cols[i].replace(',', '@@')
    newline = ''.join(cols)
    return newline.split(',')
