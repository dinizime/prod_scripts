#Author: jossan.costa@eb.mil.br

import sys
import csv
import platform
import os
import psycopg2
import time

def createDb(user, password, host, port, database):
    conn = psycopg2.connect(
        u"""dbname='{0}' user='{1}' host='{2}' port='{3}' password='{4}'""".format(
            'postgres', 
            user, 
            host, 
            port, 
            password
        )
    )
    conn.set_session(autocommit=True)
    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE ' + str(database))

def createPath(filePath):
    try:
        path, _ = os.path.split(filePath)
        os.makedirs(path)
    except FileExistsError as e:
        pass

def backupPg(user, password, host, port, database, filePath):
    createPath(filePath)
    if platform.system() == 'Linux':
        os.system('''export PGPASSWORD="{0}" && pg_dump -O -x -U {1} -h {2} -p {3} -d {4} -f "{5}"'''.format(password, user, host, port, database, filePath))
    elif platform.system() == 'Windows':
        os.system('''SET PGPASSWORD={0}'''.format(password))
        os.system('''pg_dump -O -x -U {0} -h {1} -p {2} -d {3} -f "{4}"'''.format(user, host, port, database, filePath))

def restorePg(user, password, host, port, database, filePath):
    createDb(user, password, host, port, database)
    if platform.system() == 'Linux':
        os.system('''export PGPASSWORD="{0}" && psql -U {1} -h {2} -p {3} -d {4} -f "{5}"'''.format(password, user, host, port, database, filePath))
    elif platform.system() == 'Windows':
        os.system('''SET PGPASSWORD={0}'''.format(password))
        os.system('''psql -U {0} -h {1} -p {2} -d {3} -f "{4}"'''.format(user, host, port, database, filePath))

def readCsv(csvPath):
    with open('{0}'.format(csvPath), "r") as infile:
        reader = csv.reader(infile)
        next(reader, None)
        for row in reader:
            yield row

def main(argv):
    try:
        csvInput = argv[0]
        user = argv[1]
        password = argv[2]
        mode = argv[3]
        pgFunction = None
        if mode == '--restore':
            pgFunction = restorePg
        else:
            pgFunction = backupPg
        for row in readCsv(csvInput):
            host, port, database, path = row
            pgFunction(user, password, host, port, database, path)
    except Exception as e:
        print('usage: pgFromCsv.py file.csv <user> "<password>" --backup|--restore')
        print(e)

if __name__ == '__main__':
    main(sys.argv[1:])