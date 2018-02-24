import sys
import psycopg2

"""
A very simple ETL pipeline: reading a local file and pushing to Redshift
"""

def extract(filename):
    """
    extract portion of ETL
     - In this case it is simply a local tab separated file
     - This file path is given as an input
     - Copy records from the file into a list and returns it
    """
    raw_rows = []
    with open(filename) as f:
        for line in f:
            raw_rows.append(line.rstrip())
    return raw_rows

def transform(raw_rows):
    """
    transform portion of ETL
     - Read line by line from the input parameter
     - Convert it into dictionary format
    """
    records = []
    for row in raw_rows:
        fields = row.split("\t")
        records.append({ 'id': fields[0], 'state': fields[1], 'city': fields[2] })
    return records

def load(dbname, host, user, password, port, records):
    """
    load portion of ETL
     - Read from dictionary and push it to Redshift
    """
    connstr = "dbname='{dbname}' user='{user}' host='{host}' password='{password}' port='{port}'".format(
        dbname=dbname,
        user=user,
        host=host,
        password=password,
        port=port
    )
    conn = psycopg2.connect(connstr)
    conn.autocommit = True
    cur = conn.cursor()
    for record in records:
        sql = "INSERT INTO zipcode VALUES({id}, '{state}', '{city}')".format(
            id=record['id'],
            state=record['state'],
            city=record['city'])
        print(sql)
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

# MAIN
dbname='prod'
user='admin'
host='13.124.7.179'
password='Kookmin1'
port='5439'

if __name__== "__main__"
    raw_rows = extract(tsv_filename)
    records = transform(raw_rows)
    load(dbname, user, host, password, port, records)
