#!/usr/bin/env python
# coding: utf-8

import configparser
import sys
import getopt
import os
import logging
import re
from db_utils import db_connect
from db_utils import mysql_connect
from sqlite3 import IntegrityError
import hashlib
import copy
import random
import MySQLdb
import string
import time
import datetime
from multiprocessing import Manager, Pool
from multiprocessing.pool import ThreadPool # Multithreading





### configure logging
level    = logging.DEBUG
format   = '%(asctime)s - %(message)s'
handlers = [logging.FileHandler('test.log'), logging.StreamHandler()]
logging.basicConfig(level = level, format = format, handlers = handlers)


def readConfig():
    config = configparser.ConfigParser()
    config.read("config.ini")


class fingerprint():

    def createFingerprint(self,query):
        logging.debug(query)
        # where values is a simple number
        query=re.sub(r'(=|<>) *([\d]+)', r'\1<randomNumber>', query)
        # everything between '' and ""
        query_search=re.search(r'(\'|\")(.*?)(\'|\")', query)
        if query_search:
            result=self.data_type(query_search.group(2))
            query=re.sub(r'(\'|\")(.*?)(\'|\")', rf'{result}', query)

        # Between-And
        query_between=re.search(r'(?i)(WHERE(.*))BETWEEN(.*)AND(\'|\")?(\s\S*)(\'|\")?((;| )(.*))$', query)
        if query_between:
            type1=self.data_type(query_between.group(3))
            type2=self.data_type(query_between.group(5))
            query=re.sub(r'(?i)(WHERE(.*))BETWEEN(.*)AND(.*)', rf'\1BETWEEN {type1} AND {type2}<2> {query_between.group(7)}', query)

        #if re.match(r'(?i)^select', query):
        # IN and VALUES list
        if re.match(r'(?i).*( IN | VALUES?)', query):
            list=re.search(r'(?i)(.*)( IN | VALUES)?\s*\((.*)\)(.*)',query)
            #type3=self.data_type(list.group(3).split(",")[-1])
            query=('{} ('.format(list.group(1)))
            for value in list.group(3).split(","):
                type3=self.data_type(value)
                query=query+f'{type3},'
                #query=('{} ({}{}) {}'.format(
                #        list.group(1),
                #        type3,
                #        f',{type3}'*(len(list.group(3).split(","))-1),
                #        list.group(4)
                #    ))
            query=query[:-1]+")"+f'{list.group(4)}'


        if re.match(r'(?i)^update', query):
            # SET values
            if re.match(r'(?i).*(SET)', query):
                list=re.search(r'(?i)(.*SET\s*)(.*)(where .*)',query)
                set_list=list.group(2).split(",")
                # read values and split them
                for v,i in enumerate(set_list):
                    item=(i.split("="))
                    # if values is equal with the variable plus an intiger, that's incrementing so we do not change it
                    isincrement=re.match(rf'{item[0]} *\+ *\d',item[1])
                    if isincrement:
                        set_list[v]=item[0]+"="+item[1]

                    else:
                        item[1]=self.data_type(str(item[1]))
                        set_list[v]=item[0]+"="+item[1]

                query=('{}{} {}'.format(
                        list.group(1),
                        ', '.join(set_list),
                        list.group(3)
                    ))
        logging.debug(query)
        return(query)

    def data_type(self,data):
        data=data.strip()
        isnumber=re.match(r'^[+-]?((\d+(\.\d+)?)|(\.\d+))$',data)
        if isnumber:
            logging.debug(f"It is a number: {data}")
            return("<randomNumber>")
        else:
            isdate=re.search(r'\d{2}(?:\d{2})?-\d{1,2}-\d{1,2}( )?(\d{1,2}:\d{1,2}:\d{1,2})?(\.\d*)?',data)
            if isdate:
                logging.debug(f"It is a date: {data}")
                return("<randomDate>")
            else:
                logging.debug(f"It is a string: {data}")
                return("<randomString>")


class readData():

    def __init__(self):
        self.regex="(?i)^(select|update|insert|delete|replace)"
        self.splitstring="# Time: "
        self.trx_order_id=0
        self.count=1
        self.query_values=""


    def openFile(self,slowlogFile):
        if os.path.exists(slowlogFile):
           with open(slowlogFile, 'r') as f:
               try:
                    logging.debug(f"{slowlogFile} is opend.")
                    filecontent = f.readlines()
                    return filecontent

               except :
                   logging.critical("Could not Open the file!")
        else:
            logging.info("Slow log file does not exists!")


    def parseSlowLog(self,slowlogFile):
        # read the file into filecontent
        filecontent=readC.openFile(slowlogFile)
        # find first # Time: 2020-01-21T16:55:13.249612Z
        for lines in readC.filter_output_between_strings(filecontent, self.splitstring):
            for sublines in lines:
                if 'InnoDB_trx_id' in sublines:
                    trx=sublines.split(":")
                    trxid=str(trx[1]).strip()
                    if trxid != "0":
                        logging.info(f"This is part of a transaction: {trxid}")

                if re.match(self.regex, sublines) is not None:
                    query=sublines.rstrip()
                    query_text=finger.createFingerprint(query)
                    hash_obj = hashlib.md5(query_text.encode())
                    sql_statement=f"""Insert Into queries (trx_id,trx_order_id,count,query_hash,query_text,query_values) values ("{trxid}",{self.trx_order_id},{self.count},"{hash_obj.hexdigest()}","{query_text}","{self.query_values}")"""
                    #print(sql_statement)
                    try:
                        cur = con.cursor()
                        cur.execute(sql_statement)
                        con.commit()
                    except IntegrityError:
                        logging.debug(f"Query already inserted in the DB: {query_text}")

    def cleanDB(self):
        # There could be many transactions which contains the same queries, we will remove the duplicate transactions now.
        cur = con.cursor()
        # get all the transaction IDs
        cur.execute("select DISTINCT trx_id from queries order by trx_id asc")
        trx_ids = cur.fetchall()
        for i in trx_ids:
            # go trough all the transactions and select the hashs
            cur.execute(f'select query_hash from queries where trx_id="{i[0]}" order by trx_id,query_hash')
            try:
                # hashing the hashes
                hash1_result=cur.fetchall()
                hash1_obj = hashlib.md5(str(hash1_result).encode())
                hash1=hash1_obj.hexdigest()
                logging.debug(f"hash1: {hash1}")

                # get all the transaction IDs in a reverse order
                cur.execute("select DISTINCT trx_id from queries order by trx_id desc")
                trx_ids_reverse = cur.fetchall()
                for x in trx_ids_reverse:
                    cur.execute(f'select query_hash from queries where trx_id="{x[0]}" and trx_id<>"{i[0]}" order by trx_id,query_hash')
                    try:
                        # create hash from hashes
                        hash2_result=cur.fetchall()
                        hash2_obj = hashlib.md5(str(hash2_result).encode())
                        hash2=hash2_obj.hexdigest()
                        logging.debug(f"hash2: {hash2}")
                        # compare the two hash if they are the same that means the queries are the same in both transaction we can delete one of them.
                        if hash1 == hash2:
                            logging.debug(f"Same hash: {hash1} vs {hash2} ")
                            cur = con.cursor()
                            cur.execute(f'delete from queries where trx_id = "{x[0]}"')
                            con.commit()
                    except:
                        logging.debug(f"Id is not in DB: {x[0]}")


            except:
                logging.debug(f"Id is not in DB: {i[0]}")




    def filter_output_between_strings(self,listLog,separator):
        result = []
        sublist = []
        for x in listLog:
            if separator in x:
                if sublist:
                    result.append(sublist)
                sublist = []
            else:
                sublist.append(x)
        result.append(sublist)
        return result


class createWorkingSet():
    def readDB(self):
        # get all the transaction and queries
        cur.execute("select trx_id,trx_order_id,count,query_text,query_values from queries order by trx_id,trx_order_id;")

        return(cur.fetchall())

    def buildWorkingSet(self):
        try:
            all_fields=self.readDB()
            queue=[]
            for i,fields in enumerate(all_fields):
                if not fields[4] and fields[0] == "0":
                    queue.append(fields)
            return(queue)
        except:
            logging.info("Could not read the DB!")


class Counter(object):
    def __init__(self, man):
        self.val = man.Value('i', 0)
        self.lock = man.Lock()
    def increment(self):
        with self.lock:
            self.val.value += 1
    def value(self):
        with self.lock:
            return self.val.value

# class Counter():
#     def __init__(self):
#         self.value =  0
#     def increment(self):
#         self.value += 1
#     def value(self):
#         return self.value

class runMain():

    def runQueries(self,data):
        #counter = data[0]
        for row in str(data).splitlines():
        #try:
            query=str(row).split(',')[3]
            #query=row[3]
            randomInt=random.randint(0,10000)
            randomInt2=random.randint(0,10000)
            letters = string.ascii_lowercase
            randomStr='"'+''.join(random.choice(letters) for i in range(10))+'"'
            query=str(query).replace('\'','')
            query=str(query).replace('<randomNumber><2>',str(randomInt2))
            query=str(query).replace('<randomNumber>',str(randomInt))
            query=str(query).replace('<randomString>',str(randomStr))
            #mysqlcur = mysqlcon.cursor()
            try:
                #try:
                    #logging.debug(query)
                mysqlcur.execute(query)
                counter.increment()
                if re.match(r'(\d)+\.0',str(round(time.time(),1))):
                    logging.info(f'Number of Queries: {counter.val.value}')
                # NB : you won't get an IntegrityError when reading
            #except (MySQLdb.Error, MySQLdb.Warning) as e:
                logging.info(f"Error1= {e}")
                if '1062, "Duplicate entry' in str(e):
                    continue
                else:
                    return None

                try:
                    data = mysqlcur.fetchall()

                except TypeError as e:
                    logging.info(f"Error2= {e}")
                    return None

            finally:
                print("bela")
                #mysqlcur.close()
                #break



            #except Exception: # Try to catch something more specific
            #    pass

def start_threading(queries,numberofthreads):
    try:
        thread_pool = ThreadPool(numberofthreads)
        results = thread_pool.map(runmain.runQueries, queries)
        thread_pool.close()
        thread_pool.join()
        del thread_pool
        return all(results)
    except Exception as e:
        logging.debug(f'bela: {e}')
        return False


def main(argv):
    runtime = 10
    slowlogFile = ''
    message="""Usage:
    Process SlowLog File: pyLoad.py -i <slowlogFile>
    Start the the test Load: pyLoad.py --run (--sec 60)"""
    try:
        opts, args = getopt.getopt(argv,"hi:rs:",["slowlogFile=","run","sec"])
    except getopt.GetoptError:
        print(message)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(message)
            sys.exit()
        elif opt in ("-i", "--slowlogFile"):
            slowlogFile = arg
            print(slowlogFile)
        elif opt in ("-s", "--sec"):
            runtime = arg
        elif opt in ("-r", "--run"):
            run=True

    if slowlogFile:
        readC.parseSlowLog(slowlogFile)
        readC.cleanDB()
    elif run:
        t_end = time.time() + int(runtime)
        while time.time() < t_end:
            run = createWorkingSet()
            numberofthreads=2
            queries=run.buildWorkingSet()
            start_threading(queries,numberofthreads)
        logging.info(f'Finally Number of Queries: {counter.val.value}')




if __name__== "__main__":
    runmain=runMain()
    manager = Manager()
    counter = Counter(manager)
    readC = readData()
    finger = fingerprint()

    #try:
    con = db_connect()
    cur = con.cursor()

    mysqlcon = mysql_connect("127.0.0.1",18923,"msandbox","msandbox","sysbench")
    mysqlcur = mysqlcon.cursor()

    #except:
    #    logging.info("Could not connect to the DB!")
    #    sys.exit(2)
    main(sys.argv[1:])

