#!/usr/bin/env python
# coding: utf-8

import sys
import getopt
import os
import logging
import re
import hashlib
import csv
import json
from json import dumps, loads, JSONEncoder, JSONDecoder
import pickle

### configure logging
level = logging.DEBUG
format = '%(asctime)s - %(message)s'
handlers = [logging.FileHandler('test.log'), logging.StreamHandler()]
logging.basicConfig(level=level, format=format, handlers=handlers)


class fingerprint():

    def __init__(self):
        self.final_dict = {}



    def write_file(self, filename, data_to_csv):
        with open(filename, mode='a') as csv_file:
            #fieldnames = ['trxid', 'query']
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # writer.writeheader()
            writer.writerow(data_to_csv)

            # for key, value in data.items():
            #    writer.writerow(value)

    def createFingerprint(self, trxid, hash, query, result_csv, debug, trxid_seq):
        variable_counter = 1
        myvalues = {}
        self.trxid_seq = trxid_seq
        self.hash = hash
        self.query = ' '.join(query.split())
        self.result_csv = result_csv
        self.origingal_query = self.query
        self.trxid = trxid
        self.debug = debug
        self.new_hash = False

        if self.debug:
            logging.debug(self.query )

        # Between-And
        #query_between = re.search(r'(?i)(WHERE(.*))BETWEEN(.*)AND(\'|\")?(\s\S*)(\'|\")?((;| )(.*))$', query)
        query_between = re.search(r'(?i)(WHERE(.*))BETWEEN (.*) AND ([\d]+|(\'|\")(.*)(\'|\"))(.*)$', self.query)
        if query_between:
            # if value starts with a number and contains - or : it is a date
            # we generate a different hash for that
            if re.match(r"^\"\d+[-:]", query_between.group(4)):
                self.new_hash = True

            # we are using two sub to have different variable counters
            self.query = re.sub(r'(?i)(WHERE(.*))BETWEEN(.*)AND',
                           rf'\1BETWEEN <v{variable_counter}> AND {query_between.group(4)} ', self.query)
            if f'v{variable_counter}' not in myvalues:
                myvalues[f'v{variable_counter}'] = set()
                myvalues[f'v{variable_counter}'].add(query_between.group(3))
                variable_counter += 1

            self.query = re.sub(r'(?i)(WHERE(.*))BETWEEN(.*)AND(.*)',
                           rf'\1 BETWEEN \3 AND <v{variable_counter}> {query_between.group(8)}',
                           self.query)
            if f'v{variable_counter}' not in myvalues:
                myvalues[f'v{variable_counter}'] = set()
                myvalues[f'v{variable_counter}'].add(query_between.group(4))
                variable_counter += 1

        # If it is an INSERT|REPLACE INTO VALUES
        query_matches = re.search(r'(?i)(INSERT|REPLACE) INTO', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'(?i)(VALUES).\((.*)\)', self.query)
                if query_matches:
                    self.query = re.sub(r'(?i)(VALUES).\((.*)\)', rf'\1 <v{variable_counter}>', self.query, 1)

                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(2))
                    variable_counter += 1
                else:
                    break

        # If it is an SELECT ... IN ()
        query_matches = re.search(r'(?i)WHERE(.*?)IN\s+\((.*?)\)', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'(?i)(IN)\s+\((.*?)\)', self.query)
                if query_matches:
                    self.query = re.sub(r'(?i)(IN)\s+(\()(.*?)(\))(\)+)?(.*?)', rf'\1 <v{variable_counter}>\5 \6', self.query, 1)
#                    query = re.sub(r'(?i)(IN).*?\((.*?)\)(.*)', rf'\1 <v{variable_counter}>\3', query, 1)

                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(2))
                    variable_counter += 1
                else:
                    break

        query_matches = re.search(r'(?i)^update.(.*).(SET)\s(.*?)((\swhere|;)(.*))', self.query)
        if query_matches:
            setlist = re.search(r'(?i)(.*SET\s*)(.*?)(;|where.*|$)', self.query)
            set_split = list(finger.mysplit(setlist.group(2)))
            # Using enumerate()
            for v, i in enumerate(set_split):
                item = i.split('=', 1)
                #if values is equal with the variable plus an intiger, that's incrementing so we do not change it
                isincrement = re.match(rf'{item[0]} *\+ *\d', item[1])
                if isincrement:
                    set_split[v] = item[0] + "=" + item[1]

                else:
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(item[1])
                    item[1] = f'<v{variable_counter}>'
                    set_split[v] = item[0] + "=" + item[1]
                    variable_counter += 1

            self.query = ('{}{} {}'.format(
                 setlist.group(1),
                 ', '.join(set_split),
                 setlist.group(3)
             ))

                # pos = 0
            # exp = re.compile(r"""(['"]?)(.*?)\1(,|$)""")
            # while True:
            #     m = exp.search(list.group(2), pos)
            #     result = m.group(2)
            #     separator = m.group(3)
            #     print(f"res: {result}")
            #     print(f"sep: {separator}")
            #     item = (result.split("="))
            #     #if values is equal with the variable plus an intiger, that's incrementing so we do not change it
            #     isincrement = re.match(rf'{item[0]} *\+ *\d', item[1])
            #     if isincrement:
            #         print(isincrement)
            #         #set_list[v] = item[0] + "=" + item[1]
            #
            #     else:
            #         if f'v{variable_counter}' not in myvalues:
            #             myvalues[f'v{variable_counter}'] = set()
            #         myvalues[f'v{variable_counter}'].add(item[1])
            #         item[1] = f'<v{variable_counter}>'
            #         #set_list[v] = item[0] + "=" + item[1]
            #         variable_counter += 1
            #     if not separator:
            #         break
            #
            #     pos = m.end(0)

            #set_list = list.group(2).split(",")
            #print(set_list)
            # read values and split them
            #for v, i in enumerate(set_list):
            #    print(v,i)
                #item = (i.split("="))
                # if values is equal with the variable plus an intiger, that's incrementing so we do not change it
                # isincrement = re.match(rf'{item[0]} *\+ *\d', item[1])
                # if isincrement:
                #     set_list[v] = item[0] + "=" + item[1]
                #
                # else:
                #     if f'v{variable_counter}' not in myvalues:
                #         myvalues[f'v{variable_counter}'] = set()
                #     myvalues[f'v{variable_counter}'].add(item[1])
                #     item[1] = f'<v{variable_counter}>'
                #     set_list[v] = item[0] + "=" + item[1]
                #     variable_counter += 1


            # self.query = ('{}{} {}'.format(
            #     list.group(1),
            #     ', '.join(set_list),
            #     list.group(3)
            # ))

        # where values is a simple number
        query_matches = re.search(r'(=|<>|>|<)(\s+)?([\d]+)', self.query)
        # if it matches we go trough the whole line and replace all matches but we increase the counter as well.
        # SELECT c FROM sbtest1 WHERE id=23 and bela=29 and lajos="fsdfsf";
        # SELECT c FROM sbtest1 WHERE id=<v1> and bela=<v2> and lajos=v3;
        if query_matches:
            while True:
                query_matches = re.search(r'(=|<>|>|<)(\s+)?([\d]+)', self.query)
                if query_matches:
                    self.query = re.sub(r'(=|<>|>|<)(\s+)?([\d]+)', rf'\1 <v{variable_counter}>', self.query, 1)
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(3))
                    variable_counter += 1
                else:
                    break

        # everything between '' and ""
        query_matches = re.search(r'(\'|\")(.*?)(\'|\")', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'(\'|\")(.*?)(\'|\")', self.query)
                if query_matches:
                    self.query = re.sub(r'(\'|\")(.*?)(\'|\")', rf'<v{variable_counter}>', self.query,1)
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(2))
                    variable_counter += 1
                else:
                    break

        if self.debug:
            logging.debug(self.query)

        # if there is a date in between , we generate a different hash
        if self.new_hash:
            self.query = self.query + "date"
            hash_obj = hashlib.md5(self.query.encode())
        else:
            hash_obj = hashlib.md5(self.query.encode())

        if hash_obj.hexdigest() not in self.final_dict:
            self.temp_dict = {}
            self.temp_dict = {hash_obj.hexdigest(): {
                'trxid': {0},
                'trxid_seq': {0},
                'orig_hash': {0},
                'orig_query': {None},
                'figerprint' : {None},
                'values' : {None}
            }}
            self.temp_dict[hash_obj.hexdigest()]['trxid'] = self.trxid
            self.temp_dict[hash_obj.hexdigest()]['trxid_seq'] = self.trxid_seq
            self.temp_dict[hash_obj.hexdigest()]['orig_hash'] = self.hash
            self.temp_dict[hash_obj.hexdigest()]['orig_query'] = self.origingal_query
            self.temp_dict[hash_obj.hexdigest()]['figerprint'] = self.query
            self.temp_dict[hash_obj.hexdigest()]['values'] = myvalues
            self.final_dict.update(self.temp_dict)
        else:
            self.temp_dict = {}
            self.temp_dict = {hash_obj.hexdigest(): {
                'trxid': {0},
                'trxid_seq': {0},
                'orig_hash': {0},
                'orig_query': {None},
                'figerprint' : {None},
                'values' : {None}
            }}
            self.temp_dict[hash_obj.hexdigest()]['trxid'] = self.trxid
            self.temp_dict[hash_obj.hexdigest()]['trxid_seq'] = self.trxid_seq
            self.temp_dict[hash_obj.hexdigest()]['orig_hash'] = self.hash
            self.temp_dict[hash_obj.hexdigest()]['orig_query'] = self.origingal_query
            self.temp_dict[hash_obj.hexdigest()]['figerprint'] = self.query
            self.temp_dict[hash_obj.hexdigest()]['values'] = myvalues
            finger.merge_dict(self.final_dict, self.temp_dict)

        # hash_obj = hashlib.md5(query.encode())
        data = []
        data.append(self.trxid)
        data.append(self.trxid_seq)
        data.append(self.hash)
        data.append(hash_obj.hexdigest())
        data.append(self.origingal_query)
        data.append(self.query)
        data.append(myvalues)
        finger.write_file(result_csv, data)
        return self.query

    def save_dict(self, dict):
        self.dict = dict
        for key in dict.keys():

            data_csv = []
            data_csv.append(key)
            data_csv.append(self.dict[key]['trxid'])
            data_csv.append(self.dict[key]['trxid_seq'])
            data_csv.append(self.dict[key]['orig_query'])
            data_csv.append(self.dict[key]['figerprint'])
            data_csv.append(self.dict[key]['values'])
            finger.write_file("queries.grouped.csv", data_csv)

    def mysplit(self,text):
        in_quotes = False
        fragment = ''
        escape = False
        for ch in text:
            if "\\" in ch:
                escape = True
            elif ch in ("'", '"') and escape:
                fragment += ch
                escape = False
            elif ch == ',' and not in_quotes:
                yield fragment
                fragment = ''
                escape = False
            else:
                fragment += ch
                if ch == in_quotes:
                    in_quotes = False
                    escape = False
                elif ch in ("'", '"'):
                    in_quotes = ch
                    escape = False
        yield fragment

    def merge_dict(self,dict1, dict2):
        for k in dict2:
            for v in dict2[k]['values']:
                dict1[k]['values'][v].update(dict2[k]['values'][v])

    def write_dict(self):
        with open('pyload_query_dictionary.csv', 'w', newline="") as csv_file:
            writer = csv.writer(csv_file)
            for key, value in self.final_dict.items():
                writer.writerow([key, value])

    def get_dic(self):
        return(self.final_dict)

class readData():

    def __init__(self):
        # finding all queries and they could start with a comment as well
        self.regex = "(?i)^(\/\*(.*)\*\/)?( |)(select|update|insert|delete|replace)"
        self.splitstring = "# Time: "
        self.trx_order_id = 0
        self.count = 1
        self.query_values = ""
        self.trxid = 0
        self.re_trx_id = re.compile(r'^# InnoDB_trx_id: ([0-9]+)')
        self.stop_list = "(?i)^(set timestamp=|SELECT @@version|SHOW GLOBAL STATUS|use(.*);|set session |set global)"

    def openFile(self, slowlogFile, debug):
        self.debug = debug
        if os.path.exists(slowlogFile):
            with open(slowlogFile, 'r') as f:
                try:
                    if self.debug:
                        logging.debug(f"{slowlogFile} is opend.")
                    filecontent = f.readlines()
                    return filecontent

                except:
                    logging.critical("Could not Open the file!")
        else:
            if debug:
                if self.debug:
                    logging.info("Slow log file does not exists!")

    def parseSlowLog(self, slowlogFile, debug):
        # read the file into filecontent
        self.slow_dictionary = {}
        self.debug = debug
        self.i = 1
        filecontent = readC.openFile(slowlogFile, self.debug)
        # find first # Time: 2020-01-21T16:55:13.249612Z
        #for lines in readC.filter_output_between_strings(filecontent, self.splitstring):
        for lines in readC.get_queries(filecontent):
            self.trxid = lines[0]
                #if trxid != "0":
                #    logging.info(f"This is part of a transaction: {trxid}")

            if re.match(self.regex, lines[1]) is not None:
                query = lines[1]
                query_text = query
                if re.match(r'(?i)(\/\*(.*)\*\/)', query):
                    query_text = re.sub(r'(?i)(\/\*(.*)\*\/)', rf'', query)
                hash_obj = hashlib.md5(query_text.encode())
                if hash_obj.hexdigest() not in self.slow_dictionary:
                    temp_dict = {hash_obj.hexdigest(): {
                        'trxid': {0},
                        'trxid_seq': {0},
                        'orig_query': {None}
                    }}
                    temp_dict[hash_obj.hexdigest()]['trxid'] = self.trxid
                    temp_dict[hash_obj.hexdigest()]['trxid_seq'] = self.i
                    temp_dict[hash_obj.hexdigest()]['orig_query'] = query_text
                    self.slow_dictionary.update(temp_dict)
            self.i += 1

        return(self.slow_dictionary)


    def filter_output_between_strings(self, listLog, separator):
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

    def get_queries(self, filecontent):
        query = []
        trx_id = None
        for line in filecontent:
            if line.startswith('# Time:') or line.startswith('# User@Host:'):
                if query:
                    yield trx_id, ' '.join(query)
                query = []
            elif line.startswith('# InnoDB_trx_id'):
                m = self.re_trx_id.match(line)
                trx_id = int(m.group(1))
            #elif line.startswith('#') or line.lower().startswith('set timestamp'):
            elif line.startswith('#') or re.match(self.stop_list, line):
                continue
            else:
                query.append(line.strip())
        yield trx_id, ' '.join(query)

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def main(argv):
    runtime = 10
    slowlogFile = ''
    debug = 0
    message = """Usage:
    Process SlowLog File: pyLoad.py -i <slowlogFile>
    """
    try:
        opts, args = getopt.getopt(argv, "hi:d", ["slowlogFile=", "debug"])
    except getopt.GetoptError:
        print(message)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(message)
            sys.exit()
        elif opt in ("-i", "--slowlogFile"):
            slowlogFile = arg
        elif opt in ("-d", "--debug"):
            debug = 1

    if slowlogFile:
        result_csv = "queries.csv"
        queries_dcitionary = readC.parseSlowLog(slowlogFile, debug)
        line_count = 0

        for k in queries_dcitionary.keys():
            finger.createFingerprint(trxid=queries_dcitionary[k]['trxid'], trxid_seq=queries_dcitionary[k]['trxid_seq'], query=queries_dcitionary[k]['orig_query'], hash=k, result_csv=result_csv, debug=debug)

        finger.save_dict(finger.get_dic())
        data=json.dumps(finger.get_dic(), cls=SetEncoder)
        #j = dumps(finger.get_dic(), cls=PythonObjectEncoder)
        #finger.write_dict()
        with open("pyload_query_dictionary.json", "w") as json_file:
            json.dump(data, json_file)
        #print(finger.get_dic())

        # for row in b:
        #    if line_count != 0:
        #        finger.createFingerprint(row['query'])
        #        line_count +=1
        # readC.parseSlowLog(slowlogFile)
        # readC.cleanDB()
        # data={"q1": {'id': 'John Smith', 'name': 'Accounting', 'count': 'November'},
        # "q2":{'id': 'BJohn Smith', 'name': 'Accounting', 'count': 'November'}}


if __name__ == "__main__":

    readC = readData()
    finger = fingerprint()

    main(sys.argv[1:])
