#!/usr/bin/env python
# coding: utf-8

import sys
import getopt
import os
import logging
import re
import hashlib
import csv
from template import lua_templates


### configure logging
level = logging.DEBUG
format = '%(asctime)s - %(message)s'
handlers = [logging.FileHandler('test.log'), logging.StreamHandler()]
logging.basicConfig(level=level, format=format, handlers=handlers)


class fingerprint():
    """
    Creating Finger Prints from the queries.
    Parsing all queries and removes the variables and replaces them with <v1>, <v2> etc...
    """

    # def __init__(self):
    #     self.final_dict = {}

    def write_file(self, filename, data_to_csv):
        """
        This function is writing data to CSV file.

        :param filename: output filename, where to write the csv file
        :param data_to_csv: the data what you want to write down.
        """
        file_exists = os.path.isfile(filename)
        with open(filename, mode='a') as csv_file:
            fieldnames = ['thread_id', 'sequence', 'trx_number' , 'finger_hash', 'finger_hash_trx', 'orig_query', 'fingerprint', 'values' ]
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if not file_exists:
                writer.writerow(fieldnames)
            #writer.writerow(fieldnames)
            writer.writerow(data_to_csv)

   # def isdigit(self , string):
   #     return bool(re.search(r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)', string))

    def createFingerprint(self, thread_id, hash, query, result_csv, debug, sequence):
        """
        The actual function which is creating the query finger prints.
        There is a lot of regexp and parsing happening in this function.
        Tried to prepare for all queries but if there is an issue most of the time you have to look around in this section.

        :param thread_id: The thread_id of the query.
        :param hash: The hash of the query.
        :param query: The original query itself.
        :param result_csv: The file where to write the result.
        :param debug: Debug,yes or no.
        :param sequence: This number is incrementing, every query has a unique number. We know the order of teh queries in the trx.
        :return:
        """
        variable_counter = 1
        myvalues = {}

        self.sequence = sequence
        self.hash = hash
        self.query = ' '.join(query.split())
        self.result_csv = result_csv
        self.origingal_query = self.query
        self.thread_id = thread_id
        self.debug = debug
        self.new_hash = False
        self.hash_query_list = 0
        self.trx_number = 0

        if self.debug:
            logging.debug(self.query)

        # Between-And
        query_between = re.search(r'(?i)(WHERE(.*))BETWEEN (\'|\")?(.*?)(\'|\")? AND ([\d]+|(\'|\")(.*?)(\'|\"))(.*)$', self.query)

        if query_between:
            # if value starts with a number and contains - or : if it is a date
            # we generate a different hash for that
            if re.match(r"^\"\d+[-:]", query_between.group(4)):
                 self.new_hash = True

            # we are using two sub to have different variable counters for the values
            # example `between 5 and 10` - `between $<v1> and $<v2>`
            # In one query there could be multiple BETWEEN conditions. First we go trough all of them and change the first part.
            # Example: Between $<v1> and 10
            # Then we go trough on all of them again and we change the second part
            # Example: Between $<v1> and $<v2>
            while True:
                between_first = re.search(r'(?i)(WHERE(.*?))BETWEEN\s+(\'|\"|\s)?(?!\$<v\d+>)(.*?)(\'|\"|\s+)?\s+AND(.*?)',self.query)
                if between_first:
                    self.query = re.sub(r'(?i)(WHERE(.*?))BETWEEN\s+(\'|\"|\s)?(?!\$<v\d+>)(.*?)(\'|\"|\s+)?\s+AND(.*?)', rf'\1 BETWEEN $<v{variable_counter}> AND', self.query,1)
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(between_first.group(4))
                    variable_counter += 1
                else:
                    break

            while True:
                between_first = re.search(r'(?i)between\s+(\$<v\d+>)\s+and\s+(?!\$<v\d+>)(.*?)(\s+|;)',self.query)
                if between_first:
                        self.query = re.sub(r'(?i)between\s+(\$<v\d+>)\s+and\s+(?!\$<v\d+>)(.*?)(\s+|;)', rf'BETWEEN \1 AND $<v{variable_counter}>\3', self.query,1)
                        if f'v{variable_counter}' not in myvalues:
                            myvalues[f'v{variable_counter}'] = set()
                        myvalues[f'v{variable_counter}'].add(between_first.group(2))
                        variable_counter += 1
                else:
                    break

        # If it is an INSERT|REPLACE INTO VALUES
        query_matches = re.search(r'(?i)(INSERT|REPLACE)(\s+)?(ignore)?(\s+)?(INTO)?', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'(?i)(VALUES).\(((?!\$<v\d+>).*)\)', self.query)
                if query_matches:
                    self.query = re.sub(r'(?i)(VALUES).\(((?!\$<v\d+>).*)\)', rf'\1 ($<v{variable_counter}>)', self.query, 1)

                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(2))
                    variable_counter += 1
                else:
                    break

        # If it is an SELECT ... IN ()
        query_matches = re.search(r'(?i)(IN)(\s+)?(\()(?!\$<v\d+>)([^select].*?)(\))(\)+)?(.*?)', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'(?i)(IN)(\s+)?(\()(?!\$<v\d+>)([^select].*?)(\))(\)+)?(.*?)', self.query)
                if query_matches:
                    self.query = re.sub(r'(?i)(IN)(\s+)?(\()(?!\$<v\d+>)([^select].*?)(\))(\)+)?(.*?)', f'\1 IN ($<v{variable_counter}>)\7',
                                        self.query, 1)

                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(4))
                    variable_counter += 1
                else:
                    break

        # everything between '' and ""
        query_matches = re.search(r'([\"\'])((?:\\\1|.)*?)\1', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'([\"\'])((?:\\\1|.)*?)\1', self.query)
                if query_matches:
                    self.query = re.sub(r'([\"\'])((?:\\\1|.)*?)\1', rf'$<v{variable_counter}>', self.query, 1)
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(2))
                    variable_counter += 1
                else:
                    break

        # where values is a simple number
        query_matches = re.search(r'(=|<>|=>|<=|>=|=<|<|>)(\s+)?(-)?([\d]+(\.[\d]+)?)', self.query)
        # if it matches we go trough the whole line and replace all matches but we increase the counter as well.
        # SELECT c FROM sbtest1 WHERE id=23 and bela=29 and lajos="fsdfsf";
        # SELECT c FROM sbtest1 WHERE id=<v1> and bela=<v2> and lajos=v3;
        if query_matches:
            while True:
                query_matches = re.search(r'(=|<>|=>|<=|>=|=<|<|>)(\s+)?(-)?([\d]+(\.[\d]+)?)', self.query)
                if query_matches:
                    self.query = re.sub(r'(=|<>|=>|<=|>=|=<|<|>)(\s+)?(-)?([\d]+(\.[\d]+)?)', rf'\1 $<v{variable_counter}>',
                                        self.query, 1)
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()

                    if query_matches.group(4).isdigit():
                        myvalues[f'v{variable_counter}'].add(int(query_matches.group(4)))
                    else:
                        myvalues[f'v{variable_counter}'].add(float(query_matches.group(4)))
                    variable_counter += 1
                else:
                    break

        # Queries which contain interval
        query_matches = re.search(r'(?i)=(\s*)INTERVAL(\s*)\((?!\$<v\d+>)(.*?)\)', self.query)
        if query_matches:
            while True:
                query_matches = re.search(r'(?i)=(\s*)INTERVAL(\s*)\((?!\$<v\d+>)(.*?)\)', self.query)
                if query_matches:
                    self.query = re.sub(r'(?i)=(\s*)INTERVAL(\s*)\((?!\$<v\d+>)(.*?)\)',rf'= INTERVAL($<v{variable_counter}>)',self.query, 1)
                    if f'v{variable_counter}' not in myvalues:
                        myvalues[f'v{variable_counter}'] = set()
                    myvalues[f'v{variable_counter}'].add(query_matches.group(3))
                    variable_counter += 1
                else:
                    break



        if self.debug:
            logging.debug(self.query)
        # we generate a hash we add trx_id to the query as well because the same query can be part of multiple transactions as well.
        # with this they will have a different hash but queries outside of a transactions trx_id=0 will have the same hash
        # if there is a date in between , we generate a different hash
        if self.new_hash:
            self.hash_query = self.query + str(self.sequence) + "date"
            self.hash_obj_thread = hashlib.md5(self.hash_query.encode()).hexdigest()
            self.hash_finger = self.query + "date"
            self.hash_obj = hashlib.md5(self.hash_finger.encode()).hexdigest()
        else:
            self.hash_query = self.query + str(self.sequence)
            self.hash_obj_thread = hashlib.md5(self.hash_query.encode()).hexdigest()
            self.hash_finger = self.query
            self.hash_obj = hashlib.md5(self.hash_finger.encode()).hexdigest()

        self.temp_dict = {}
        self.temp_dict = {self.thread_id: {
            self.hash_obj: {
                'thread_id': {0},
                'sequence': {0},
                'trx_number': {0},
                'orig_hash': {0},
                'orig_query': {None},
                'finger_hash': {0},
                'finger_hash_trx': {0},
                'fingerprint': {None},
                'hash_query_list': {0},
                'values': {None}
            }
        }}
        self.temp_dict[self.thread_id][self.hash_obj]['thread_id'] = self.thread_id
        self.temp_dict[self.thread_id][self.hash_obj]['sequence'] = self.sequence
        self.temp_dict[self.thread_id][self.hash_obj]['trx_number'] = self.trx_number
        self.temp_dict[self.thread_id][self.hash_obj]['orig_hash'] = self.hash
        self.temp_dict[self.thread_id][self.hash_obj]['orig_query'] = self.origingal_query
        self.temp_dict[self.thread_id][self.hash_obj]['finger_hash'] = self.hash_obj
        self.temp_dict[self.thread_id][self.hash_obj]['finger_hash_trx'] = self.hash_obj_thread
        self.temp_dict[self.thread_id][self.hash_obj]['fingerprint'] = self.query
        self.temp_dict[self.thread_id][self.hash_obj]['hash_query_list'] = self.hash_query_list
        self.temp_dict[self.thread_id][self.hash_obj]['values'] = myvalues

        # hash_obj = hashlib.md5(query.encode())
        data = []
        data.append(self.thread_id)
        data.append(self.sequence)
        data.append(self.trx_number)
        data.append(self.hash)
        data.append(self.hash_obj)
        data.append(self.hash_obj_thread)
        data.append(self.origingal_query)
        data.append(self.query)
        data.append(myvalues)
        finger.write_file(result_csv, data)
        return self.temp_dict

    def save_dict(self, dict, file ):
        """
        Saving a dictionary to CSV.
        :param dict: The Dictionary.
        :param file: The filename.
        """
        self.dict = dict
        for trx_hash in dict.keys():
            for finger_hash in dict[trx_hash].keys():
                data_csv = []
                # data_csv.append(thread_id)
                data_csv.append(self.dict[trx_hash][finger_hash]['thread_id'])
                data_csv.append(self.dict[trx_hash][finger_hash]['sequence'])
                data_csv.append(self.dict[trx_hash][finger_hash]['trx_number'])
                data_csv.append(self.dict[trx_hash][finger_hash]['finger_hash'])
                data_csv.append(self.dict[trx_hash][finger_hash]['finger_hash_trx'])
                data_csv.append(self.dict[trx_hash][finger_hash]['orig_query'])
                data_csv.append(self.dict[trx_hash][finger_hash]['fingerprint'])
                data_csv.append(self.dict[trx_hash][finger_hash]['values'])
                finger.write_file(file, data_csv)


class readData():

    def __init__(self):
        # finding all queries and they could start with a comment as well
        self.regex = "(?i)^(\/\*(.*)\*\/)?( |)(select|update|insert|delete|replace|commit|begin|start transaction|SET autocommit)"
        self.splitstring = "# Time: "
        self.trx_order_id = 0
        self.count = 1
        self.query_values = ""
        self.thread_id = 0
        self.re_thread_id = re.compile(r'^# Thread_id: (\d+)')
        self.re_thread_id_userhost = re.compile(r'^# User@Host: (.*) Id:\s+(\d+)')

        self.stop_list = "(?i)^(set timestamp=|SELECT @@version|SHOW GLOBAL STATUS|use(.*);|set session |set global)"

    def openFile(self, slowlogFile, debug):
        self.debug = debug
        if os.path.exists(slowlogFile):
            # encoding because we need escape characters as well
            with open(slowlogFile, 'r', encoding='raw_unicode_escape') as f:
                try:
                    if self.debug:
                        logging.debug(f"{slowlogFile} is opened.")
                    filecontent = [line for line in f.readlines() if line.strip()]
                    return filecontent

                except Exception as e:
                    logging.critical("Could not Open the file!")
                    print(e)
                    sys.exit()
        else:
            if debug:
                if self.debug:
                    logging.info("Slow log file does not exists!")

    def parseSlowLog(self, slowlogFile, debug):
        """
        Gives back the query , the tread ID and the sequence number in a dictionary.
        :param slowlogFile:
        :param debug:
        :return:
        """
        # read the file into filecontent
        self.slow_dictionary = {}
        self.debug = debug
        # we need i to be able to keep the sequence of the queries in a transaction
        # which query has higher seq number should be executed later
        self.i = 1
        filecontent = readC.openFile(slowlogFile, self.debug)
        # find first # Time: 2020-01-21T16:55:13.249612Z
        for lines in readC.get_queries(filecontent):
            self.thread_id = lines[0]

            if re.match(self.regex, lines[1]) is not None:
                query = lines[1]
                query_text = query
                if re.match(r'(?i)(\/\*(.*)\*\/)', query):
                    query_text = re.sub(r'(?i)(\/\*(.*)\*\/)', rf'', query)
                hash_text = query_text + str(self.i)
                hash_obj = hashlib.md5(hash_text.encode()).hexdigest()
                temp_dict = {hash_obj: {
                    'thread_id': {0},
                    'sequence': {0},
                    'orig_query': {None}
                }}
                temp_dict[hash_obj]['thread_id'] = self.thread_id
                temp_dict[hash_obj]['sequence'] = self.i
                temp_dict[hash_obj]['orig_query'] = query_text
                self.slow_dictionary.update(temp_dict)
            self.i += 1
        return (self.slow_dictionary)

    def get_queries(self, filecontent):
        """
        Reads the slow query log and collects the queries and the trhead ID.

        :param filecontent:
        :return:
        """
        query = []
        thread_id = 0
        for line in filecontent:
                # filter out mysqldump and pt-table-checksum queries
                if not re.search(r'(?i)(( from mysql\.[a-z]+)|(SELECT \/\*!40001 SQL_NO_CACHE \*\/ \* FROM)|(REPLACE INTO `percona`.`checksums` )|(FROM performance_schema\.[a-z]+)|(FROM information_schema\.[a-z]+))',
                                line):

                    if line.startswith('# Time:') or line.startswith('# User@Host:'):
                        if query:
                            yield thread_id, ' '.join(query)
                        if line.startswith('# User@Host:'):
                            m = self.re_thread_id_userhost.match(line)
                            if m:
                                thread_id = str(m.group(2))
                        query = []
                    elif line.startswith('# Thread_id:'):
                        m = self.re_thread_id.match(line)
                        thread_id = str(m.group(1))
                    elif line.startswith('#') or re.search(self.stop_list, line):
                        continue
                    else:
                        query.append(line.strip())

        yield thread_id, ' '.join(query)

class createSysbench():

    def __init__(self, final_dict):
        self.final_dict = final_dict


    def variables_lua(self):
        """

        :return:
        """
        self.query_list = []
        self.values_list = {}
        self.values_dict = {}
        self.values = {}

        for key in self.final_dict.keys():
            for f_hash in self.final_dict[key].keys():
                seq_number = self.final_dict[key][f_hash]['sequence']
                if self.final_dict[key][f_hash]['fingerprint'].lower() not in "begin; start transaction; SET autocommit = commit;".lower():
                    self.sysb_trx[seq_number] = {}
                    self.values_list[seq_number] = []
                    self.query_list.append(seq_number)
                    self.values = self.final_dict[key][f_hash]['values']
                    self.query = f"\"{self.final_dict[key][f_hash]['fingerprint']}\""
                    self.sysb_trx[seq_number] = self.values
                    self.values_dict[seq_number] = {}
                    for k in self.values.keys():
                        self.values_dict[seq_number].update({k: self.values[k]})
                    for match in re.finditer(r'(<v(\d+)>)', self.query):
                        if match:
                            self.query = f"{self.query} q{seq_number}_v{match.group(2)},"
                            self.values_list[seq_number].append(match.group(2))
                        else:
                            break

        return lua_templates.pyload_variables.render(queries=self.query_list, values_list=self.values_list,values=self.values_dict)


    def common_lua(self):
        """

        :return:
        """
        self.query_list = []
        self.values_list = {}

        for key in self.final_dict.keys():
            for f_hash in self.final_dict[key].keys():
                seq_number = self.final_dict[key][f_hash]['sequence']
                if self.final_dict[key][f_hash]['fingerprint'].lower() not in "begin; start transaction; SET autocommit = commit;".lower():
                    self.sysb_trx[seq_number] = {}
                    self.values_list[seq_number] = []
                    self.query_list.append(seq_number)
                    self.query = f"\"{self.final_dict[key][f_hash]['fingerprint']}\" % {{"
                    for match in re.finditer(r'(<v(\d+)>)', self.query):
                        if match:
                            self.query = f"{self.query} v{match.group(2)} = get_random(q{seq_number}_v{match.group(2)}),"
                            self.values_list[seq_number].append(match.group(2))
                        else:
                            break
                self.query = f"{self.query[:-1]} }}"
                self.sysb_trx[seq_number] = self.query
        return lua_templates.oltp_common.render(cmdline_queries=self.query_list, sysb_trx=self.sysb_trx,values_list=self.values_list)

    def execute_lua(self):
        """

        :return:
        """
        self.query_list = []
        self.query_execute = []
        self.sysb_trx = {}

        for key in self.final_dict.keys():
            for f_hash in self.final_dict[key].keys():
                seq_number = self.final_dict[key][f_hash]['sequence']

                if self.final_dict[key][f_hash]['trx_number'] == 0:
                    self.query_list.append(f"prepare_q{seq_number}()")
                    self.query_execute.append(f"execute_q{seq_number}()")
                else:
                    if self.final_dict[key][f_hash]['fingerprint'].lower() not in "begin; start transaction; SET autocommit = commit;".lower():
                        self.query_list.append(f"prepare_q{seq_number}()")
                        trx_n = self.final_dict[key][f_hash]['trx_number']
                        if trx_n not in self.sysb_trx:
                             self.sysb_trx[trx_n] = []
                        self.sysb_trx[trx_n].append(f"execute_q{seq_number}")

        return lua_templates.pyload_queries.render(prepare_queries=self.query_list, query_execute=self.query_execute,transaction_number=self.sysb_trx)

    def write_template(self,filename,data):
        f = open(filename,"w+")
        f.write(data)
        f.close()

def main(argv):
    slowlogFile = ''
    debug = 0
    testrun = 0
    final_dict = {}

    final_dict['0'] = {}
    tmp_dict = {}
    # dictionary which holds all the thread numbers and if a transaction is avtive or not in that thread
    thread_dict = {}

    trxID = 1
    message = """Usage:
    Process SlowLog File: pyLoad.py -i <slowlogFile>
    """
    try:
        opts, args = getopt.getopt(argv, "hi:do:t", ["slowlogFile=", "debug","outputFile=","testrun"])
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
        elif opt in ("-o", "--outputFile"):
            outputFile = arg
            if os.path.exists(outputFile):
                os.remove(outputFile)
        elif opt in ("-t", "--testrun"):
            testrun = 1



    """
    Reads all the queries and thread ID from the dictionary. 
    If there is `begin|start transaction|SET autocommit=0`, in that thread there is a transaction running.
    All the queries are in the same transaction in that thread until a `commit| set autocommit =1 | rollback` is not coming.
    If there is a query running in a thread but there was no star transaction in that case query is running outside of a transaction.
    """

    if testrun:
        slowlogFile = "test_file/test_file.source.log"
        result_csv = "test_file/tmp.log"
        if os.path.exists(result_csv):
            os.remove(result_csv)
        outputFile = result_csv
    else:
        result_csv = outputFile

    if slowlogFile:
        queries_dcitionary = readC.parseSlowLog(slowlogFile, debug)

        for k in queries_dcitionary.keys():

            thread_id = queries_dcitionary[k]['thread_id']

            # check if a transaction is starting or not
            if re.match(r"(?i)^(\s+)?(begin|start transaction|SET autocommit(\s+)?=(\s+)?0)(\s+)?;$",
                        queries_dcitionary[k]['orig_query']):
                # check if thread in the dictionary or not, if not add it
                if thread_id not in thread_dict:
                    thread_dict[thread_id] = {}
                # transaction is running
                if thread_id != 0:
                    thread_dict[thread_id].update({'trxRunning': True})
                    thread_dict[thread_id].update({'trxID': trxID})

                if debug:
                    logging.info(
                        f"{thread_id} - {thread_dict[thread_id]['trxRunning']} - {thread_dict[thread_id]['trxID']}")
                # adding the transaction start to the final dictionary
                result = (finger.createFingerprint(thread_id=thread_id, sequence=queries_dcitionary[k]['sequence'],
                                                   query=queries_dcitionary[k]['orig_query'], hash=k,
                                                   result_csv=result_csv, debug=debug))
                tmp_dict.update(result)
                trxID += 1

            # check other queries
            elif re.match("(?i)^(\/\*(.*)\*\/)?( |)(select|update|insert|delete|replace)",
                          queries_dcitionary[k]['orig_query']):
                result = finger.createFingerprint(thread_id=thread_id, sequence=queries_dcitionary[k]['sequence'],
                                                  query=queries_dcitionary[k]['orig_query'], hash=k,
                                                  result_csv=result_csv, debug=debug)

                # if it is not in the thread dictionary , adding
                if thread_id not in thread_dict:
                    thread_dict[thread_id] = {}

                # if it is a transaction
                if 'trxRunning' in thread_dict[thread_id]:
                    if thread_dict[thread_id]['trxRunning']:
                        if debug:
                            logging.info(f"Running in a transaction: {queries_dcitionary[k]['orig_query']}")
                        tmp_dict[thread_id].update(result[thread_id])
                else:
                    # transaction is not running in this thread
                    for hash in result[thread_id].keys():
                        # if hash already in dictionary with thread id 0 (no transaction), we merge values
                        if hash in final_dict['0'].keys():
                            for v in result[thread_id][hash]['values']:
                                final_dict['0'][hash]['values'][v].update(result[thread_id][hash]['values'][v])
                        # otherwise we just add to the dictionary
                        else:
                            final_dict['0'].update(result[thread_id])

            elif re.match(r"(?i)^(\s+)?(commit|rollback|SET autocommit(\s+)?=(\s+)?1)(\s+)?;$",
                          queries_dcitionary[k]['orig_query']):
                result = finger.createFingerprint(thread_id=thread_id, sequence=queries_dcitionary[k]['sequence'],
                                                  query=queries_dcitionary[k]['orig_query'], hash=k,
                                                  result_csv=result_csv, debug=debug)
                trx_query_list = []
                # if it is not in the thread dictionary , adding
                if thread_id not in thread_dict:
                    thread_dict[thread_id] = {}

                if 'trxRunning' in thread_dict[thread_id]:
                    if thread_dict[thread_id]['trxRunning']:
                        if debug:
                            logging.info(f"Commiting a Transaction: {queries_dcitionary[k]['orig_query']}")
                        tmp_dict[thread_id].update(result[thread_id])

                        for key in tmp_dict[thread_id].keys():
                            for trx_query_fingerprint in tmp_dict[thread_id][key]['fingerprint'].splitlines():
                                trx_query_list.append(trx_query_fingerprint)

                        # creating the hash
                        hash_query_list = hashlib.md5(str(trx_query_list).encode()).hexdigest()

                        if hash_query_list not in final_dict.keys():
                            final_dict[hash_query_list] = {}
                            for key in tmp_dict[thread_id].keys():
                                for finger_hash in tmp_dict[thread_id][key]['finger_hash'].splitlines():
                                    final_dict[hash_query_list][finger_hash] = {}
                                    final_dict[hash_query_list][finger_hash].update(tmp_dict[thread_id][finger_hash])
                                    final_dict[hash_query_list][finger_hash].update(
                                        {'trx_number': thread_dict[thread_id]['trxID']})
                            del tmp_dict[thread_id]
                            thread_dict[thread_id].update({'trxRunning': False})
                            thread_dict[thread_id].update({'trxID': '0'})
                        else:
                            for finger_hash in tmp_dict[thread_id].keys():
                                for v in tmp_dict[thread_id][finger_hash]['values']:
                                    final_dict[hash_query_list][finger_hash]['values'][v].update(
                                        tmp_dict[thread_id][finger_hash]['values'][v])
                            del tmp_dict[thread_id]
                            thread_dict[thread_id].update({'trxRunning': False})
                            thread_dict[thread_id].update({'trxID': '0'})

        outputFile = outputFile + ".tmp.csv"
        if os.path.exists(outputFile):
            os.remove(outputFile)
        finger.save_dict(final_dict, outputFile)

        SysB = createSysbench(final_dict)
        result_execute = SysB.execute_lua()
        SysB.write_template("run_pyload.lua", result_execute)

        result_common = (SysB.common_lua())
        SysB.write_template("run_common.lua", result_common)

        result_variables = (SysB.variables_lua())
        SysB.write_template("run_variables.lua", result_variables)




if __name__ == "__main__":
    readC = readData()
    finger = fingerprint()
    main(sys.argv[1:])
