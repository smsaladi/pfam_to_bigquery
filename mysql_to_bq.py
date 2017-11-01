"""
"""

from __future__ import print_function

import re
import json
from collections import OrderedDict

import argparse


class SQLField():
    sql2bq = {
        'int': 'INTEGER',
        'tinyint': 'INTEGER',
        'smallint': 'INTEGER',
        'mediumint': 'INTEGER',
        'bigint': 'INTEGER',

        'float': 'FLOAT',
        'double': 'FLOAT',
        'decimal': 'FLOAT',

        'char': 'STRING',
        'varchar': 'STRING',
        'text': 'STRING',
        'tinytext': 'STRING',
        'mediumtext': 'STRING',
        'longtext': 'STRING',

        'timestamp': 'TIMESTAMP',
        'datetime': 'DATETIME',
        'date': 'DATE',

        'blob': 'BYTES',
        'mediumblob': 'BYTES',
        'longblob': 'BYTES',
        'enum': 'INTEGER'
        }

    def __init__(self, sqltype, nullable):
        self.sqltype = sqltype
        self.nullable = nullable
        return

    @property
    def mode(self):
        if self.nullable:
            return 'NULLABLE'
        else:
            return 'REQUIRED'

    @property
    def bqtype(self):
        raw = self.sqltype.split('(')[0]
        return self.__class__.sql2bq[raw]


class SQLTable():
    re_name = re.compile('CREATE TABLE `(.*?)` \(')
    re_contents = re.compile('\((.*)\)', flags=re.MULTILINE | re.DOTALL)

    def __init__(self, name, fields):
        self.name = name
        self.fields = fields


    @classmethod
    def from_mysql_create(cls, s):
        fields = OrderedDict()

        contents = cls.re_contents.findall(s)[0].replace('\n', '')

        for line in re.split(",\s+", contents, flags=re.MULTILINE):
            line = line.rstrip().lstrip()
            tokens = line.split()

            if "`" in tokens[0]:
                name, sqltype, *opts = tokens

                name = name.replace('`', '')

                if 'NOT NULL' in line:
                    nullable = False
                else:
                    nullable = True

                fields[name] = SQLField(sqltype, nullable)

            elif line.startswith('PRIMARY KEY') or tokens[0] == "KEY":
                name = tokens[2].split('`')[1]
                fields[name].req = True

        return cls(cls.re_name.findall(s)[0], fields)


    def bq_json(self):
        to_print = []
        for k, v in self.fields.items():
            to_print.append({'name': k,
                             'type': v.bqtype,
                             'mode': v.mode})

        return json.dumps(to_print, indent=2)


def custom_pfam_mods(tbl):
    if tbl.name in ["pfamseq", "pfamseq_antifam", "uniprot"]:
       tbl.fields['sequence'].sqltype = 'longtext'
    return tbl


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mysql_file')

    args = parser.parse_args()

    with open(args.mysql_file, 'r') as fh:
        text = fh.read()

    stmts = re.findall('(CREATE TABLE.*?);', text,
                       flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)

    if len(stmts) == 0:
        raise ValueError("No CREATE statement found")

    for s in stmts:
        tbl = SQLTable.from_mysql_create(s)
        tbl = custom_pfam_mods(tbl)
        print(tbl.bq_json())

    return

if __name__ == '__main__':
    main()
