"""

python format_gc_tsv.py pfamtxt.size.txt md5_checksums

"""

import os.path
import argparse

import base64

import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sizes')
    parser.add_argument('md5')
    parser.add_argument('--loc',
        default="http://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/database_files/")

    args = parser.parse_args()

    df_sizes = pd.read_table(args.sizes, sep='\s+', header=None,
                             usecols=[4, 8],
                             index_col=1, names=['size', 'name'])
    df_check = pd.read_csv(args.md5, sep='\s+', header=None,
                           index_col=1, names=['check'])

    df_join = pd.merge(df_sizes, df_check, how='left',
                       left_index=True, right_index=True)

    print('TsvHttpData-1.0')
    for ind, row in df_join.iterrows():
        location = os.path.join(args.loc, ind)
        size = row['size']

        check_b64 = base64.b64encode(bytes.fromhex(row['check']))

        print(location, size, check_b64.decode(), sep='\t')

    return



if __name__ == '__main__':
    main()
