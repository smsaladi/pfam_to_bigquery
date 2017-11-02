"""

python split_table.py architecture.txt.gz

"""

import os.path
import argparse

import dask.dataframe as dd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('table_txt_gz')
    parser.add_argument('--size_cutoff', default=3.8e9)

    args = parser.parse_args()

    # 50 MB block size
    df = dd.read_table(args.table_txt_gz, blocksize=50000000,
                       sep='\t', header=None, encoding='latin-1')

    base, ext = os.path.splitext(args.table_txt_gz)

    df.to_csv(base + '_part*.txt', header=False, index=False, sep='\t')
    return


if __name__ == '__main__':
    main()
