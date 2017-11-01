## Import pfam to Google Bigquery

export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

sudo apt-get update
sudo apt-get --assume-yes install htop lftp gcsfuse tmux less parallel

mkdir -p pfam31 pfamsql

# mount cloud store
gcsfuse pfam31 ./pfam31/

lftp ftp.ebi.ac.uk <<EOF

mirror --include-glob="*.sql.gz" --exclude-glob="*.innodb.sql.gz" --parallel=20 /pub/databases/Pfam/current_release/database_files/ ~/pfamsql
mirror --include-glob="*.txt.gz" --continue --parallel=10 /pub/databases/Pfam/current_release/database_files/ ~/pfam31/

EOF


## can be done locally
## convert sql into google bigquery format
gzip -d pfamsql/*.sql.gz

mkdir pfambq
ls pfamsql/*.sql | parallel "python mysql_to_bq.py {} > pfambq/{/.}.json"

# load into bq
# https://cloud.google.com/bigquery/bq-command-line-tool
ls pfambq/*.json | parallel -j1 'echo {}; bq --project "pfamdb" --nosync load --source_format=CSV --field_delimiter="\t" --encoding="ISO-8859-1" --null_marker="NULL" --replace pfam31.{/.} gs://pfam31/{/.}.txt.gz {}'

# 4 gb max file size

# use google cloud utility to download into bucket

# get sizes
lftp ftp.ebi.ac.uk > pfamtxt.size.txt <<EOF
ls /pub/databases/Pfam/current_release/database_files/*.txt.gz

EOF

# get md5 checksums
lftp ftp.ebi.ac.uk <<EOF
get /pub/databases/Pfam/current_release/database_files/md5_checksums

EOF

# make file for google cloud download
python format_gc_tsv.py pfamtxt.size.txt md5_checksums > pfam_gc_dl.tsv
