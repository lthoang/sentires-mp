#!/usr/bin/bash
#SBATCH -c 128
#SBATCH --mem 503G
source ~/.bashrc
conda activate seer

# DATASET=electronic
# REVIEW_CORPUS=/data/shared/download/jmcauley/reviews_Electronics_5.json.gz

DATASET=$1
REVIEW_CORPUS=$2

echo $DATASET
echo $REVIEW_CORPUS

python -u etc/code/data_parser.py -i /data/shared/download/jmcauley/$REVIEW_CORPUS -o /data/shared/sentires/$DATASET
java -jar sentires.jar -t pre -c task/smu.$DATASET.task

mkdir /data/shared/sentires/$DATASET/select
mkdir /data/shared/sentires/$DATASET/pos

split -n l/5000 -a 10 /data/shared/sentires/$DATASET/$REVIEW_CORPUS.select /data/shared/sentires/$DATASET/select/seg

java -jar sentires.jar -t posdir -c task/smu.$DATASET.task

cat /data/shared/sentires/$DATASET/pos/* > /data/shared/sentires/$DATASET/$REVIEW_CORPUS.pos

cp /data/shared/sentires/$DATASET/$REVIEW_CORPUS.pos /data/shared/sentires/$DATASET/$REVIEW_CORPUS.pos.copy

java -Xmx400g -jar sentires.jar -t validate -c task/smu.$DATASET.task

java -Xmx500g -jar sentires.jar -t lexicon -c task/smu.$DATASET.task

java -Xmx500g -jar sentires.jar -t profile -c task/smu.$DATASET.task

python -u etc/code/dump_profile.py -o /data/shared/sentires/$DATASET -pos /data/shared/sentires/$DATASET/$REVIEW_CORPUS.positive.profile -neg /data/shared/sentires/$DATASET/$REVIEW_CORPUS.negative.profile
