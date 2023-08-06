import argparse
import gzip
import json
from tqdm import tqdm
import os
from collections import Counter
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', type=str, default='/data/shared/download/jmcauley/reviews_Electronics_5.json.gz')
parser.add_argument('-o', '--output_dir', type=str, default='/data/shared/sentires/electronic')
parser.add_argument('-n', '--number_of_reviews', type=int, default=-1)
parser.add_argument('-mu', '--min_user_freq', type=int, default=5)
parser.add_argument('-mi', '--min_item_freq', type=int, default=5)
args = parser.parse_args()

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield eval(l)

def parse_data(path, number_of_reviews = -1):
  user_ids = set()
  item_ids = set()
  reviews = []
  # reviewTexts = []
  # item_reviews = {}

  count = 0
  for l in parse(path):
    user_ids.add(l['reviewerID'])
    item_ids.add(l['asin'])
    # reviewTexts.append(l['reviewText'])
    reviews.append(l)
    count += 1
    if count==number_of_reviews:
      break
  return user_ids, item_ids, reviews

def main():
  input_file_name = os.path.basename(args.input)
  user_ids, item_ids, reviews = parse_data(args.input, args.number_of_reviews)
  print("# users: {}".format(len(user_ids)))
  print("# items: {}".format(len(item_ids)))
  print("# reviews: {}".format(len(reviews)))

  user_freq = Counter(r['reviewerID'] for r in reviews)
  reviews = [r for r in reviews if user_freq[r['reviewerID']] >= args.min_user_freq]
  item_freq = Counter(r['asin'] for r in reviews)
  reviews = [r for r in reviews if item_freq[r['asin']] >= args.min_item_freq]

  output_dir = args.output_dir
  os.makedirs(output_dir, exist_ok=True)

  item_reviews = {}

  print("Appending reviews into items array")
  for r in tqdm(reviews, desc='Append reviews to items'):
    item_reviews.setdefault(r['asin'], []).append(r)
  print("Done appending reviews to items array")

  # build products file
  count = 0
  print("Starting write data into files")
  raw_file = open(os.path.join(output_dir, input_file_name + ".raw"), "w")
  product_file = open(os.path.join(output_dir, input_file_name + ".product"), "w")
  product_file_review_level = open(os.path.join(output_dir, input_file_name + ".review.product"), "w")
  for item_id, records in tqdm(item_reviews.items(), desc='Write data into files'):
    product_file.write("{}\t{}\t{}\n".format(item_id, len(records), "http://dummy.url"))
    for r in records:
      product_file_review_level.write("{}\t{}\t{}\n".format("|".join([r['reviewerID'], r['asin'], str(r['overall']), str(r['unixReviewTime'])]), 1, "http://dummy.url"))
      product_file_review_level.write("\t<DOC>\n\t{}\n\t{}\n\t</DOC>\n".format(r['summary'], r['reviewText']))
      product_file.write("\t<DOC>\n\t{}\n\t{}\n\t</DOC>\n".format(r['summary'], r['reviewText']))
      raw_file.write("<DOC>\n{}\n{}\n</DOC>\n".format(r['summary'], r['reviewText']))
  product_file_review_level.close()
  product_file.close()
  raw_file.close()
  print("Done parsing data into files.")

  

if __name__ == '__main__':
  main()