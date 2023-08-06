import os
import re
import csv
import argparse
import pandas as pd
from tqdm import tqdm
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from collections import Counter

OPINION_EXP = re.compile(r"(.*)<o>(.*?)</o>(.*)")
ASPECT_EXP = re.compile(r"(.*)<f>(.*?)</f>(.*)")
TAGGED_EXP = re.compile(r"<\w>(.*?)</\w>")
TARGET_EXP = re.compile(r"\[.*\]")

# NEGATION_TOKENS = ["no", "n't", "not", "hardly", "never", "ever"]

# def sentiment_negation(text):
#     for token in text.lower().split():
#         if token in NEGATION_TOKENS:
#             return -1
#     return 1


def readline(path):
    with open(path, "r") as f:
        for line in f:
            yield line


def parse_sentence(sentence, opinion, aspect):
    original_sentence = str(sentence)
    stemmer = PorterStemmer()
    opinion_masker = {
        "keyword": opinion,
        "type": "opinion",
        "fmt": r"\1<o>\2</o>",
        "exp": OPINION_EXP,
        "ufmt": "<o>{}</o>",
    }
    aspect_masker = {
        "keyword": aspect,
        "type": "aspect",
        "fmt": r"\1<f>\2</f>",
        "exp": ASPECT_EXP,
        "ufmt": "<f>{}</f>",
    }

    maskers = (
        [opinion_masker, aspect_masker]
        if len(opinion) > len(aspect)
        else [aspect_masker, opinion_masker]
    )

    for masker in maskers:
        sentence = re.sub(
            re.compile("(^| )({})".format(masker["keyword"])),
            masker["fmt"],
            sentence,
            1,
        )
        if not masker["exp"].match(sentence):
            masker["keyword"] = stemmer.stem(masker["keyword"])
            sentence = re.sub(
                re.compile("(^| )({})".format(masker["keyword"])),
                masker["fmt"],
                sentence,
                1,
            )
        sentence = re.sub(
            re.compile(masker["ufmt"].format(masker["keyword"])),
            masker["ufmt"].format("_".join(masker["keyword"].split())),
            sentence,
        )

    sentence = re.sub(
        r"(<\w?>[ \w]+)(</\w?>)([\w]+)?([-/]+)?(\w+)?", r"\1\3\2 \4\5", sentence
    )
    sentence = re.sub(r"\(\d+\)$", "", sentence).strip()

    opinion_pos = None
    aspect_pos = None

    opinion_segments = OPINION_EXP.match(sentence)
    if opinion_segments is not None:
        opinion_pos = len(
            word_tokenize(re.sub(TAGGED_EXP, r"\1", opinion_segments.group(1)))
        )
        opinion = opinion_segments.group(2)
    aspect_segments = ASPECT_EXP.match(sentence)
    if aspect_segments is not None:
        aspect_pos = len(
            word_tokenize(re.sub(TAGGED_EXP, r"\1", aspect_segments.group(1)))
        )
        aspect = aspect_segments.group(2)
    tokens = word_tokenize(re.sub(TAGGED_EXP, r"\1", sentence))
    sentence_len = len(tokens)
    sentence = " ".join(tokens)
    return sentence, sentence_len, opinion_pos, opinion, aspect_pos, aspect


POS_PROFILE_DUMP_FILENAME = "profile.pos.dump.csv"
NEG_PROFILE_DUMP_FILENAME = "profile.neg.dump.csv"
PROFILE_DUMP_FILENAME = "profile.dump.csv"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--out", type=str, default="dist/", help="Ouput directory"
    )
    parser.add_argument(
        "-pos",
        "--pos_profile_file",
        type=str,
        default="data/sentires/profile/example.pos.profile",
    )
    parser.add_argument(
        "-neg",
        "--neg_profile_file",
        type=str,
        default="data/sentires/profile/example.neg.profile",
    )
    parser.add_argument(
        "-mo",
        "--manual_long_opinion_phrases_file",
        type=str,
        default="/data/shared/sentires/manual.opinion",
        help="common long opinion phrases (>=2 tokens)",
    )
    return parser.parse_args()


def construct_long_opinion_phrase(opinion, sentence, long_opinion_phrases):
    for long_opinion_phrase in long_opinion_phrases.get(opinion, []):
        search = re.search(
            re.compile("(^| )({})".format(long_opinion_phrase.lower())),
            sentence.lower(),
        )
        if search is not None:
            span = search.span()
            return sentence[span[0] : span[1]].strip()
    return opinion


def export_profile_csv(path, output, sentiment=1, long_opinion_phrases={}):
    columns = [
        "reviewerID",
        "asin",
        "overall",
        "unixReviewTime",
        "sentire_aspect",
        "aspect_pos",
        "aspect",
        "opinion_pos",
        "opinion",
        "sentence",
        "sentence_len",
        "sentence_count",
        "sentiment",
    ]
    df = pd.DataFrame(columns=columns)
    record = {"sentiment": sentiment}
    cur_product = None
    cur_aspect = None
    cur_opinion = None
    readline_f = readline
    with open(output, "w") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for line in tqdm(readline_f(path), desc=path):
            tokens = line.split("\t")
            if tokens[0] == "e":
                cur_product = tokens[2].strip()
                user_id, item_id, rating, unixReviewTime = parse_review_id(cur_product)
                record.update(
                    {
                        "reviewerID": user_id,
                        "asin": item_id,
                        "overall": rating,
                        "unixReviewTime": unixReviewTime,
                    }
                )
            elif tokens[0] == "f":
                cur_aspect = tokens[2].strip()
            elif tokens[0] == "o":
                cur_opinion = tokens[3].strip()
            elif tokens[0] == "c":
                sentence = tokens[4].strip()
                match = re.search(r"\((\d+)\)$", sentence)
                sentence_count = int(float(match.group(1))) if match else 1
                sentence = re.sub(r"\(\d+\)$", "", sentence).strip()
                t_opinion = construct_long_opinion_phrase(
                    cur_opinion, sentence, long_opinion_phrases
                )
                (
                    sentence,
                    sentence_len,
                    opinion_pos,
                    opinion,
                    aspect_pos,
                    aspect,
                ) = parse_sentence(sentence, t_opinion, cur_aspect)
                record.update(
                    {
                        "sentence": sentence,
                        "sentence_len": sentence_len,
                        "sentence_count": sentence_count,
                        "opinion_pos": opinion_pos,
                        "opinion": opinion,
                        "aspect_pos": aspect_pos,
                        "aspect": aspect,
                        "sentire_aspect": cur_aspect
                    }
                )
                writer.writerow(record)


def parse_review_id(review_id):
    tokens = review_id.split("|")
    user_id, item_id, overall_rating, unixReviewTime = (
        tokens[0],
        tokens[1],
        tokens[2],
        tokens[3],
    )
    return user_id, item_id, overall_rating, unixReviewTime


def get_long_opinion_phrases(phrases=[]):
    phrases_index = {}
    for phrase in phrases:
        for token in phrase.split():
            token_phrases = phrases_index.setdefault(token, [])
            if phrase not in token_phrases:
                token_phrases.append(phrase)
            token_phrases.sort(key=len, reverse=True)
    return phrases_index


def main(args):
    os.makedirs(args.out, exist_ok=True)
    phrases = (
        pd.read_csv(args.manual_long_opinion_phrases_file, header=None)[0].unique().tolist()
        if os.path.exists(args.manual_long_opinion_phrases_file)
        else []
    )
    long_opinion_phrases = get_long_opinion_phrases(phrases)
    pos_profile_dump_file = os.path.join(args.out, POS_PROFILE_DUMP_FILENAME)
    neg_profile_dump_file = os.path.join(args.out, NEG_PROFILE_DUMP_FILENAME)
    export_profile_csv(
        args.pos_profile_file, pos_profile_dump_file, 1, long_opinion_phrases
    )
    export_profile_csv(
        args.neg_profile_file, neg_profile_dump_file, -1, long_opinion_phrases
    )
    pos_profile = pd.read_csv(pos_profile_dump_file)
    neg_profile = pd.read_csv(neg_profile_dump_file)
    profile = pd.concat([pos_profile, neg_profile])
    profile.to_csv(os.path.join(args.out, PROFILE_DUMP_FILENAME), index=False)


if __name__ == "__main__":
    main(parse_args())
