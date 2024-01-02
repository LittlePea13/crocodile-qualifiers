import os, sys
import re
from csv import reader
from tqdm import tqdm
import argparse

import sqlite3
import os
import json
from concurrent.futures import ProcessPoolExecutor
import sys
import csv

csv.field_size_limit(sys.maxsize)
def extract_wikidata_entities_from_line(line):
    data = json.loads(line)
    links = data.get('links', [])
    wikidata_entities = set()

    for link in links:
        wikidata_entity = link.get('wikidata')
        if wikidata_entity:
            wikidata_entities.add(wikidata_entity)

    return wikidata_entities

def process_jsonl_file(file_path):
    wikidata_entities_set = set()

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            wikidata_entities_set.update(extract_wikidata_entities_from_line(line))

    return wikidata_entities_set

def process_directory(directory_path):
    # jsonl_files = [f for f in os.listdir(directory_path)]
    # jsonl_paths = [os.path.join(directory_path, f) for f in jsonl_files]
    jsonl_paths = []
    for i,j,y in os.walk(directory_path):
        for file_name in y:
            jsonl_paths.append(i + '/' + file_name)
    # print(f'Found {jsonl_paths} jsonl files')
    # exit()
    with ProcessPoolExecutor() as executor:
        # Use list to force the execution of map and gather results
        results = list(executor.map(process_jsonl_file, jsonl_paths))

    # Combine results from all processes into a single set
    final_wikidata_entities_set = set()
    for result in results:
        final_wikidata_entities_set.update(result)

    return final_wikidata_entities_set


def main_db(input_file = '/mnt/sda/RE-NLG-Dataset/datasets/wikidata/wikidata-triples.csv', folder_input = 'text', output_file = 'wikidata-triples-ca-subj.csv'):
    wikidata_ids = process_directory(folder_input)
    print(f'Found {len(wikidata_ids)} different entities')
    try:
        os.remove(output_file)
    except FileNotFoundError:
        pass
    conn = sqlite3.connect(output_file, isolation_level="EXCLUSIVE")
    with conn:
        conn.execute(
            """CREATE TABLE triplets (
            subject text,
            relation text,
            object text,
            qualifier text,
            qualifier_object text,
            subjobj text)"""
        )

    c = conn.cursor()

    # open file in read mode
    with open(input_file, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Iterate over each row in the csv using reader object
        for i, row in tqdm(enumerate(csv_reader)):
            # row variable is a list that represents a row in csv
            fields = row[0].split('\t')
            if fields[0] in wikidata_ids:
                if len(fields) == 3:
                    c.execute(
                        "INSERT INTO triplets (subject, relation, object, qualifier, qualifier_object, subjobj) VALUES (?, ?, ?, ?, ?, ?)",
                        (fields[0], fields[1], fields[2], '', '', fields[0] + '\t' + fields[2]),
                    )
                elif len(fields) == 5:
                    c.execute(
                        "INSERT INTO triplets (subject, relation, object, qualifier, qualifier_object, subjobj) VALUES (?, ?, ?, ?, ?, ?)",
                        (row[0].split('\t')[0], row[0].split('\t')[1], row[0].split('\t')[2], row[0].split('\t')[3], row[0].split('\t')[4],
                        row[0].split('\t')[0] + '\t' + row[0].split('\t')[2]),
                    )
    conn.commit()
    conn.execute("""CREATE INDEX idx_triplet_id ON triplets(subjobj);""")
    conn.commit()
    conn.execute("""CREATE INDEX idx_triplet_trio ON triplets(subject, relation, object);""")
    conn.commit()
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("--input", 
                        help="XML wiki dump file")
    parser.add_argument("--output",
                        help="XML wiki dump file")
    parser.add_argument("--input_triples", 
                        help="XML wiki dump file")

    args = parser.parse_args()
    main_db(args.input_triples, args.input, args.output)
