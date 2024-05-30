import argparse
import re
import shutil
from pathlib import Path
import random
import uuid
import math
import os
import sys
import base64


# Create a %run cell pointing the given ID. The path is relative to notebook it is in.
def create_percent_run_cell(id):
    path = f'./notebook_{id}'
    ret = [f'%run {path}']
    return '\n'.join(ret) + '\n'


def create_junk_cell(junk_cell_size):
    bytes_needed = 768 #1048576 / 4 * 3
    random_bytes = os.urandom(junk_cell_size * bytes_needed)
    random_string = base64.b64encode(random_bytes).decode('utf-8')
    return '\n'.join(random_string) + '\n'


def generate_chain(outpath, chain_count, junk_cell_count, junk_cell_size):
    ids = list(range(chain_count))
    print("ids: ", ids)

    # Create a simple binary tree of %run calls
    for i in ids:
        generate_notebook(outpath, i, ids[i + i + 1] if i + i + 1 < len(ids) else None,
                          ids[i + i + 2] if i + i + 2 < len(ids) else None, junk_cell_count, junk_cell_size)


def generate_notebook(outpath, id, child_id1, child_id2, junk_cell_count, junk_cell_size):
    # notebooks needed
    children = [i for i in [child_id1, child_id2] if i is not None]
    print(f'Notebook {id} has children: {children}')
    ret = []
    ret.append(f"{id}\n")
    for i in children:
        ret.append(create_percent_run_cell(i))
    for _ in range(junk_cell_count):
        ret.append(create_junk_cell(junk_cell_size))
    cell_sep = '\n# COMMAND ----------\n'
    nb_header = '# Databricks notebook source\n'
    # Add another cell so there are no empty notebooks
    nb_content = nb_header + cell_sep.join(ret)
    notebook_path = f'{outpath}/notebook_{id}.py'
    print(f'Writing {notebook_path}')
    with open(notebook_path, 'w+') as f:
        f.write(nb_content)
    #Path(notebook_path).expanduser().write_text(nb_content)


def make_zip(nb_path):
    name = "chain_get"
    if nb_path != ".":
        name = nb_path.split("/")[-1]
    shutil.make_archive(name, 'zip', nb_path)


def main():
    parser = argparse.ArgumentParser(description='Create notebook chain')
    parser.add_argument('--chain_count',
                        help="The total number of %run calls to generate in binary call tree",
                        type=int, default=2)
    parser.add_argument('--nb_path', help="path to directory", default="./chain_test")
    parser.add_argument('--zip', help="Should a zip be created of the nb_path",
                        action=argparse.BooleanOptionalAction)
    parser.add_argument('--junk_cell_count', help="Extra junk cells to add (to increase size)",
                        type=int, default=0)
    parser.add_argument('--junk_cell_size', help="Extra junk size in MBs",
                        type=int, default=0)
    args = parser.parse_args()
    nb_path = args.nb_path
    nb_path = re.sub('(\s|/)*$', '', nb_path)
    if nb_path:
        print("nb_path: ", nb_path)
        print("chain count: ", args.chain_count)
        generate_chain(nb_path, args.chain_count, args.junk_cell_count, args.junk_cell_size)
        if args.zip:
            make_zip(nb_path)
    else:
        parser.print_help()
        parser.exit(message="nb_path must be specified")


if __name__ == '__main__':
    main()

