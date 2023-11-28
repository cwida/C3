# for each dataset, read .table.sql
# for each column, check for datatype
# generate yaml

import yaml
import os
import re
import os
import subprocess
import sys

target_directory = f"{os.path.dirname(__file__)}/../PublicBIbenchmark"
row_count = 65536

if len(sys.argv) != 3:
    print("usage: python3 create_public_bi_datasets.py [path to public_bi_benchmark repository] [path to PublicBIbencmark directory with decompressed CSVs]")
    sys.exit()
repo_dir = sys.argv[1] + "benchmark/"
decompressed_dir = sys.argv[2]

def create_schema_yaml():
    for dataset in os.listdir(repo_dir):
        if os.path.isdir(f"{repo_dir}{dataset}"):
            for table in os.listdir(f"{repo_dir}{dataset}/tables"):

                table_name = re.search('^(.+)\.table\.sql', table).group(1)
                print(f"generating schema.yaml for: {table_name}")
                yaml_dict = {"columns": []}
                f = open(f"{repo_dir}{dataset}/tables/{table}", "r")

                for i in f.readlines()[1:-1]:
                    col_name = re.search('"(.+)"', i).group(1).replace("/", "_")
                    if "integer" in i or "smallint" in i:
                        yaml_dict["columns"].append({"name": col_name, "type": "integer"})
                    elif "varchar" in i:
                        yaml_dict["columns"].append({"name": col_name, "type": "string"})
                    elif "decimal" in i or "double" in i:
                        yaml_dict["columns"].append({"name": col_name, "type": "double"})
                    else:  # "timestamp", "date", "time", "boolean", "bigint"
                        yaml_dict["columns"].append({"name": col_name, "type": "skip"})

                yaml_dir = f"{repo_dir}{dataset}/yaml/{table_name}"
                if not os.path.exists(yaml_dir):
                        os.makedirs(yaml_dir)
                
                yaml_path = f"{yaml_dir}/schema_temp.yaml"
                with open(yaml_path, 'w') as yaml_file:
                    yaml.dump([yaml_dict], yaml_file)

                yaml_path_final = f"{yaml_dir}/schema.yaml"
                with open(yaml_path_final, "w") as output:
                    with open(yaml_path, "r") as input:
                        output.write(input.read()[2:])

                print(f"schema.yaml in {yaml_path_final}")
                os.remove(yaml_path)

def create_small_benchmark_c3():
    for dataset in os.listdir(repo_dir):
        if os.path.isdir(f"{repo_dir}{dataset}"):
            for table in os.listdir(f"{repo_dir}{dataset}/tables"):
                table_name = re.search('^(.+)\.table\.sql', table).group(1)

                if (table_name == f"{dataset}_1" or table_name == "TrainsUK1_2") or table_name == "TrainsUK2_1":

                    print(f"generating dataset copy for: {table_name}")

                    dataset_path = f"{target_directory}/{dataset}/{table_name}"
                    if not os.path.exists(dataset_path):
                        os.makedirs(dataset_path)

                    subprocess.run(f"head -n {row_count} {decompressed_dir}{dataset}/{table_name}.csv > {dataset_path}/{table_name}.csv", shell=True)
                    subprocess.run(f"cp -fr {repo_dir}{dataset}/yaml/{table_name}/schema.yaml {dataset_path}/schema.yaml", shell=True)            
            
                    print(f"dataset in {dataset_path}")
    
if __name__ == "__main__":
    # create_schema_yaml()
    create_small_benchmark_c3()
    print("done")
