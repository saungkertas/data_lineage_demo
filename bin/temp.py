import argparse
import glob
import os
import subprocess
import sys

from bigquery import get_client
from pyscaffold.cli import parse_args


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="specify job")
    parser.add_argument(
        '--key', help="location json key credentials", required=True)
    # parser.add_argument('--service_type', help="low key, based directory name of its sql", required=True)
    # parser.add_argument('--sql_dir', help="sql directory", required=True)
    return parser.parse_args(args)


def main(args):
    count_job = 0
    count_table = 0
    args = parse_args(args)
    if args.key:
        json_key = args.key
    else:
        json_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = get_client(json_key_file=json_key, readonly=False)

    subprocess.call("cowsay Hi Angels....! time to check consistency ", shell=True)

    print(
        "Start checking consistency.")
    print("-----")
    print("-----")
    print("")

    dqs_table_query = """WITH TA AS( SELECT table_name,metric_name ,ROUND(max(count_tolerance)+ POWER(ROUND(max(count_tolerance),2),0.5),2) as count_tolerance FROM `bi-gojek.audit.xx_checking_consistency_threshold` group by table_name,metric_name order by table_name, count_tolerance) SELECT table_name,metric_name ,IF(count_tolerance > 100,100,count_tolerance) as count_tolerance FROM TA"""

    count_dqs_job, _ = client.query(query=dqs_table_query, use_legacy_sql=False, timeout=6000)
    dqs_rows = client.get_query_rows(job_id=count_dqs_job)
    total_dqs_tables = 0
    for row in dqs_rows:
        print('TABLE_NAME:' + row['table_name'])
        print('metricConsistencyTolerance:')
        print('  ' + row['metric_name'] + ': ' + str(row['count_tolerance']))
        print('')


def read_sql(sql_file_path):
    with open(sql_file_path, "r") as file:
        return file.read()


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
