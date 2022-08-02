import json
from datetime import datetime, timedelta
from pathlib import Path
import json
import boto3
import os
import pandas as pd
from prefect import task, Flow, Parameter
from typing import Union
from prefect.schedules import IntervalSchedule
from io import StringIO, BufferedIOBase

AWS_KEY = os.getenv("AWS_KEY")
AWS_SECRET = os.getenv("AWS_SECRET")

s3 = boto3.resource("s3", aws_access_key_id=AWS_KEY,
                    aws_secret_access_key=AWS_SECRET, )
source_folder = "unprocessed/"
s3_destination = "processed/"
destination_folder = Path("local")
destination_folder.mkdir(exist_ok=True, parents=True)
extracted_folder = Path("extracted")
extracted_folder.mkdir(exist_ok=True, parents=True)
my_bucket = s3.Bucket("sams-bucket-garlic-aioli")

for obj in my_bucket.objects.filter(Prefix=source_folder):
    if obj.key.endswith("/"):
        continue
    obj_content = obj.get()
    decoded = json.loads(obj_content["Body"].read().decode())
    my_bucket.download_file(obj.key, (destination_folder / Path(obj.key).name).as_posix())


@task(max_retries=10, retry_delay=timedelta(seconds=10))
def extract(input_file: Union[Path, str]) -> dict:
    res = open(input_file, "r")
    res_load = json.loads(res.read())
    if not res:
        raise Exception('No data fetched!')
    return res_load


@task
def transform(data: dict) -> pd.DataFrame:
    transformed = []
    for user in data:
        transformed.append({
            'ID': user['id'],
            'Name': user['name'],
            'Username': user['username'],
            'Email': user['email'],
            'Address': f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
            'PhoneNumber': user['phone'],
            'Company': user['company']['name']
        })
    return pd.DataFrame(transformed)


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def load(data: pd.DataFrame, path: str) -> None:
    data.to_csv(path_or_buf=path, index=False)


# scheduler = IntervalSchedule(
#     interval=timedelta(seconds=5)
# )

# scheduler = CronSchedule(
#     cron='* * * * *'
# )

@task
def upload_s3(obj_: pd.DataFrame, remote_path: str):
    with StringIO() as buf:
        obj_.to_csv(path_or_buf=buf)
        buf.seek(0)
        my_bucket.Object(remote_path).put(Body=buf.getvalue())


def prefect_flow():
    with Flow(name='simple_etl_pipeline') as flow:
        files = destination_folder.glob("*.*")
        for file in files:
            users = extract(file)
            df_users = transform(users)
            load(data=df_users,
                 path=f'{os.getcwd()}\\{extracted_folder}\\{file.stem}_parsed_{int(datetime.now().timestamp())}.csv')
            upload_s3(df_users, f"processed/{file.stem}_parsed_{int(datetime.now().timestamp())}.csv")
    return flow


if __name__ == '__main__':
    flow = prefect_flow()
    flow.run()

# # register flow with Prefect Cloud
#     flow.register(project_name="prefect_demo")
#
#
# # start the agent
#     flow.run_agent(token="pcu_mBIb2VIjj1MWxoWWrUkYglh3CRfE9E2Blczr")
