from databend_py import Client
import os


def create_csv():
    import csv
    with open('upload.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([1, 'a'])
        writer.writerow([1, 'b'])


def upload_to_stage():
    client = Client.from_url("http://root:root@localhost:8000")
    # upload [(1, 'a'), (1, 'b')] as csv to stage ~
    stage_path = client.upload_to_stage('@~', "upload.csv", [(1, 'a'), (1, 'b')])
    print(stage_path)
    # stage_path is @~/upload.csv


def upload_file_to_stage():
    create_csv()
    client = Client.from_url("http://root:root@localhost:8000")
    with open("upload.csv", "rb") as f:
        stage_path = client.upload_to_stage('@~', "upload.csv", f)
        print(stage_path)

    os.remove("upload.csv")
