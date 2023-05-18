import requests
import io
import csv
import uuid
import json
import time


class DataUploader:
    def __init__(self, client, settings, default_stage_dir='@~', debug=False):
        # TODO: make it depends on Connection instead of Client
        self.client = client
        self.settings = settings
        self.default_stage_dir = default_stage_dir
        self._debug = debug

    def upload_to_table(self, table_name, data):
        stage_path = self._gen_stage_path(self.default_stage_dir)
        presigned_url, headers = self._execute_presign(stage_path)
        self._upload_to_presigned_url(presigned_url, headers, data)
        self._execute_copy(table_name, stage_path, 'CSV')

    def upload_to_stage(self, stage_dir, filename, data):
        stage_path = self._gen_stage_path(stage_dir, filename)
        presigned_url, headers = self._execute_presign(stage_path)
        self._upload_to_presigned_url(presigned_url, headers, data)
        return stage_path

    def _gen_stage_path(self, stage_dir, stage_filename=None):
        if stage_filename is None:
            stage_filename = '%s.csv' % uuid.uuid4()
        if stage_filename.startswith('/'):
            stage_filename = stage_filename[1:]
        # TODO: escape the stage_path if it contains special characters
        stage_path = '%s/%s' % (stage_dir, stage_filename)
        return stage_path

    def _execute_presign(self, stage_path):
        start_time = time.time()
        _, row = self.client.execute('presign upload %s' % stage_path)
        presigned_url = row[0][2]
        headers = json.loads(row[0][1])
        if self._debug:
            print('upload:_execute_presign %s: %s' % (stage_path, time.time() - start_time))
        return presigned_url, headers

    def _upload_to_presigned_url(self, presigned_url, headers, data):
        # TODO: if data's type is bytes or File, then upload it directly
        if isinstance(data, list):
            buf = io.BytesIO()
            buf_writer = csv.writer(buf, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            buf_writer.writerows(data)
            buf_size = buf.getbuffer().nbytes
            data_len = len(data)
        else:
            raise Exception('data is not a list: %s' % type(data))
        start_time = time.time()
        try:
            resp = requests.put(presigned_url, headers=headers, data=buf)
            resp.raise_for_status()
        finally:
            if self._debug:
                print('upload:_upload_to_presigned_url len=%d bufsize=%d %s' % (data_len, buf_size, time.time() - start_time))

    def _execute_copy(self, table_name, stage_path, file_type):
        start_time = time.time()
        sql = self._make_copy_statement(table_name, stage_path, file_type)
        self.client.execute(sql)
        if self._debug:
            print('upload:_execute_copy table=%s %s' % (table_name, time.time() - start_time))

    def _make_copy_statement(self, table_name, stage_path, file_type):
        copy_options = self._generate_copy_options()
        return f"COPY INTO {table_name} FROM {stage_path} " \
            f"FILE_FORMAT = (type = {file_type} RECORD_DELIMITER = '\r\n') " \
            f"PURGE = {copy_options['PURGE']} FORCE = {copy_options['FORCE']} " \
            f"SIZE_LIMIT={copy_options['SIZE_LIMIT']} ON_ERROR = {copy_options['ON_ERROR']}"

    def _generate_copy_options(self):
        # copy options docs: https://databend.rs/doc/sql-commands/dml/dml-copy-into-table#copyoptions
        copy_options = {}
        if "copy_purge" in self.settings:
            copy_options["PURGE"] = self.settings["copy_purge"]
        else:
            copy_options["PURGE"] = False

        if "force" in self.settings:
            copy_options["FORCE"] = self.settings["force"]
        else:
            copy_options["FORCE"] = False

        if "size_limit" in self.settings:
            copy_options["SIZE_LIMIT"] = self.settings["size_limit"]
        else:
            copy_options["SIZE_LIMIT"] = 0

        if "on_error" in self.settings:
            copy_options["ON_ERROR"] = self.settings["on_error"]
        else:
            copy_options["ON_ERROR"] = "abort"
        return copy_options