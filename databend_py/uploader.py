import requests
import io
import csv
import uuid
import json
import time
import gzip
from . import log


class DataUploader:
    def __init__(self, client, connection, settings, default_stage_dir='@~', debug=False, compress=False):
        # TODO: make it depends on Connection instead of Client
        self.client = client
        self.connection = connection
        self.settings = settings
        self.default_stage_dir = default_stage_dir
        self._compress = compress
        self._debug = debug

    def upload_to_table_by_copy(self, table_name, data):
        if len(data) == 0:
            return
        stage_path = self._gen_stage_path(self.default_stage_dir)
        presigned_url, headers = self._execute_presign(stage_path)
        self._upload_to_presigned_url(presigned_url, headers, data)
        self._execute_copy(table_name, stage_path, 'CSV')

    def replace_into_table(self, table_name, conflict_keys, data):
        """
        :param table_name: table name
        :param conflict_keys: if use replace, the conflict_keys can't be None
        :param data: list data to insert/replace
        :return:
        """
        if len(data) == 0:
            return
        stage_path = self._gen_stage_path(self.default_stage_dir)
        presigned_url, headers = self._execute_presign(stage_path)
        self._upload_to_presigned_url(presigned_url, headers, data)
        sql_statement = f"REPLACE INTO {table_name} ON ({','.join(conflict_keys)}) VALUES"
        self._execute_with_attachment(sql_statement, stage_path, "CSV")

    def upload_to_stage(self, stage_dir, filename, data):
        stage_path = self._gen_stage_path(stage_dir, filename)
        presigned_url, headers = self._execute_presign(stage_path)
        self._upload_to_presigned_url(presigned_url, headers, data)
        return stage_path

    def _gen_stage_path(self, stage_dir, stage_filename=None):
        if stage_filename is None:
            suffix = '.csv.gz' if self._compress else '.csv'
            stage_filename = '%s%s' % (uuid.uuid4(), suffix)
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

    def _serialize_data(self, data, compress):
        # In Python3 csv.writer expects a file-like object opened in text mode. In Python2, csv.writer expects a file-like object opened in binary mode.
        start_time = time.time()
        buf = io.StringIO()
        csvwriter = csv.writer(buf, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerows(data)
        output = buf.getvalue()
        if compress:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gzwriter:
                gzwriter.write(output.encode('utf-8'))
            output = buf.getvalue()
        if self._debug:
            print('upload:_serialize_data %s' % (time.time() - start_time))
        return output

    def _upload_to_presigned_url(self, presigned_url, headers, data):
        # Check if data is bytes or File
        if isinstance(data, (bytes, io.IOBase)):
            data = data.read()  # Read the data from the buffer
            buf = data
            buf_size = len(buf)
            data_len = 1
        elif isinstance(data, list):
            buf = self._serialize_data(data, self._compress)
            buf_size = len(buf)
            data_len = len(data)
        else:
            raise Exception('data is not bytes, File, or a list: %s' % type(data))
        start_time = time.time()
        try:
            resp = requests.put(presigned_url, headers=headers, data=buf)
            resp.raise_for_status()
        finally:
            if self._debug:
                print('upload:_upload_to_presigned_url len=%d bufsize=%d %s' % (
                    data_len, buf_size, time.time() - start_time))

    def _execute_copy(self, table_name, stage_path, file_type):
        start_time = time.time()
        sql = self._make_copy_statement(table_name, stage_path, file_type)
        self.client.execute(sql)
        if self._debug:
            print('upload:_execute_copy table=%s %s' % (table_name, time.time() - start_time))

    def _make_copy_statement(self, table_name, stage_path, file_type):
        # copy options docs: https://databend.rs/doc/sql-commands/dml/dml-copy-into-table#copyoptions
        copy_options = {}
        copy_options["PURGE"] = self.settings.get("copy_purge", False)
        copy_options["FORCE"] = self.settings.get("force", False)
        copy_options["SIZE_LIMIT"] = self.settings.get("size_limit",
                                                       0)  # TODO: is this correct to set size_limit = 100?
        copy_options["ON_ERROR"] = self.settings.get("on_error", "abort")
        return f"COPY INTO {table_name} FROM {stage_path} " \
               f"FILE_FORMAT = (type = {file_type} RECORD_DELIMITER = '\\r\\n' COMPRESSION = AUTO) " \
               f"PURGE = {copy_options['PURGE']} FORCE = {copy_options['FORCE']} " \
               f"SIZE_LIMIT={copy_options['SIZE_LIMIT']} ON_ERROR = {copy_options['ON_ERROR']}"

    def _execute_with_attachment(self, sql_statement, stage_path, file_type):
        start_time = time.time()
        data = self._make_attachment(sql_statement, stage_path, file_type)
        url = self.connection.format_url()

        try:
            resp_dict = self.connection.do_query(url, data)
            self.client_session = resp_dict.get("session", self.connection.default_session())
            if self._debug:
                print('upload:_execute_attachment sql=%s %s' % (sql_statement, time.time() - start_time))
        except Exception as e:
            log.logger.error(
                f"http error on {url}, SQL: {sql_statement} error msg:{str(e)}"
            )
            raise

    def _make_attachment(self, sql_statement, stage_path, file_type):
        copy_options = {}
        copy_options["PURGE"] = self.settings.get("copy_purge", "False")
        copy_options["FORCE"] = self.settings.get("force", "False")
        copy_options["SIZE_LIMIT"] = self.settings.get("size_limit", "0")
        copy_options["ON_ERROR"] = self.settings.get("on_error", "abort")

        file_format_options = {}
        file_format_options["type"] = file_type

        data = {
            "sql": sql_statement,
            "stage_attachment": {"location": stage_path, "file_format_options": file_format_options,
                                 "copy_options": copy_options}
        }
        return data
