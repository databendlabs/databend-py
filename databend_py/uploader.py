import requests
import io


class DataUploader:
    def __init__(self, client, settings, stage_dir='@~', debug=False):
        # TODO: make it depends on Connection instead of Client
        self.client = client
        self.debug = debug
        self.settings = settings
        self.stage_dir = stage_dir

    def upload_to_table(self, database, table, data):
        stage_path = self._gen_stage_path()
        presigned_url, headers = self._execute_presign(stage_path)
        self._upload_to_presigned_url(presigned_url, headers, data)
        self._execute_copy(database, table, stage_path)

    def upload_to_stage(self, filename, data):
        pass

    def _gen_stage_path(self, stage_filename=None):
        if stage_filename is None:
            stage_filename = '%s.csv' % uuid.uuid4()
        if stage_filename.startswith('/'):
            stage_filename = stage_filename[1:]
        # TODO: escape the stage_path if it contains special characters
        stage_path = '%s/%s' % (self.stage_dir, stage_filename)
        return stage_path

    def _execute_presign(self, stage_path):
        _, row = self.client.execute('presign upload %s' % stage_path)
        presigned_url = row[0][2]
        headers = json.loads(row[0][1])
        return presigned_url, headers

    def _upload_to_presigned_url(self, presigned_url, headers, data):
        buf = io.BytesIO()
        w = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        w.writerows(data)
        resp = requests.put(presigned_url, headers=headers, data=buf)
        resp.raise_for_status()

    def _execute_copy(self, database, table, stage_path):
        sql = self._make_copy_statement(database, table, stage_path)
        self.client.execute(sql)

    def _make_copy_statement(self, database, table, stage_path):
        copy_options = self._generate_copy_options()
        return f"COPY INTO {database}.{table} FROM {stage_path} " \
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