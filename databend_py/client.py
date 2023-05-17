import csv
import json
import os
import re
import requests
import time
import uuid
from urllib.parse import urlparse, parse_qs, unquote

from .connection import Connection
from .result import QueryResult
from .util.escape import escape_params
from .util.helper import asbool, Helper


class Client(object):
    """
    Client for communication with the databend http server.
    Single connection is established per each connected instance of the client.
    """

    def __init__(self, *args, **kwargs):
        self.settings = (kwargs.pop('settings', None) or {}).copy()
        self.connection = Connection(*args, **kwargs)
        self.query_result_cls = QueryResult
        self.helper = Helper
        self._debug = asbool(self.settings.get('debug', False))

    def __enter__(self):
        return self

    def disconnect(self):
        self.disconnect_connection()

    def disconnect_connection(self):
        self.connection.disconnect()

    def _data_generator(self, raw_data):
        while raw_data['next_uri'] is not None:
            try:
                raw_data = self._receive_data(raw_data['next_uri'])
                yield raw_data
            except (Exception, KeyboardInterrupt):
                self.disconnect()
                raise

    def _receive_data(self, next_uri: str):
        resp = self.connection.next_page(next_uri)
        raw_data = json.loads(resp.content)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        return raw_data

    def _receive_result(self, query, query_id=None, with_column_types=False):
        raw_data = self.connection.query(query)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        gen = self._data_generator(raw_data)
        result = self.query_result_cls(
            gen, raw_data, with_column_types=with_column_types)
        return result.get_result()

    def _iter_receive_result(self, query, query_id=None, with_column_types=False):
        raw_data = self.connection.query(query)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        gen = self._data_generator(raw_data)
        result = self.query_result_cls(
            gen, raw_data, with_column_types=with_column_types)
        _, rows = result.get_result()
        for row in rows:
            for r in row:
                yield r

    def execute(self, query, params=None, with_column_types=False,
                query_id=None, settings=None):
        """
        Executes query.
        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param query_id: the query identifier. If no query id specified
                         Databend server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).

        :return: * number of inserted rows for INSERT queries with data.
                   Returning rows count from INSERT FROM SELECT is not
                   supported.
                 * if `with_column_types=False`: `list` of `tuples` with
                   rows/columns.
                 * if `with_column_types=True`: `tuple` of 2 elements:
                    * The first element is `list` of `tuples` with
                      rows/columns.
                    * The second element information is about columns: names
                      and types.
        """
        # INSERT queries can use list/tuple/generator of list/tuples/dicts.
        # For SELECT parameters can be passed in only in dict right now.
        is_insert = isinstance(params, (list, tuple))

        if is_insert:
            # remove the `\n` '\s' `\t` in the SQL
            query = " ".join([s.strip() for s in query.splitlines()]).strip()
            rv = self._process_insert_query(query, params)
            return [], rv

        column_types, rv = self._process_ordinary_query(
            query, params=params, with_column_types=with_column_types,
            query_id=query_id)
        return column_types, rv

    # params = [(1,),(2,)] or params = [(1,2),(2,3)]
    def _process_insert_query(self, query, params):
        insert_rows = 0
        if "values" in query:
            query = query.split("values")[0] + 'values'
        elif "VALUES" in query:
            query = query.split("VALUES")[0] + 'VALUES'
        insert_re = re.compile("(?i)^INSERT INTO\s+\x60?([\w.^\(]+)\x60?\s*(\([^\)]*\))?")
        match = insert_re.match(query.strip())
        if len(match.group().split(' ')) < 2:
            raise Exception("Not standard insert statement")
        table_name = match[1]

        batch_size = query.count(',') + 1
        if params is not None:
            tuple_ls = [tuple(params[i:i + batch_size]) for i in range(0, len(params), batch_size)]
            filename = self._generate_csv(tuple_ls)
            with open(filename, "rb") as f:
                self._sync_csv_file_into_table(f, filename, table_name, "CSV")
            insert_rows = len(tuple_ls)
            os.remove(filename)

        return insert_rows

    def _process_ordinary_query(self, query, params=None, with_column_types=False,
                                query_id=None):
        if params is not None:
            query = self._substitute_params(
                query, params, self.connection.context
            )
        return self._receive_result(query, query_id=query_id, with_column_types=with_column_types, )

    def execute_iter(self, query, params=None, with_column_types=False,
                     query_id=None, settings=None):
        if params is not None:
            query = self._substitute_params(
                query, params, self.connection.context
            )
        return self._iter_receive_result(query, query_id=query_id, with_column_types=with_column_types)

    def _iter_process_ordinary_query(self, query, with_column_types=False, query_id=None):
        return self._iter_receive_result(query, query_id=query_id, with_column_types=with_column_types)

    def _substitute_params(self, query, params, context):
        if not isinstance(params, dict):
            raise ValueError('Parameters are expected in dict form')

        escaped = escape_params(params, context)
        return query % escaped

    @classmethod
    def from_url(cls, url):
        """
        Return a client configured from the given URL.

        For example::

            https://[user:password]@localhost:8000/default?secure=True
            http://[user:password]@localhost:8000/default
            databend://[user:password]@localhost:8000/default

        Any additional querystring arguments will be passed along to
        the Connection class's initializer.
        """
        parsed_url = urlparse(url)

        settings = {}
        kwargs = {}
        for name, value in parse_qs(parsed_url.query).items():
            if not value or not len(value):
                continue

            timeouts = {
                'connect_timeout',
                'send_receive_timeout',
                'sync_request_timeout'
            }

            value = value[0]

            if name == 'client_name':
                kwargs[name] = value
            elif name == 'secure':
                kwargs[name] = asbool(value)
            elif name == 'copy_purge':
                kwargs[name] = asbool(value)
                settings[name] = asbool(value)
            elif name == 'debug':
                settings[name] = asbool(value)
            elif name in timeouts:
                kwargs[name] = float(value)
            else:
                settings[name] = value  # settings={'copy_purge':False}
        secure = kwargs.get("secure", False)
        kwargs['secure'] = secure

        host = parsed_url.hostname

        if parsed_url.port is not None:
            kwargs['port'] = parsed_url.port

        path = parsed_url.path.replace('/', '', 1)
        if path:
            kwargs['database'] = path

        if parsed_url.username is not None:
            kwargs['user'] = unquote(parsed_url.username)

        if parsed_url.password is not None:
            kwargs['password'] = unquote(parsed_url.password)

        if settings:
            kwargs['settings'] = settings

        return cls(host, **kwargs)

    def _generate_csv(self, bindings):
        file_name = f'/tmp/{uuid.uuid4()}.csv'
        with open(file_name, "w+") as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerows(bindings)

        return file_name

    def stage_csv_file(self, file_descriptor, file_name):
        stage_path = "@~/%s" % file_name
        start_presign_time = time.time()
        _, row = self.execute('presign upload %s' % stage_path)
        if self._debug:
            print("upload: presign file:%s duration:%ss" % (file_name, time.time() - start_presign_time))

        presigned_url = row[0][2]
        headers = json.loads(row[0][1])
        start_upload_time = time.time()
        try:
            resp = requests.put(presigned_url, headers=headers, data=file_descriptor)
            resp.raise_for_status()
        finally:
            if self._debug:
                print("upload: put file:%s duration:%ss" % (file_name, time.time() - start_upload_time))
        return stage_path

    def _sync_csv_file_into_table(self, file_descriptor, file_name, table, file_type):
        start = time.time()
        stage_path = self.stage_csv_file(file_descriptor, file_name)
        copy_options = self._generate_copy_options()
        _, _ = self.execute(
            f"COPY INTO {table} FROM {stage_path} FILE_FORMAT = (type = {file_type} RECORD_DELIMITER = '\r\n')\
             PURGE = {copy_options['PURGE']} FORCE = {copy_options['FORCE']}\
              SIZE_LIMIT={copy_options['SIZE_LIMIT']} ON_ERROR = {copy_options['ON_ERROR']}")
        if self._debug:
            print("upload: copy %s duration:%ss" % (file_name, int(time.time() - start)))

    def upload(self, file_descriptor, file_name, table_name, file_type=None):
        """
        upload the file to database.table according to the file
        filename: the filename
        table_name: the table which write into
        file_type: the file type, default CSV
        """
        if not file_type:
            if len(file_name.split(".")) > 0:
                file_type = file_name.split(".")[1].upper()
            else:
                file_type = "CSV"
        self._sync_csv_file_into_table(file_descriptor, file_name, table_name, file_type)

    def upload_to_stage(self, file_descriptor, stage_path=None, file_name=None):
        """
        upload the file to user stage
        :param stage_path: target stage path
        :param file_descriptor: open file handler
        :param file_name:
        :return:
        """
        if stage_path is None:
            stage_path = "~"
        if file_name is None:
            file_name = f'{uuid.uuid4()}'
        stage_path = f"@{stage_path}/{file_name}"
        _, row = self.execute('presign upload %s' % stage_path)
        presigned_url = row[0][2]
        headers = json.loads(row[0][1])
        resp = requests.put(presigned_url, headers=headers, data=file_descriptor)
        resp.raise_for_status()
        return stage_path

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
