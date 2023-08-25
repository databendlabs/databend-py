import json
from urllib.parse import urlparse, parse_qs, unquote

from .connection import Connection
from .uploader import DataUploader
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
        self.result_config = (kwargs.pop('result_config', None) or {}).copy()
        self.connection = Connection(*args, **kwargs)
        self.query_result_cls = QueryResult
        self.helper = Helper
        self._debug = asbool(self.settings.get('debug', False))
        self._uploader = DataUploader(self, self.connection, self.settings, debug=self._debug,
                                      compress=self.settings.get('compress', False))

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
            gen, raw_data, with_column_types=with_column_types, **self.result_config)
        return result.get_result()

    def _iter_receive_result(self, query, query_id=None, with_column_types=False):
        raw_data = self.connection.query(query)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        gen = self._data_generator(raw_data)
        result = self.query_result_cls(
            gen, raw_data, with_column_types=with_column_types, **self.result_config)
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
        if len(query.split(' ')) < 3:
            raise Exception("Not standard insert/replace statement")
        table_name = query.split(' ')[2]
        batch_size = query.count(',') + 1
        if params is not None and len(params) > 0:
            if isinstance(params[0], tuple):
                tuple_ls = params
            else:
                tuple_ls = [tuple(params[i:i + batch_size]) for i in range(0, len(params), batch_size)]
            insert_rows = len(tuple_ls)
            self._uploader.upload_to_table_by_copy(table_name, tuple_ls)
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
        result_config = {}
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
            elif name == 'compress':
                settings[name] = asbool(value)
            elif name in timeouts:
                kwargs[name] = float(value)
            elif name == 'persist_cookies':
                kwargs[name] = asbool(value)
            elif name == 'null_to_none':
                result_config[name] = asbool(value)
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
        if result_config:
            kwargs['result_config'] = result_config

        return cls(host, **kwargs)

    def insert(self, database_name, table_name, data):
        """
        insert the data into database.table according to the file
        database_name: the target database
        table_name: the table which write into
        data: the data which write into, it's a list of tuple
        """
        # TODO: escape the database & table name
        self._uploader.upload_to_table_by_copy("%s.%s" % (database_name, table_name), data)

    def replace(self, database_name, table_name, conflict_keys, data):
        """
        replace the data into database.table according to the file
        database_name: the target database
        table_name: the table which write into
        conflict_keys: the key that use to replace into
        data: the data which write into, it's a list of tuple
        """
        self._uploader.replace_into_table("%s.%s" % (database_name, table_name), conflict_keys, data)

    def upload_to_stage(self, stage_dir, file_name, data):
        """
        upload the file to user stage
        :param stage_dir: target stage directory
        :param file_name: the target file name which placed into the stage_dir
        :param data: the data value or file handler
        :return:
        """
        return self._uploader.upload_to_stage(stage_dir, file_name, data)
