import io
import re
from urllib.parse import urlparse, parse_qs, unquote
from .connection import Connection
from .util.helper import asbool, Helper
from .util.escape import escape_params
from .result import QueryResult
import json, operator, csv, uuid, requests, time, os


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

    def __enter__(self):
        return self

    def disconnect(self):
        self.disconnect_connection()

    def disconnect_connection(self):
        self.connection.disconnect()

    def data_generator(self, raw_data):

        while raw_data['next_uri'] is not None:
            try:
                raw_data = self.receive_data(raw_data['next_uri'])
                if not raw_data['data']:
                    break
                yield raw_data

            except (Exception, KeyboardInterrupt):
                self.disconnect()
                raise

    def receive_data(self, next_uri: str):
        resp = self.connection.next_page(next_uri)
        raw_data = json.loads(resp.content)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        return raw_data

    def receive_result(self, query, query_id=None, with_column_types=False):
        raw_data = self.connection.query(query, None)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        gen = self.data_generator(raw_data)
        result = self.query_result_cls(
            gen, raw_data, with_column_types=with_column_types)
        return result.get_result()

    def iter_receive_result(self, query, query_id=None, with_column_types=False):
        raw_data = self.connection.query(query, None)
        helper = self.helper()
        helper.response = raw_data
        helper.check_error()
        gen = self.data_generator(raw_data)
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
            rv = self.process_insert_query(query, params)
            return [], rv

        column_types, rv = self.process_ordinary_query(
            query, params=params, with_column_types=with_column_types,
            query_id=query_id)
        return column_types, rv

    # params = [(1,),(2,)] or params = [(1,2),(2,3)]
    def process_insert_query(self, query, params):
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
            filename = self.generate_csv(tuple_ls)
            csv_data = self.get_csv_data(filename)
            self.sync_csv_file_into_table(filename, csv_data, table_name)
            insert_rows = len(tuple_ls)

        return insert_rows

    def process_ordinary_query(self, query, params=None, with_column_types=False,
                               query_id=None):
        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )
        return self.receive_result(query, query_id=query_id, with_column_types=with_column_types, )

    def execute_iter(self, query, params=None, with_column_types=False,
                     query_id=None, settings=None):
        if params is not None:
            query = self.substitute_params(
                query, params, self.connection.context
            )
        return self.iter_receive_result(query, query_id=query_id, with_column_types=with_column_types)

    def iter_process_ordinary_query(self, query, with_column_types=False, query_id=None):
        return self.iter_receive_result(query, query_id=query_id, with_column_types=with_column_types)

    def substitute_params(self, query, params, context):
        if not isinstance(params, dict):
            raise ValueError('Parameters are expected in dict form')

        escaped = escape_params(params, context)
        return query % escaped

    @classmethod
    def from_url(cls, url):
        """
        Return a client configured from the given URL.

        For example::

            http://[user:password]@localhost:8000/default
            http://[user:password]@localhost:8000/default
            databend://[user:password]@localhost:8000/default

        Any additional querystring arguments will be passed along to
        the Connection class's initializer.
        """
        url = urlparse(url)

        settings = {}
        kwargs = {}

        host = url.hostname
        port = url.port if url.port is not None else 443

        if url.port is not None:
            kwargs['port'] = url.port
            port = url.port

        path = url.path.replace('/', '', 1)
        if path:
            kwargs['database'] = path

        if url.username is not None:
            kwargs['user'] = unquote(url.username)

        if url.password is not None:
            kwargs['password'] = unquote(url.password)

        if url.scheme == 'http':
            kwargs['secure'] = False
        if url.scheme == 'https':
            kwargs['secure'] = True

        for name, value in parse_qs(url.query).items():
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
            elif name in timeouts:
                kwargs[name] = float(value)
            else:
                settings[name] = value

        if settings:
            kwargs['settings'] = settings

        return cls(host, **kwargs)

    def generate_csv(self, bindings):
        file_name = f'{uuid.uuid4()}.csv'
        with open(file_name, "w+") as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerows(bindings)

        return file_name

    def get_csv_data(self, filename):
        with open(filename, "r") as csvfile:
            return io.StringIO(csvfile.read())

    def stage_csv_file(self, filename, data):
        stage_path = "@~/%s" % filename
        _, row = self.execute('presign upload %s' % stage_path)
        presigned_url = row[0][2]
        headers = json.loads(row[0][1])
        resp = requests.put(presigned_url, headers=headers, data=data)
        resp.raise_for_status()
        return stage_path

    def sync_csv_file_into_table(self, filename, data, table):
        start = time.time()
        stage_path = self.stage_csv_file(filename, data)
        _, _ = self.execute("COPY INTO %s FROM %s FILE_FORMAT = (type = CSV)" % (table, stage_path))
        print("sync %s duration:%ss" % (filename, int(time.time() - start)))
        os.remove(filename)
