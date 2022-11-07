from urllib.parse import urlparse, parse_qs, unquote
from .connection import Connection
from .util.helper import asbool, Helper
from .util.escape import escape_params
from .result import QueryResult
import json, operator


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
                if not raw_data:
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
        for rows in result.get_result():
            for row in rows:
                yield row

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
            rv = self.process_insert_query(query, params)
            return rv

        rv = self.process_ordinary_query(
            query, params=params, with_column_types=with_column_types,
            query_id=query_id)
        return rv

    # params = [(1,),(2,)] or params = [(1,2),(2,3)]
    def process_insert_query(self, query, params):
        insert_rows = 0
        if params is not None:
            for p in params:
                if len(p) == 1:
                    s = f'{p}'.replace(',', '')
                    q = f'{query} {s}'
                    self.connection.query_with_session(q)
                    insert_rows += 1
                else:
                    q = f'{query} {p}'
                    self.connection.query_with_session(q)
                    insert_rows += 1

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

            http://[user:password]@localhost:9000/default
            http://[user:password]@localhost:9440/default

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

        if url.scheme == 'https':
            kwargs['secure'] = True

        for name, value in parse_qs(url.query).items():
            if not value or not len(value):
                continue
            if url.scheme == 'https':
                kwargs['secure'] = True

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
