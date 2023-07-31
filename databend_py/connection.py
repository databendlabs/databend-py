import json
import os
import base64
import time
from requests.auth import HTTPBasicAuth

import environs
import requests
from . import log
from . import defines
from .context import Context
from databend_py.errors import WarehouseTimeoutException, UnexpectedException, ServerException
from databend_py.retry import retry
from databend_py.sdk_info import sdk_info

headers = {'Content-Type': 'application/json', 'User-Agent': sdk_info(), 'Accept': 'application/json',
           'X-DATABEND-ROUTE': 'warehouse'}


class ServerInfo(object):
    def __init__(self, name, version_major, version_minor, version_patch,
                 revision, timezone, display_name):
        self.name = name
        self.version_major = version_major
        self.version_minor = version_minor
        self.version_patch = version_patch
        self.revision = revision
        self.timezone = timezone
        self.display_name = display_name

        super(ServerInfo, self).__init__()

    def version_tuple(self):
        return self.version_major, self.version_minor, self.version_patch

    def __repr__(self):
        version = '%s.%s.%s' % (
            self.version_major, self.version_minor, self.version_patch
        )
        items = [
            ('name', self.name),
            ('version', version),
            ('revision', self.revision),
            ('timezone', self.timezone),
            ('display_name', self.display_name)
        ]

        params = ', '.join('{}={}'.format(key, value) for key, value in items)
        return '<ServerInfo(%s)>' % (params)


def get_error(response):
    if response['error'] is None:
        return None

    # Wrap errno into msg, for result check
    return ServerException(
        response['error']['message'],
        response['error']['code'])


class Connection(object):
    # Databend http handler doc: https://databend.rs/doc/reference/api/rest

    # Call connect(**driver)
    # driver is a dict contains:
    # {
    #   'user': 'root',
    #   'host': '127.0.0.1',
    #   'port': 3307,
    #   'database': 'default'
    # }
    def __init__(self, host, port=None, user=defines.DEFAULT_USER, password=defines.DEFAULT_PASSWORD,
                 database=defines.DEFAULT_DATABASE, secure=False, copy_purge=False, session_settings=None,
                 persist_cookies=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.secure = secure
        self.copy_purge = copy_purge
        self.session_max_idle_time = defines.DEFAULT_SESSION_IDLE_TIME
        self.client_session = session_settings
        self.additional_headers = dict()
        self.query_option = None
        self.context = Context()
        self.requests_session = requests.Session()
        self.schema = 'http'
        if self.secure:
            self.schema = 'https'
        e = environs.Env()
        if os.getenv("ADDITIONAL_HEADERS") is not None:
            print(os.getenv("ADDITIONAL_HEADERS"))
            self.additional_headers = e.dict("ADDITIONAL_HEADERS")
        self.persist_cookies = persist_cookies
        self.cookies = None

    def default_session(self):
        return {"database": self.database}

    def make_headers(self):
        if "Authorization" not in self.additional_headers:
            return {
                **headers, **self.additional_headers,
                "Authorization":
                    "Basic " + base64.b64encode("{}:{}".format(
                        self.user, self.password).encode(encoding="utf-8")).decode()
            }
        else:
            return {**headers, **self.additional_headers}

    def get_description(self):
        return '{}:{}'.format(self.host, self.port)

    def disconnect(self):
        self.client_session = dict()

    @retry(times=5, exceptions=WarehouseTimeoutException)
    def do_query(self, url, query_sql):
        response = self.requests_session.post(url,
                                              data=json.dumps(query_sql),
                                              headers=self.make_headers(),
                                              auth=HTTPBasicAuth(self.user, self.password),
                                              verify=True)
        try:
            resp_dict = json.loads(response.content)
        except json.decoder.JSONDecodeError:
            raise UnexpectedException("failed to parse response: %s" % response.content)
        if resp_dict and resp_dict.get('error') and "no endpoint" in resp_dict.get('error'):
            raise WarehouseTimeoutException
        if resp_dict and resp_dict.get('error'):
            raise UnexpectedException("failed to query: %s" % response.content)
        if self.persist_cookies:
            self.cookies = response.cookies
        return resp_dict

    def query(self, statement):
        url = self.format_url()
        log.logger.debug(f"http sql: {statement}")
        query_sql = {'sql': statement, "string_fields": True}
        if self.client_session is not None and len(self.client_session) != 0:
            if "database" not in self.client_session:
                self.client_session = self.default_session()
            query_sql['session'] = self.client_session
        else:
            self.client_session = self.default_session()
            query_sql['session'] = self.client_session
        log.logger.debug(f"http headers {self.make_headers()}")
        try:
            resp_dict = self.do_query(url, query_sql)
            self.client_session = resp_dict.get("session", self.default_session())
            return resp_dict
        except Exception as err:
            log.logger.error(
                f"http error on {url}, SQL: {statement} error msg:{str(err)}"
            )
            raise

    def format_url(self):
        if self.schema == "https" and self.port is None:
            self.port = 443
        elif self.schema == "http" and self.port is None:
            self.port = 80
        return f"{self.schema}://{self.host}:{self.port}/v1/query/"

    def reset_session(self):
        self.client_session = dict()

    def next_page(self, next_uri):
        url = "{}://{}:{}{}".format(self.schema, self.host, self.port, next_uri)
        return self.requests_session.get(url=url, headers=self.make_headers(), cookies=self.cookies)

    # return a list of response util empty next_uri
    def query_with_session(self, statement):
        response_list = list()
        response = self.query(statement)
        log.logger.debug(f"response content: {response}")
        response_list.append(response)
        start_time = time.time()
        time_limit = 12
        session = response.get("session", self.default_session())
        if session:
            self.client_session = session
        while response['next_uri'] is not None:
            resp = self.next_page(response['next_uri'])
            response = json.loads(resp.content)
            log.logger.debug(f"Sql in progress, fetch next_uri content: {response}")
            self.check_error(response)
            session = response.get("session", self.default_session())
            if session:
                self.client_session = session
            response_list.append(response)
            if time.time() - start_time > time_limit:
                log.logger.warning(
                    f"after waited for {time_limit} secs, query still not finished (next uri not none)!"
                )
        return response_list

    def check_error(self, resp):
        error = get_error(resp)
        if error:
            raise error
