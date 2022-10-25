import configparser
from contextlib import contextmanager
import subprocess
from unittest import TestCase

from databend_driver.client import Client
from tests import log

file_config = configparser.ConfigParser()
file_config.read(['../setup.cfg'])

log.configure(file_config.get('log', 'level'))


class BaseTestCase(TestCase):
    required_server_version = None
    server_version = None

    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')

    client = None
    client_kwargs = None

    def _create_client(self, **kwargs):
        client_kwargs = {
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }
        client_kwargs.update(kwargs)
        return Client(self.host, **client_kwargs)

    def created_client(self, **kwargs):
        return self._create_client(**kwargs)
