from databend_driver.client import Client
from tests.testcase import TestCase
import types


class ClientFromUrlTestCase(TestCase):
    def assertHostsEqual(self, client, another, msg=None):
        self.assertEqual(client.connection.host, another, msg=msg)

    def test_simple(self):
        c = Client.from_url('https://app.databend.com:443')

        self.assertHostsEqual(c, 'app.databend.com')
        self.assertEqual(c.connection.database, 'default')
        self.assertEqual(c.connection.user, 'root')

        c = Client.from_url('https://host:443/db')

        self.assertHostsEqual(c, 'host')
        self.assertEqual(c.connection.database, 'db')
        self.assertEqual(c.connection.password, '')

    def test_ordinary_query(self):
        c = Client.from_url('http://localhost:8081')
        r = c.execute("select 1", with_column_types=False)
        self.assertEqual(r, [('1',)])

        # test with_column_types=False
        r = c.execute("select 1", with_column_types=True)
        self.assertEqual(r, [('1', 'UInt8'), ('1',)])

    def test_iter_query(self):
        c = Client.from_url('http://localhost:8081')

        result = c.execute_iter("show tables", with_column_types=False)

        self.assertIsInstance(result, types.GeneratorType)
        print([i for i in result])

        self.assertEqual(list(result), [])
