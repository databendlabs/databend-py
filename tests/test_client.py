from databend_py.client import Client
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

        c.execute('DROP TABLE IF EXISTS test')
        c.execute('CREATE TABLE if not exists test (x Int32,y VARCHAR)')
        c.execute("DESC TABLE test")
        r1 = c.execute('INSERT INTO test (x,y) VALUES', [(1, 'yy')])
        # insert_rows = 1
        self.assertEqual(r1, 1)
        ss = c.execute('select * from test')
        self.assertEqual(ss, [('1', 'yy')])

    def test_iter_query(self):
        c = Client.from_url('http://root:@localhost:8081')
        self.assertEqual(c.connection.user, 'root')

        result = c.execute_iter("select 1", with_column_types=False)

        self.assertIsInstance(result, types.GeneratorType)
        result_list = [i for i in result]
        print(result_list)
        self.assertEqual(result_list, ['1'])

        self.assertEqual(list(result), [])
