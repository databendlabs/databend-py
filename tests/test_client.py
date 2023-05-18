from databend_py import Client
from unittest import TestCase
import types, os


def sample_insert_data():
    return [(1, 'a'), (1, 'b')]


class DatabendPyTestCase(TestCase):
    def __init__(self, databend_url):
        super().__init__()
        self.databend_url = databend_url

    def assertHostsEqual(self, client, another, msg=None):
        self.assertEqual(client.connection.host, another, msg=msg)

    def test_simple(self):
        c = Client.from_url('https://app.databend.com:443?secure=True&copy_purge=True&debug=True')

        self.assertHostsEqual(c, 'app.databend.com')
        self.assertEqual(c.connection.database, 'default')
        self.assertEqual(c.connection.user, 'root')
        self.assertEqual(c.connection.copy_purge, True)
        self.assertEqual(c.settings.get("debug"), True)

        c = Client.from_url('https://host:443/db')

        self.assertHostsEqual(c, 'host')
        self.assertEqual(c.connection.database, 'db')
        self.assertEqual(c.connection.password, '')

        c = Client.from_url("databend://localhost:8000/default?secure=true")
        self.assertEqual(c.connection.schema, "https")
        c = Client.from_url("databend://root:root@localhost:8000/default")
        self.assertEqual(c.connection.schema, "http")
        c = Client.from_url("databend://root:root@localhost:8000/default?secure=false")
        self.assertEqual(c.connection.schema, "http")

    def test_session_settings(self):
        session_settings = {"db": "database"}
        c = Client(host="localhost", port=8000, user="root", password="root", session_settings={"db": "database"})
        self.assertEqual(c.connection.client_session, session_settings)

    def test_ordinary_query(self):
        select_test = '''
        select
      null as db,
      name as name,
      database as schema,
      if(engine = 'VIEW', 'view', 'table') as type
    from system.tables
    where database = 'default';
        '''
        # if use the host from databend cloud, must set the 'ADDITIONAL_HEADERS':
        # os.environ['ADDITIONAL_HEADERS'] = 'X-DATABENDCLOUD-TENANT=TENANT,X-DATABENDCLOUD-WAREHOUSE=WAREHOUSE'
        c = Client.from_url(self.databend_url)
        _, r = c.execute("select 1", with_column_types=False)
        self.assertEqual(r, ([(1,)]))
        column_types, _ = c.execute(select_test, with_column_types=True)
        print(column_types)
        self.assertEqual(column_types, [('db', 'NULL'), ('name', 'String'), ('schema', 'String'), ('type', 'String')])

        # test with_column_types=True
        r = c.execute("select 1", with_column_types=True)
        self.assertEqual(r, ([('1', 'UInt8')], [(1,)]))

    def test_batch_insert(self):
        c = Client.from_url(self.databend_url)
        c.execute('DROP TABLE IF EXISTS test')
        c.execute('CREATE TABLE if not exists test (x Int32,y VARCHAR)')
        c.execute('DESC  test')
        _, r1 = c.execute('INSERT INTO test (x,y) VALUES (%,%)', [1, 'yy', 2, 'xx'])
        self.assertEqual(r1, 2)
        _, ss = c.execute('select * from test')
        print(ss)
        self.assertEqual(ss, [(1, 'yy'), (2, 'xx')])

    def test_batch_insert_with_tuple(self):
        c = Client.from_url(self.databend_url)
        c.execute('DROP TABLE IF EXISTS test')
        c.execute('CREATE TABLE if not exists test (x Int32,y VARCHAR)')
        c.execute('DESC  test')
        _, r1 = c.execute('INSERT INTO test (x,y) VALUES (%,%)', [(3, 'aa'), (4, 'bb')])
        self.assertEqual(r1, 2)
        _, ss = c.execute('select * from test')
        self.assertEqual(ss, [(3, 'aa'), (4, 'bb')])

    def test_iter_query(self):
        client = Client.from_url(self.databend_url)
        result = client.execute_iter("select 1", with_column_types=False)
        self.assertIsInstance(result, types.GeneratorType)
        result_list = [i for i in result]
        print(result_list)
        self.assertEqual(result_list, [1])
        self.assertEqual(list(result), [])

    def test_upload(self):
        client = Client.from_url(self.databend_url)
        client.execute('DROP TABLE IF EXISTS test_upload')
        client.execute('CREATE TABLE if not exists test_upload (x Int32,y VARCHAR)')
        client.execute('DESC test_upload')
        client.upload("default.test_upload", [(1, 'a'), (1, 'b')])
        _, upload_res = client.execute('select * from test_upload')
        self.assertEqual(upload_res, [(1, 'a'), (1, 'b')])

    def test_upload_to_stage(self):
        client = Client.from_url(self.databend_url)
        stage_path = client.upload_to_stage('@~', "upload.csv", [(1, 'a'), (1, 'b')])
        self.assertEqual(stage_path, "@~/upload.csv")

    def tearDown(self):
        client = Client.from_url(self.databend_url)
        client.execute('DROP TABLE IF EXISTS test')
        client.disconnect()


if __name__ == '__main__':
    print("start test......")
    # os.environ['TEST_DATABEND_DSN'] = "http://root:@localhost:8000"
    dt = DatabendPyTestCase(databend_url=os.getenv("TEST_DATABEND_DSN"))
    dt.test_simple()
    dt.test_ordinary_query()
    dt.test_batch_insert()
    dt.test_iter_query()
    dt.test_upload()
    dt.tearDown()
    print("end test.....")
