import os
import unittest
import types
from databend_py import Client


def sample_insert_data():
    return [(1, "a"), (1, "b")]


def create_csv():
    import csv

    with open("upload.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([1, "a"])
        writer.writerow([1, "b"])


class DatabendPyTestCase(unittest.TestCase):
    databend_url = None

    def setUp(self):
        self.databend_url = os.getenv("TEST_DATABEND_DSN")

    def assertHostsEqual(self, client, another, msg=None):
        self.assertEqual(client.connection.host, another, msg=msg)

    def test_simple(self):
        c = Client.from_url(
            "https://app.databend.com:443?secure=True&copy_purge=True&debug=True"
        )

        self.assertHostsEqual(c, "app.databend.com")
        self.assertEqual(c.connection.database, "default")
        self.assertEqual(c.connection.user, "root")
        self.assertEqual(c.connection.copy_purge, True)
        self.assertEqual(c.settings.get("debug"), True)

        c = Client.from_url("https://host:443/db")

        self.assertHostsEqual(c, "host")
        self.assertEqual(c.connection.database, "db")
        self.assertEqual(c.connection.password, "")

        c = Client.from_url("databend://localhost:8000/default?secure=true")
        self.assertEqual(c.connection.schema, "https")
        c = Client.from_url("databend://root:root@localhost:8000/default")
        self.assertEqual(c.connection.schema, "http")
        c = Client.from_url("databend://root:root@localhost:8000/default?secure=false")
        self.assertEqual(c.connection.schema, "http")
        c = Client.from_url("databend://root:root@localhost:8000/default?compress=True")
        self.assertEqual(c._uploader._compress, True)
        self.assertEqual(c.connection.connect_timeout, 180)
        self.assertEqual(c.connection.read_timeout, 180)

        c = Client.from_url(
            "databend://root:root@localhost:8000/default?connect_timeout=30&read_timeout=30"
        )
        self.assertEqual(c.connection.connect_timeout, 30)
        self.assertEqual(c.connection.read_timeout, 30)

        self.assertEqual(c.connection.persist_cookies, False)
        c = Client.from_url(
            "https://root:root@localhost:8000?persist_cookies=True&tenant=tn1&warehouse=wh1"
        )
        self.assertEqual(c.connection.persist_cookies, True)
        self.assertEqual(c.connection.tenant, "tn1")
        self.assertEqual(c.connection.warehouse, "wh1")

    def test_session_settings(self):
        session_settings = {"db": "database"}
        c = Client(
            host="localhost",
            port=8000,
            user="root",
            password="root",
            session_settings={"db": "database"},
        )
        self.assertEqual(c.connection.client_session, session_settings)

    def test_ordinary_query(self):
        select_test = """
        select
      null as db,
      name as name,
      database as schema,
      if(engine = 'VIEW', 'view', 'table') as type
    from system.tables
    where database = 'default';
        """
        # if use the host from databend cloud, must set the 'ADDITIONAL_HEADERS':
        # os.environ['ADDITIONAL_HEADERS'] = 'X-DATABENDCLOUD-TENANT=TENANT,X-DATABENDCLOUD-WAREHOUSE=WAREHOUSE'
        c = Client.from_url(self.databend_url)
        _, r = c.execute("select 1", with_column_types=False)
        self.assertEqual(r, ([(1,)]))
        column_types, _ = c.execute(select_test, with_column_types=True)
        print(column_types)
        self.assertEqual(
            column_types,
            [
                ("db", "NULL"),
                ("name", "String"),
                ("schema", "String"),
                ("type", "String"),
            ],
        )

        # test with_column_types=True
        r = c.execute("select 1", with_column_types=True)
        self.assertEqual(r, ([("1", "UInt8")], [(1,)]))

    def test_batch_insert(self):
        c = Client.from_url(self.databend_url)
        c.execute("DROP TABLE IF EXISTS test")
        c.execute("CREATE TABLE if not exists test (x Int32,y VARCHAR)")
        c.execute("DESC  test")
        _, r1 = c.execute("INSERT INTO test (x,y) VALUES (%,%)", [1, "yy", 2, "xx"])
        self.assertEqual(r1, 2)
        _, ss = c.execute("select * from test")
        print(ss)
        self.assertEqual(ss, [(1, "yy"), (2, "xx")])

    def test_batch_insert_with_tuple(self):
        c = Client.from_url(self.databend_url)
        c.execute("DROP TABLE IF EXISTS test")
        c.execute("CREATE TABLE if not exists test (x Int32,y VARCHAR)")
        c.execute("DESC  test")
        _, r1 = c.execute("INSERT INTO test (x,y) VALUES", [(3, "aa"), (4, "bb")])
        self.assertEqual(r1, 2)
        _, ss = c.execute("select * from test")
        self.assertEqual(ss, [(3, "aa"), (4, "bb")])

    def test_batch_insert_with_dict_list(self):
        c = Client.from_url(self.databend_url)
        c.execute("DROP TABLE IF EXISTS test")
        c.execute("CREATE TABLE if not exists test (x Int32,y VARCHAR)")
        c.execute("DESC  test")
        _, r1 = c.execute("INSERT INTO test (x,y) VALUES", [{"x": 5, "y": "cc"}, {"x": 6, "y": "dd"}])
        self.assertEqual(r1, 2)
        _, ss = c.execute("select * from test")
        self.assertEqual(ss, [(5, "cc"), (6, "dd")])

    def test_batch_insert_with_dict_multi_fields(self):
        c = Client.from_url(self.databend_url)
        c.execute("DROP TABLE IF EXISTS test")
        c.execute("CREATE TABLE if not exists test (id int, x Int32, y VARCHAR, z Int32)")
        c.execute("DESC  test")
        _, r1 = c.execute("INSERT INTO test (x,y) VALUES", [{"x": 7, "y": "ee"}, {"x": 8, "y": "ff"}])
        self.assertEqual(r1, 2)
        _, ss = c.execute("select * from test")
        self.assertEqual(ss, [('NULL', 7, 'ee', 'NULL'), ('NULL', 8, 'ff', 'NULL')])

    def test_iter_query(self):
        client = Client.from_url(self.databend_url)
        result = client.execute_iter("select 1", with_column_types=False)
        self.assertIsInstance(result, types.GeneratorType)
        result_list = [i for i in result]
        print(result_list)
        self.assertEqual(result_list, [1])
        self.assertEqual(list(result), [])

    def test_insert(self):
        client = Client.from_url(self.databend_url)
        client.execute("DROP TABLE IF EXISTS test_upload")
        client.execute("CREATE TABLE if not exists test_upload (x Int32,y VARCHAR)")
        client.execute("DESC test_upload")
        client.insert("default", "test_upload", [(1, "a"), (1, "b")])
        _, upload_res = client.execute("select * from test_upload")
        self.assertEqual(upload_res, [(1, "a"), (1, "b")])

    def test_replace(self):
        client = Client.from_url(self.databend_url)
        client.execute("DROP TABLE IF EXISTS test_replace")
        client.execute("CREATE TABLE if not exists test_replace (x Int32,y VARCHAR)")
        client.execute("DESC test_replace")
        client.replace("default", "test_replace", ["x"], [(1, "a"), (2, "b")])
        client.replace("default", "test_replace", ["x"], [(1, "c"), (2, "d")])
        _, upload_res = client.execute("select * from test_replace")
        self.assertEqual(upload_res, [(1, "c\r"), (2, "d\r")])

    def test_insert_with_compress(self):
        client = Client.from_url(self.databend_url + "?compress=True&debug=True")
        self.assertEqual(client._uploader._compress, True)
        client.execute("DROP TABLE IF EXISTS test_upload")
        client.execute("CREATE TABLE if not exists test_upload (x Int32,y VARCHAR)")
        client.execute("DESC test_upload")
        client.insert("default", "test_upload", [(1, "a"), (1, "b")])
        _, upload_res = client.execute("select * from test_upload")
        self.assertEqual(upload_res, [(1, "a"), (1, "b")])

    def test_upload_to_stage(self):
        client = Client.from_url(self.databend_url)
        stage_path = client.upload_to_stage("@~", "upload.csv", [(1, "a"), (1, "b")])
        self.assertEqual(stage_path, "@~/upload.csv")

    def test_upload_file_to_stage(self):
        create_csv()
        client = Client.from_url(self.databend_url)
        with open("upload.csv", "rb") as f:
            stage_path = client.upload_to_stage("@~", "upload.csv", f)
            print(stage_path)
            self.assertEqual(stage_path, "@~/upload.csv")

        os.remove("upload.csv")

    def test_select_over_paging(self):
        expected_column = [("number", "UInt64")]
        client = Client.from_url(self.databend_url)
        columns, data = client.execute(
            "SELECT * FROM numbers(10001)", with_column_types=True
        )
        self.assertEqual(expected_column, columns)

    def tearDown(self):
        client = Client.from_url(self.databend_url)
        client.execute("DROP TABLE IF EXISTS test")
        client.disconnect()

    def test_cookies(self):
        client = Client.from_url(self.databend_url)
        client.execute("select 1")
        self.assertIsNone(client.connection.cookies)

        if "?" in self.databend_url:
            url_with_persist_cookies = f"{self.databend_url}&persist_cookies=true"
        else:
            url_with_persist_cookies = f"{self.databend_url}?persist_cookies=true"
        client = Client.from_url(url_with_persist_cookies)
        client.execute("select 1")
        # self.assertIsNotNone(client.connection.cookies)

    def test_null_to_none(self):
        client = Client.from_url(self.databend_url)
        _, data = client.execute("select NULL as test")
        self.assertEqual(data[0][0], "NULL")

        if "?" in self.databend_url:
            url_with_null_to_none = f"{self.databend_url}&null_to_none=true"
        else:
            url_with_null_to_none = f"{self.databend_url}?null_to_none=true"
        client = Client.from_url(url_with_null_to_none)
        _, data = client.execute("select NULL as test")
        self.assertIsNone(data[0][0])

    def test_special_chars(self):
        client = Client.from_url(self.databend_url)
        client.execute("create or replace table test_special_chars (x string)")
        client.execute("INSERT INTO test_special_chars (x) VALUES", [("ó")])
        _, data = client.execute("select * from test_special_chars")
        self.assertEqual(data, [("ó",)])

    def test_set_query_id_header(self):
        os.environ["ADDITIONAL_HEADERS"] = (
            "X-DATABENDCLOUD-TENANT=TENANT,X-DATABENDCLOUD-WAREHOUSE=WAREHOUSE"
        )
        client = Client.from_url(self.databend_url)
        self.assertEqual(
            "X-DATABENDCLOUD-TENANT" in client.connection.additional_headers, True
        )
        self.assertEqual(
            client.connection.additional_headers["X-DATABENDCLOUD-TENANT"], "TENANT"
        )
        client.execute("select 1")
        execute_query_id1 = client.connection.additional_headers["X-DATABEND-QUERY-ID"]
        self.assertEqual(
            "X-DATABEND-QUERY-ID" in client.connection.additional_headers, True
        )
        client.execute("select 2")
        self.assertNotEqual(
            execute_query_id1,
            client.connection.additional_headers["X-DATABEND-QUERY-ID"],
        )

    def test_commit(self):
        client = Client.from_url(self.databend_url)
        client.execute("create or replace table test_commit (x int)")
        client.begin()
        client.execute("insert into test_commit values (1)")
        _, data = client.execute("select * from test_commit")
        self.assertEqual(data, [(1,)])

        client2 = Client.from_url(self.databend_url)
        client2.begin()
        client2.execute("insert into test_commit values (2)")
        _, data = client2.execute("select * from test_commit")
        self.assertEqual(data, [(2,)])

        client.commit()
        _, data = client.execute("select * from test_commit")
        self.assertEqual(data, [(1,)])

    def test_rollback(self):
        client = Client.from_url(self.databend_url)
        client.execute("create or replace table test_rollback (x int)")
        client.begin()
        client.execute("insert into test_rollback values (1)")
        _, data = client.execute("select * from test_rollback")
        self.assertEqual(data, [(1,)])

        client2 = Client.from_url(self.databend_url)
        client2.begin()
        client2.execute("insert into test_rollback values (2)")
        _, data = client2.execute("select * from test_rollback")
        self.assertEqual(data, [(2,)])

        client.rollback()
        _, data = client.execute("select * from test_rollback")
        self.assertEqual(data, [])

    def test_cast_bool(self):
        client = Client.from_url(self.databend_url)
        _, data = client.execute("select 'False'::boolean union select 'True'::boolean")
        self.assertEqual(len(data), 2)

    def test_temp_table(self):
        client = Client.from_url(self.databend_url)
        client.execute("create temp table t1(a int)")
        client.execute("insert into t1 values (1)")
        _, data = client.execute("select * from t1")
        self.assertEqual(data, [(1,)])
        client.execute("drop table t1")


if __name__ == "__main__":
    unittest.main()
