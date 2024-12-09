from databend_py import Client


def replace_into():
    client = Client.from_url("http://root:root@localhost:8000")
    client.execute("DROP TABLE IF EXISTS test_replace")
    client.execute("CREATE TABLE if not exists test_replace (x Int32,y VARCHAR)")
    client.execute("DESC test_replace")
    client.replace("default", "test_replace", ["x"], [(1, "a"), (2, "b")])
    client.replace("default", "test_replace", ["x"], [(1, "c"), (2, "d")])
    _, upload_res = client.execute("select * from test_replace")
    # upload_res is [(1, 'c\r'), (2, 'd\r')]
