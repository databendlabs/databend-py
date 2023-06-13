from databend_py import Client


def insert():
    client = Client.from_url("http://root:root@localhost:8000")
    client.execute('DROP TABLE IF EXISTS test_upload')
    client.execute('CREATE TABLE if not exists test_upload (x Int32,y VARCHAR)')
    client.execute('DESC test_upload')
    client.insert("default", "test_upload", [(1, 'a'), (1, 'b')])
    _, upload_res = client.execute('select * from test_upload')
    # upload_res is [(1, 'a'), (1, 'b')]


def batch_insert():
    c = Client.from_url("http://root:root@localhost:8000")
    c.execute('DROP TABLE IF EXISTS test')
    c.execute('CREATE TABLE if not exists test (x Int32,y VARCHAR)')
    c.execute('DESC  test')
    _, r1 = c.execute('INSERT INTO test (x,y) VALUES (%,%)', [1, 'yy', 2, 'xx'])
    _, ss = c.execute('select * from test')
    # ss is [(1, 'yy'), (2, 'xx')]


def batch_insert_with_tuple():
    c = Client.from_url("http://root:root@localhost:8000")
    c.execute('DROP TABLE IF EXISTS test')
    c.execute('CREATE TABLE if not exists test (x Int32,y VARCHAR)')
    c.execute('DESC  test')
    # data is tuple list
    _, r1 = c.execute('INSERT INTO test (x,y) VALUES', [(3, 'aa'), (4, 'bb')])
    _, ss = c.execute('select * from test')
