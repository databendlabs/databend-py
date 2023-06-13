from databend_py import Client


def ordinary_query():
    client = Client.from_url("http://root:root@localhost:8000")
    _, res = client.execute("select 1", with_column_types=False)
    # res is [(1,)]

    column_type, res2 = client.execute("select 1", with_column_types=True)
    # column_type is [('1', 'UInt8')]
    # res2 [(1,)]
    print(column_type)
    print(res2)

    # create table/ drop table
    client.execute('DROP TABLE IF EXISTS test')
    client.execute('CREATE TABLE if not exists test (x Int32,y VARCHAR)')
