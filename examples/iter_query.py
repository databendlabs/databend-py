from databend_py import Client


def iter_query():
    client = Client.from_url("http://root:root@localhost:8000")
    result = client.execute_iter("select 1, 2, 3 from numbers(3)", with_column_types=False)
    result_list = [i for i in result]
    # result_list is [(1, 2, 3), (1, 2, 3), (1, 2, 3)]
    print(result_list)
