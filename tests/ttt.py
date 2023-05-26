def ttt():
    import databend_py
    conn = databend_py.Client.from_url("http://root:root@localhost:8000")
    columns, data = conn.execute('SELECT * FROM numbers(20060)', with_column_types=True)
    print(columns)
    print(data)


if __name__ == '__main__':
    ttt()