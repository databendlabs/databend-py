# Databend Python Driver

## Connection parameters

The driver supports various parameters that may be set as URL parameters or as properties passed to Client. Both of the
following examples are equivalent:

```python
# URL parameters
client = Client.from_url('http://root@localhost:8000/db?secure=False&copy_purge=True&debug=True')

# Client parameters
client = Client(
    host='tenant--warehouse.ch.datafusecloud.com',
    database="default",
    user="user",
    port="443",
    password="password", settings={"copy_purge": True, "force": True})
```

### Parameter References

| Parameter        | Description                                                                                              | Default     | example                                        |
|------------------|----------------------------------------------------------------------------------------------------------|-------------|------------------------------------------------|
| user             | username                                                                                                 | root        |                                                | 
| password         | password                                                                                                 | None        |                                                |                  | 
| port             | server port                                                                                              | None        |                                                |
| database         | selected database                                                                                        | default     |     
| secure           | Enable SSL                                                                                               | false       | http://root@localhost:8000/db?secure=False     |
| copy_purge       | If True, the command will purge the files in the stage after they are loaded successfully into the table | false       | http://root@localhost:8000/db?copy_purge=False |
| debug            | Enable debug log                                                                                         | False       | http://root@localhost:8000/db?debug=True       |
| persist_cookies  | if using cookies set by server to perform following requests.                                            | False       | http://root@localhost:8000/db?persist_cookies=True|
| null_to_none     | if the result data NULL which is of type str, change it to NoneType                                      | False       | http://root@localhost:8000/db?null_to_none=True|

