import os

here = os.path.abspath(os.path.dirname(__file__))


def sdk_version():
    version_py = os.path.join(here, 'VERSION')
    with open(version_py, encoding='utf-8') as f:
        first_line = f.readline()
        return first_line.strip()


def sdk_lan():
    return "databend-py"


def sdk_info():
    return f"{sdk_lan()}/{sdk_version()}"
