import os
import re
from codecs import open

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))


def read_version():
    regexp = re.compile(r'^VERSION\W*=\W*\(([^\(\)]*)\)')
    init_py = os.path.join(here, 'databend_driver', '__init__.py')
    with open(init_py, encoding='utf-8') as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1).replace(', ', '.')
        else:
            raise RuntimeError(
                'Cannot find version in databend_driver/__init__.py'
            )


github_url = 'https://github.com/databendcloud/databend-py'

with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='databend-driver',
    version=read_version(),

    description='Python driver with native interface for Databend',
    long_description=long_description,

    url=github_url,
    packages=['databend_driver'],

    author='Databend Cloud Team',
    author_email='hantmac@outlook.com',

    license='Apache License',
)
