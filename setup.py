import os
from codecs import open

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))


def read_version():
    version_py = os.path.join(here, "databend_py", "VERSION")
    with open(version_py, encoding="utf-8") as f:
        first_line = f.readline()
        return first_line.strip()


github_url = "https://github.com/databendcloud/databend-py"

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="databend-py",
    version=read_version(),
    include_package_data=True,
    description="Python driver with native interface for Databend",
    long_description=long_description,
    url=github_url,
    packages=find_packages(".", exclude=["tests*"]),
    python_requires=">=3.4, <4",
    install_requires=[
        "pytz",
        "environs",
        "requests",
        "databend-driver>=0.11.3",
    ],
    author="Databend Cloud Team",
    author_email="hantmac@outlook.com",
    license="Apache License",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    keywords="databend db database cloud analytics",
    test_suite="pytest",
)
