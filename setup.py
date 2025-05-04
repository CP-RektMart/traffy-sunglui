from setuptools import setup, find_packages

setup(
    name="data_eng",
    version="0.1",
    packages=find_packages(include=["data_eng", "data_eng.*"]),
)
