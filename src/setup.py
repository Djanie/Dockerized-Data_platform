from setuptools import setup, find_packages

setup(
    name="data_pipeline_src",
    version="0.0.1",
    packages=find_packages(where="."),
    package_dir={"": "."},
)
