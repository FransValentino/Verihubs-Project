from setuptools import find_packages, setup

setup(
    name="verihubs_dagster",
    packages=find_packages(exclude=["verihubs_dagster_tests"]),
    install_requires=[
        "dagster",
        "duckdb",
        "pandas",
        "matplotlib",
        "seaborn"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
