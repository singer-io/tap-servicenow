from setuptools import find_packages, setup

setup(
    name="tap-servicenow",
    version="0.0.1",
    description="Singer.io tap for extracting data from servicenow API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_servicenow"],
    install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.5",
        "backoff==2.2.1",
        "zeep==4.3.1",
        "parameterized==0.9.0",
    ],
    entry_points="""
          [console_scripts]
          tap-servicenow=tap_servicenow:main
      """,
    packages=find_packages(),
    package_data={
        "tap_servicenow": ["schemas/*.json"],
    },
    include_package_data=True,
)
