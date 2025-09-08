

from setuptools import setup, find_packages


setup(name="tap-servicenow",
      version="0.0.1",
      description="Singer.io tap for extracting data from ServiceNow API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_servicenow"],
      install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "backoff==2.2.1",
        "parameterized"
      ],
      entry_points="""
          [console_scripts]
          tap-servicenow=tap_servicenow:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_servicenow": ["schemas/*.json"],
      },
      include_package_data=True,
)
