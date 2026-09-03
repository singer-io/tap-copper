

from setuptools import setup, find_packages


setup(name="tap-copper",
      version="0.2.0",
      description="Singer.io tap for extracting data from copper API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_copper"],
      install_requires=[
        "singer-python==6.8.0",
        "requests==2.34.2",
        "backoff==2.2.1",
        "parameterized"
      ],
      entry_points="""
          [console_scripts]
          tap-copper=tap_copper:main
      """,
      packages=find_packages(),
      package_data={
          "tap_copper": ["schemas/*.json"],
      },
      include_package_data=True,
)
