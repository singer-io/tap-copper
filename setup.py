

from setuptools import setup, find_packages


setup(name="tap-copper",
      version="1.0.0",
      description="Singer.io tap for extracting data from copper API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_copper"],
      install_requires=[
        "singer-python==5.12.1",
        "requests==2.31.0",
      ],
      entry_points="""
          [console_scripts]
          tap-copper=tap_copper:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_copper": ["schemas/*.json"],
      },
      include_package_data=True,
)