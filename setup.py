#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-linkedin-ads',
      version='2.4.1',
      description='Singer.io tap for extracting data from the LinkedIn Marketing Ads API API 2.0',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_linkedin_ads'],
      install_requires=[
          'singer-python@git+https://github.com/railsware/singer-python/@565fcb685e6a636c3ad21e421a0662da47757573',
          'requests>=2.31.0',
          'backoff~=2.2.1',
      ],
      extras_require={
        'dev': [
            'pytest',
            'responses',
        ]
      },
      entry_points='''
          [console_scripts]
          tap-linkedin-ads=tap_linkedin_ads:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_linkedin_ads': [
              'schemas/*.json'
          ]
      })
