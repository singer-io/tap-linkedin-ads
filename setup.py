#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-linkedin-ads',
      version='2.4.0',
      description='Singer.io tap for extracting data from the LinkedIn Marketing Ads API API 2.0',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_linkedin_ads'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.32.3',
          'singer-python==6.1.0'
      ],
      extras_require={
        'dev': [
            'ipdb',
            'pylint',
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
