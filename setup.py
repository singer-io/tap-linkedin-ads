#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-linkedin-ads',
      version='1.2.0',
      description='Singer.io tap for extracting data from the LinkedIn Marketing Ads API API 2.0',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_linkedin_ads'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'singer-python==5.8.1'
      ],
      extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.4.4',
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
