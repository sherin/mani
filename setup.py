#!/usr/bin/env python
from setuptools import setup


setup(name='mani',
      version='0.0.1',
      install_requires=['redis',
                        'python-redis-lock'],
      setup_requires=['setuptools'],
      tests_require=['flake8'],
      description='Distributed cron',
      packages=['mani'],
      package_data={'mani.version': ['VERSION']},
      classifiers=['Environment :: Console',
                   'Intended Audience :: Developers',
                   'Operating System :: Unix',
                   'Operating System :: POSIX',
                   'Programming Language :: Python',
                   'Topic :: System',
                   'Topic :: Software Development',
                   'Development Status :: 4 - Beta'])
