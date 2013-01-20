# -*- coding: utf-8 -*-


import os
import re
import sys
import shlex
import subprocess

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

# Hack to prevent stupid "TypeError: 'NoneType' object is not callable"
# error in multiprocessing/util.py _exit_function when running `python
# setup.py test`
try:
    import multiprocessing
except ImportError:
    pass


base_path = os.path.dirname(__file__)


# version
ver_file = os.path.join(base_path, 'redis_collections/__init__.py')
ver_file_head = open(ver_file).read(100)

match = re.search(r'__version__ = \'([^\']*)\'', ver_file_head)
if match:
    version = match.group(1)
else:
    raise RuntimeError('Missing version number.')


# release a version, publish to GitHub and PyPI
if sys.argv[-1] == 'publish':
    command = lambda cmd: subprocess.check_call(shlex.split(cmd))
    command('git tag v' + version)
    command('git push --tags origin master:master')
    command('python setup.py sdist upload')
    sys.exit()


setup(
    name='redis-collections',
    version=version,
    description='Set of basic Python collections backed by Redis.',
    long_description=open('README.rst').read(),
    author='Honza Javorek',
    author_email='jan.javorek@gmail.com',
    url='https://github.com/honzajavorek/redis-collections',
    license=open('LICENSE').read(),
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=['redis>=2.7.2'],
    test_suite='nose.collector',
    tests_require=['nose==1.2.1'],
    zip_safe=False,
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    )
)
