# -*- coding: utf-8 -*-


import os
import re
import sys
import subprocess

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages  # NOQA

# Hack to prevent stupid "TypeError: 'NoneType' object is not callable"
# error in multiprocessing/util.py _exit_function when running `python
# setup.py test`
try:
    import multiprocessing  # NOQA
except ImportError:
    pass


base_path = os.path.dirname(__file__)


# version
meta_file = os.path.join(base_path, 'redis_collections/__init__.py')
meta_file_contents = open(meta_file).read()
meta = dict(re.findall(r'__([^_]+)__ = \'([^\']*)\'', meta_file_contents))


# release a version, publish to GitHub and PyPI
if sys.argv[-1] == 'publish':
    subprocess.check_call(['git', 'tag', 'v' + meta['version']])
    subprocess.check_call(['git', 'push', '--tags', 'origin', 'master:master'])
    # ...the rest happens on Travis CI
    sys.exit()


def readme(filename='README.rst'):
    if sys.version_info.major > 2:
        with open(filename, encoding='utf-8') as file_handle:
            return file_handle.read()
    with open(filename) as file_handle:
        return file_handle.read()


setup(
    name=meta['title'],
    version=meta['version'],
    description='Set of basic Python collections backed by Redis.',
    extras_require = {
        'redislite':  ["redislite==1.0.228"]
    },
    long_description=readme(),
    author=meta['author'],
    author_email='mail@honzajavorek.cz',
    url='https://github.com/honzajavorek/redis-collections',
    license=open('LICENSE').read(),
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=['redis>=2.7.2'],
    zip_safe=False,
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    )
)
