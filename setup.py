import io
import os
import re
import subprocess
import sys

from setuptools import setup, find_packages


base_path = os.path.dirname(__file__)


# version
meta_file = os.path.join(base_path, 'redis_collections/__init__.py')
meta_file_contents = io.open(meta_file, encoding='utf-8').read()
meta = dict(re.findall(r'__([^_]+)__ = \'([^\']*)\'', meta_file_contents))


# release a version, publish to GitHub and PyPI
if sys.argv[-1] == 'publish':
    subprocess.check_call(['git', 'tag', 'v' + meta['version']])
    subprocess.check_call(['git', 'push', '--tags', 'origin', 'master:master'])
    # ...the rest happens on Travis CI
    sys.exit()


setup(
    name=meta['title'],
    version=meta['version'],
    description='Set of basic Python collections backed by Redis.',
    long_description=io.open('README.rst', encoding='utf-8').read(),
    author=meta['author'],
    author_email='mail@honzajavorek.cz',
    url='https://github.com/honzajavorek/redis-collections',
    license='ISC',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    python_requires='>=3.4',
    install_requires=['redis>=3.1.0,<4.0.0'],
    zip_safe=False,
    keywords=['redis', 'persistence'],
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
    ),
)
