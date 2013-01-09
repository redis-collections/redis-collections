# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='redis_collections',
    version='0.0.1',
    description='Set of basic Python collections backed by Redis.',
    long_description=open('README.rst').read(),
    author='Honza Javorek',
    author_email='jan.javorek@gmail.com',
    url='https://github.com/honzajavorek/redis-collections',
    license=open('LICENSE').read(),
    py_modules=['redis_collections'],
    install_requires=['redis>=2.7.2'],
    zip_safe=False,
    classifiers=(
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    ),
)
