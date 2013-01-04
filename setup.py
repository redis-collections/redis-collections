# -*- coding: utf-8 -*-


from setuptools import setup


setup(
    name='redis_collections',
    description='Set of basic Python collections backed by Redis.',
    version='0.0.1',
    author='Honza Javorek',
    author_email='jan.javorek@gmail.com',
    url='https://github.com/honzajavorek/redis-collections',
    license='ISC',
    py_modules=['redis_collections'],
    install_requires=['redis>=2.7.2'],
    tests_require=['nose==1.2.1'],
    test_suite='nose.collector'
)
