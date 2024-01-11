# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='sample',
    version='0.1.0',
    description='Unofficial Package for Trimble Connect APIs to index Trimble Connect data',
    long_description=readme,
    author='Jason Pickup',
    author_email='jpickup@laingorourke.com.au',
    url='https://github.com/RayKing99/TrimblePy',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)