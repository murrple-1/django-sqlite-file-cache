from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='django-sqlite-file-cache',
    version='0.6.0',
    description='A Django cache backend which has a similar API to FileBasedCache, but reduces the number of inodes used',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/murrple-1/django-sqlite-file-cache',
    author='Murray Christopherson',
    author_email='murraychristopherson@gmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='django cache sqlite sqlite2 sqlite3 file filebased',
    packages=find_packages(exclude=['tests']),
    install_requires=['six', 'Django'],
)
