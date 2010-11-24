#!/usr/bin/env python
from setuptools import setup

setup(
    name='SchemaSync',
    version='0.9.1',
    description='A MySQL Schema Synchronization Utility',
    author='Mitch Matuson',
    packages=['schemasync'],
    install_requires=['SchemaObject >=0.5.2'],
    entry_points={
        'console_scripts': [
            'schemasync = schemasync.schemasync:main',
        ]
    },

    keywords = ["MySQL", "database", "schema", "migration", "SQL"],

    classifiers = [
      "Environment :: Console",
      "Intended Audience :: Information Technology",
      "Intended Audience :: System Administrators",
      "Intended Audience :: Developers",
      "License :: OSI Approved :: Apache Software License",
      "Programming Language :: Python",
      "Topic :: Database",
      "Topic :: Database :: Front-Ends",
      "Topic :: Software Development :: Build Tools",
      "Topic :: Software Development :: Code Generators",
      "Topic :: Utilities",
      ],

      long_description = """\
      Schema Sync will generate the SQL necessary to migrate the schema of a source database to a target database (patch script), as well as a the SQL necessary to undo the changes after you apply them (revert script).
      """
)
