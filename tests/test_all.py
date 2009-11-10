#!/usr/bin/python
import unittest
from test_sync_database import TestSyncDatabase
from test_sync_tables import TestSyncTables
from test_sync_columns import TestSyncColumns
from test_sync_constraints import TestSyncConstraints
from test_utils import TestVersioned, TestPNames, TestPatchBuffer

def get_database_url():
    database_url = raw_input("\nTests need to be run against the Sakila Database v0.8\n"
                            "Enter the MySQL Database Connection URL without the database name\n"
                            "Example: mysql://user:pass@host:port/\n"
                            "URL: ")
    if not database_url.endswith('/'):
        database_url += '/'
    return database_url

def regressionTest():
    test_cases = [
                  TestSyncDatabase,
                  TestSyncTables,
                  TestSyncColumns,
                  TestSyncConstraints,
                  TestVersioned,
                  TestPNames,
                  TestPatchBuffer,
                  ]
    database_url = get_database_url()

    suite = unittest.TestSuite()
    for tc in test_cases:
        tc.database_url = database_url
        suite.addTest(unittest.makeSuite(tc))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="regressionTest")