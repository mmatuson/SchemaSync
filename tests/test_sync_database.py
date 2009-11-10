#!/usr/bin/python
import unittest
import schemaobject
from schemasync import syncdb

class TestSyncDatabase(unittest.TestCase):

    def setUp(self):
        self.schema = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.src = self.schema.selected

        self.schema2 = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.dest = self.schema2.selected

    def test_database_options(self):
        """Test: src and dest database options are different"""
        self.src.options['charset'].value = "utf8"
        self.src.options['collation'].value = "utf8_general_ci"

        p,r = syncdb.sync_database_options(self.src, self.dest)
        self.assertEqual(p, "CHARACTER SET=utf8 COLLATE=utf8_general_ci")
        self.assertEqual(r, "CHARACTER SET=latin1 COLLATE=latin1_swedish_ci")


if __name__ == "__main__":
    from test_all import get_database_url
    TestSyncDatabase.database_url = get_database_url()
    unittest.main()