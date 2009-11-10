#!/usr/bin/python
import unittest
import schemaobject
from schemasync import syncdb

class TestSyncConstraints(unittest.TestCase):

    def setUp(self):

        self.schema = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.src = self.schema.selected.tables['rental']

        self.schema2 = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.dest = self.schema2.selected.tables['rental']

    def test_sync_created_index(self):
        """Test: src table has indexes not in dest table"""
        saved = self.dest.indexes['idx_fk_customer_id']
        pos = self.dest.indexes.index('idx_fk_customer_id')
        del self.dest.indexes['idx_fk_customer_id']

        for i, (p,r) in enumerate(syncdb.sync_created_constraints(self.src.indexes, self.dest.indexes)):
            self.assertEqual(p, "ADD INDEX `idx_fk_customer_id` (`customer_id`) USING BTREE")
            self.assertEqual(r, "DROP INDEX `idx_fk_customer_id`")

        self.assertEqual(i, 0)

    def test_sync_dropped_index(self):
        """Test: dest table has indexes not in src table"""
        saved = self.src.indexes['idx_fk_customer_id']
        pos = self.dest.indexes.index('idx_fk_customer_id')
        del self.src.indexes['idx_fk_customer_id']

        for i, (p,r) in enumerate(syncdb.sync_dropped_constraints(self.src.indexes, self.dest.indexes)):
            self.assertEqual(p, "DROP INDEX `idx_fk_customer_id`")
            self.assertEqual(r, "ADD INDEX `idx_fk_customer_id` (`customer_id`) USING BTREE")

        self.assertEqual(i, 0)

    def test_sync_modified_index(self):
        """Test: src table has indexes modified in dest table"""
        self.dest.indexes['idx_fk_customer_id'].kind = "UNIQUE"
        self.dest.indexes['idx_fk_customer_id'].fields = [('inventory_id', 0)]

        for i, (p,r) in enumerate(syncdb.sync_modified_constraints(self.src.indexes, self.dest.indexes)):
            if i==0:
                self.assertEqual(p, "DROP INDEX `idx_fk_customer_id`")
                self.assertEqual(r, "DROP INDEX `idx_fk_customer_id`")
            if i==1:
                self.assertEqual(p, "ADD INDEX `idx_fk_customer_id` (`customer_id`) USING BTREE")
                self.assertEqual(r, "ADD UNIQUE INDEX `idx_fk_customer_id` (`inventory_id`) USING BTREE")

        self.assertEqual(i, 1)

    def test_sync_created_fk(self):
        """Test: src table has foreign keys not in dest table"""
        saved = self.dest.foreign_keys['fk_rental_customer']
        pos = self.dest.foreign_keys.index('fk_rental_customer')
        del self.dest.foreign_keys['fk_rental_customer']

        for i, (p,r) in enumerate(syncdb.sync_created_constraints(self.src.foreign_keys, self.dest.foreign_keys)):
            self.assertEqual(p, "ADD CONSTRAINT `fk_rental_customer` FOREIGN KEY `fk_rental_customer` (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE RESTRICT ON UPDATE CASCADE")
            self.assertEqual(r, "DROP FOREIGN KEY `fk_rental_customer`")

        self.assertEqual(i, 0)

    def test_sync_dropped_fk(self):
        """Test: dest table has foreign keys not in src table"""
        saved = self.src.foreign_keys['fk_rental_customer']
        pos = self.dest.foreign_keys.index('fk_rental_customer')
        del self.src.foreign_keys['fk_rental_customer']

        for i, (p,r) in enumerate(syncdb.sync_dropped_constraints(self.src.foreign_keys, self.dest.foreign_keys)):
            self.assertEqual(p, "DROP FOREIGN KEY `fk_rental_customer`")
            self.assertEqual(r, "ADD CONSTRAINT `fk_rental_customer` FOREIGN KEY `fk_rental_customer` (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE RESTRICT ON UPDATE CASCADE")

        self.assertEqual(i, 0)

    def test_sync_modified_fk(self):
        """Test: src table has foreign keys modified in dest table"""
        self.dest.foreign_keys['fk_rental_customer'].delete_rule = "SET NULL"

        for i, (p,r) in enumerate(syncdb.sync_modified_constraints(self.src.foreign_keys, self.dest.foreign_keys)):
            if i==0:
                self.assertEqual(p, "DROP FOREIGN KEY `fk_rental_customer`")
                self.assertEqual(r, "DROP FOREIGN KEY `fk_rental_customer`")
            if i==1:
                self.assertEqual(p, "ADD CONSTRAINT `fk_rental_customer` FOREIGN KEY `fk_rental_customer` (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE RESTRICT ON UPDATE CASCADE")
                self.assertEqual(r, "ADD CONSTRAINT `fk_rental_customer` FOREIGN KEY `fk_rental_customer` (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE SET NULL ON UPDATE CASCADE")

        self.assertEqual(i, 1)

if __name__ == "__main__":
    from test_all import get_database_url
    TestSyncConstraints.database_url = get_database_url()
    unittest.main()