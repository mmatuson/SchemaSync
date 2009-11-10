#!/usr/bin/python
import unittest
import schemaobject
from schemasync import syncdb

class TestSyncTables(unittest.TestCase):

    def setUp(self):
        self.schema = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.src = self.schema.selected

        self.schema2 = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.dest = self.schema2.selected


    def test_created_tables(self):
        """Test: src db has tables not in dest db"""
        saved = self.dest.tables['rental']
        pos = self.dest.tables.index('rental')
        del self.dest.tables['rental']

        for i, (p, r) in enumerate(syncdb.sync_created_tables(self.src.tables, self.dest.tables)):
            self.assertEqual(p, "CREATE TABLE `rental` ( `rental_id` int(11) NOT NULL AUTO_INCREMENT, `rental_date` datetime NOT NULL, `inventory_id` mediumint(8) unsigned NOT NULL, `customer_id` smallint(5) unsigned NOT NULL, `return_date` datetime DEFAULT NULL, `staff_id` tinyint(3) unsigned NOT NULL, `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`rental_id`), UNIQUE KEY `rental_date` (`rental_date`,`inventory_id`,`customer_id`), KEY `idx_fk_inventory_id` (`inventory_id`), KEY `idx_fk_staff_id` (`staff_id`), KEY `idx_fk_customer_id` (`customer_id`) USING BTREE, CONSTRAINT `fk_rental_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`) ON UPDATE CASCADE, CONSTRAINT `fk_rental_inventory` FOREIGN KEY (`inventory_id`) REFERENCES `inventory` (`inventory_id`) ON UPDATE CASCADE, CONSTRAINT `fk_rental_staff` FOREIGN KEY (`staff_id`) REFERENCES `staff` (`staff_id`) ON UPDATE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
            self.assertEqual(r, "DROP TABLE `rental`;")

        self.assertEqual(i, 0)

    def test_dropped_tables(self):
        """Test: dest db has tables not in src db"""
        saved = self.src.tables['rental']
        pos = self.src.tables.index('rental')
        del self.src.tables['rental']

        for i, (p, r) in enumerate(syncdb.sync_dropped_tables(self.src.tables, self.dest.tables)):
            self.assertEqual(p, "DROP TABLE `rental`;")
            self.assertEqual(r, "CREATE TABLE `rental` ( `rental_id` int(11) NOT NULL AUTO_INCREMENT, `rental_date` datetime NOT NULL, `inventory_id` mediumint(8) unsigned NOT NULL, `customer_id` smallint(5) unsigned NOT NULL, `return_date` datetime DEFAULT NULL, `staff_id` tinyint(3) unsigned NOT NULL, `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`rental_id`), UNIQUE KEY `rental_date` (`rental_date`,`inventory_id`,`customer_id`), KEY `idx_fk_inventory_id` (`inventory_id`), KEY `idx_fk_staff_id` (`staff_id`), KEY `idx_fk_customer_id` (`customer_id`) USING BTREE, CONSTRAINT `fk_rental_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`) ON UPDATE CASCADE, CONSTRAINT `fk_rental_inventory` FOREIGN KEY (`inventory_id`) REFERENCES `inventory` (`inventory_id`) ON UPDATE CASCADE, CONSTRAINT `fk_rental_staff` FOREIGN KEY (`staff_id`) REFERENCES `staff` (`staff_id`) ON UPDATE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

        self.assertEqual(i, 0)

    def test_table_options(self):
        """Test: src and dest table have different options"""
        self.src.tables['address'].options['engine'].value = "MyISAM"

        p,r = syncdb.sync_table_options(self.src.tables['address'], self.dest.tables['address'], sync_auto_inc=False, sync_comments=False)
        self.assertEqual(p, "ENGINE=MyISAM")
        self.assertEqual(r, "ENGINE=InnoDB")


    def test_table_options_with_auto_inc(self):
        """Test: src and dest table have different options (include AUTO_INCREMENT value)"""
        self.src.tables['address'].options['engine'].value = "MyISAM"
        self.src.tables['address'].options['auto_increment'].value = 11

        p,r = syncdb.sync_table_options(self.src.tables['address'], self.dest.tables['address'], sync_auto_inc=True, sync_comments=False)
        self.assertEqual(p, "ENGINE=MyISAM AUTO_INCREMENT=11")
        self.assertEqual(r, "ENGINE=InnoDB AUTO_INCREMENT=1")


    def test_table_options_with_comment(self):
        """Test: src and dest table have different options (include table COMMENT)"""
        self.src.tables['address'].options['engine'].value = "MyISAM"
        self.src.tables['address'].options['comment'].value =  "hello world"

        p,r = syncdb.sync_table_options(self.src.tables['address'], self.dest.tables['address'], sync_auto_inc=False, sync_comments=True)
        self.assertEqual(p, "ENGINE=MyISAM COMMENT='hello world'")
        self.assertEqual(r, "ENGINE=InnoDB COMMENT=''" )


if __name__ == "__main__":
    from test_all import get_database_url
    TestSyncTables.database_url = get_database_url()
    unittest.main()