#!/usr/bin/python
import unittest
import schemaobject
from schemasync import syncdb

class TestSyncColumns(unittest.TestCase):

    def setUp(self):
        self.schema = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.src = self.schema.selected.tables['rental']

        self.schema2 = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.dest = self.schema2.selected.tables['rental']

    def test_get_previous_item(self):
        """Test get previous item from Column list"""
        lst = ['bobby tables', 'jack', 'jill']
        self.assertEqual('jack', syncdb.get_previous_item(lst, 'jill'))
        self.assertEqual('bobby tables', syncdb.get_previous_item(lst, 'jack'))
        self.assertEqual(None, syncdb.get_previous_item(lst, 'bobby tables'))
        self.assertEqual(None, syncdb.get_previous_item(lst, 'jeff'))

    def test_sync_created_column(self):
        """Test: src table has columns not in dest table (ignore Column COMMENT)"""
        saved = self.dest.columns['staff_id']
        pos = self.dest.columns.index('staff_id')
        del self.dest.columns['staff_id']

        for i, (p,r) in enumerate(syncdb.sync_created_columns(self.src.columns, self.dest.columns, sync_comments=False)):
            self.assertEqual(p, "ADD COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL AFTER `return_date`")
            self.assertEqual(r, "DROP COLUMN `staff_id`")

        self.assertEqual(i, 0)

    def test_sync_created_column_with_comments(self):
        """Test: src table has columns not in dest table (include Column COMMENT)"""
        saved = self.dest.columns['staff_id']
        pos = self.dest.columns.index('staff_id')
        del self.dest.columns['staff_id']

        self.src.columns['staff_id'].comment = "hello world"

        for i, (p,r) in enumerate(syncdb.sync_created_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            self.assertEqual(p, "ADD COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL COMMENT 'hello world' AFTER `return_date`")
            self.assertEqual(r, "DROP COLUMN `staff_id`")

        self.assertEqual(i, 0)

    def test_sync_dropped_column(self):
        """Test: dest table has columns not in src table (ignore Column COMMENT)"""
        saved = self.src.columns['staff_id']
        pos = self.src.columns.index('staff_id')
        del self.src.columns['staff_id']

        for i, (p,r) in enumerate(syncdb.sync_dropped_columns(self.src.columns, self.dest.columns, sync_comments=False)):
            self.assertEqual(p, "DROP COLUMN `staff_id`")
            self.assertEqual(r, "ADD COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL AFTER `return_date`")

        self.assertEqual(i, 0)

    def test_sync_dropped_column_with_comment(self):
        """Test: dest table has columns not in src table (include Column COMMENT)"""
        saved = self.src.columns['staff_id']
        pos = self.src.columns.index('staff_id')
        del self.src.columns['staff_id']

        self.dest.columns['staff_id'].comment = "hello world"

        for i, (p,r) in enumerate(syncdb.sync_dropped_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            self.assertEqual(p, "DROP COLUMN `staff_id`")
            self.assertEqual(r, "ADD COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL COMMENT 'hello world' AFTER `return_date`")

        self.assertEqual(i, 0)

    def test_sync_modified_column(self):
        """Test: column in src table have been modified in dest table (ignore Column COMMENT)"""
        self.dest.columns['rental_date'].type = "TEXT"
        self.dest.columns['rental_date'].null = True
        self.dest.columns['rental_date'].comment = "hello world"

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=False)):
            self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")
            self.assertEqual(r, "MODIFY COLUMN `rental_date` TEXT NULL AFTER `rental_id`")

        self.assertEqual(i, 0)

    def test_sync_multiple_modified_columns(self):
        """Test: multiple columns in src table have been modified in dest table (ignore Column COMMENT)"""
        self.dest.columns['rental_date'].type = "TEXT"
        self.dest.columns['rental_date'].null = True
        self.dest.columns['rental_date'].comment = "hello world"
        self.dest.columns['return_date'].type = "TIMESTAMP"
        
        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=False)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` TEXT NULL AFTER `rental_id`")
            if i == 1:
                self.assertEqual(p, "MODIFY COLUMN `return_date` DATETIME NULL AFTER `customer_id`")
                self.assertEqual(r, "MODIFY COLUMN `return_date` TIMESTAMP NULL AFTER `customer_id`")

        self.assertEqual(i, 1)
            
    def test_sync_modified_column_with_comments(self):
        """Test: columns in src table have been modified in dest table (include Column COMMENT)"""
        self.dest.columns['rental_date'].type = "TEXT"
        self.dest.columns['rental_date'].null = True
        self.dest.columns['rental_date'].comment = "hello world"

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            self.assertEqual(r, "MODIFY COLUMN `rental_date` TEXT NULL COMMENT 'hello world' AFTER `rental_id`")
            self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")

        self.assertEqual(i, 0)

    def test_move_col_to_end_in_dest(self):
        """Move a column in the dest table towards the end of the column list"""

        tmp = self.dest.columns._sequence[1]
        self.dest.columns._sequence.remove(tmp)
        self.dest.columns._sequence.insert(5, tmp)

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `staff_id`")

        self.assertEqual(i, 0)

    def test_move_col_to_beg_in_dest(self):
        """Move a column in the dest table towards the begining of the column list"""

        tmp = self.dest.columns._sequence[4]
        self.dest.columns._sequence.remove(tmp)
        self.dest.columns._sequence.insert(1, tmp)

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `return_date` DATETIME NULL AFTER `customer_id`")
                self.assertEqual(r, "MODIFY COLUMN `return_date` DATETIME NULL AFTER `rental_id`")

        self.assertEqual(i, 0)

    def test_swap_two_cols_in_dest(self):
        """Swap the position of 2 columns in the dest table"""

        self.dest.columns._sequence[1], self.dest.columns._sequence[5] = self.dest.columns._sequence[5], self.dest.columns._sequence[1]

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `return_date`")
            if i == 1:
                self.assertEqual(p, "MODIFY COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL AFTER `return_date`")
                self.assertEqual(r, "MODIFY COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL AFTER `rental_id`")

        self.assertEqual(i, 1)

    def test_swap_pairs_of_cols_in_dest(self):
        """Swap the position of 2 pairs of columns in the dest table"""

        a,b = self.dest.columns._sequence[1], self.dest.columns._sequence[2]
        self.dest.columns._sequence[1], self.dest.columns._sequence[2] = self.dest.columns._sequence[4], self.dest.columns._sequence[5]
        self.dest.columns._sequence[4], self.dest.columns._sequence[5] = a,b

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `customer_id`")
            if i == 1:
                self.assertEqual(p, "MODIFY COLUMN `inventory_id` MEDIUMINT(8) UNSIGNED NOT NULL AFTER `rental_date`")
                self.assertEqual(r, "MODIFY COLUMN `inventory_id` MEDIUMINT(8) UNSIGNED NOT NULL AFTER `rental_date`")
            if i == 2:
                self.assertEqual(p, "MODIFY COLUMN `customer_id` SMALLINT(5) UNSIGNED NOT NULL AFTER `inventory_id`")
                self.assertEqual(r, "MODIFY COLUMN `customer_id` SMALLINT(5) UNSIGNED NOT NULL AFTER `staff_id`")

        self.assertEqual(i, 2)

    def test_move_3_cols_in_dest(self):
        """Move around 3 columns in the dest table"""

        self.dest.columns._sequence[0], self.dest.columns._sequence[3] = self.dest.columns._sequence[3], self.dest.columns._sequence[0]
        tmp = self.dest.columns._sequence[1]
        self.dest.columns._sequence.remove(tmp)
        self.dest.columns._sequence.insert(2, tmp)

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `rental_id` INT(11) NOT NULL auto_increment FIRST")
                self.assertEqual(r, "MODIFY COLUMN `rental_id` INT(11) NOT NULL auto_increment AFTER `rental_date`")
            if i == 1:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `inventory_id`")
            if i == 2:
                self.assertEqual(p, "MODIFY COLUMN `inventory_id` MEDIUMINT(8) UNSIGNED NOT NULL AFTER `rental_date`")
                self.assertEqual(r, "MODIFY COLUMN `inventory_id` MEDIUMINT(8) UNSIGNED NOT NULL AFTER `customer_id`")

        self.assertEqual(i, 2)


    def test_move_col_to_end_in_src(self):
        """Move a column in the dest table towards the end of the column list"""

        tmp = self.src.columns._sequence[1]
        self.src.columns._sequence.remove(tmp)
        self.src.columns._sequence.insert(5, tmp)

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `staff_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")

        self.assertEqual(i, 0)

    def test_move_col_to_beg_in_src(self):
        """Move a column in the dest table towards the begining of the column list"""

        tmp = self.src.columns._sequence[4]
        self.src.columns._sequence.remove(tmp)
        self.src.columns._sequence.insert(1, tmp)

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `return_date` DATETIME NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `return_date` DATETIME NULL AFTER `customer_id`")

        self.assertEqual(i, 0)

    def test_swap_two_cols_in_src(self):
        """Swap the position of 2 columns in the dest table"""

        self.src.columns._sequence[1], self.src.columns._sequence[5] = self.src.columns._sequence[5], self.src.columns._sequence[1]

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL AFTER `rental_id`")
                self.assertEqual(r, "MODIFY COLUMN `staff_id` TINYINT(3) UNSIGNED NOT NULL AFTER `return_date`")

            if i == 1:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `return_date`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")


        self.assertEqual(i, 1)

    def test_move_3_cols_in_src(self):
        """Move around 3 columns in the dest table"""

        self.src.columns._sequence[0], self.src.columns._sequence[3] = self.src.columns._sequence[3], self.src.columns._sequence[0]
        tmp = self.src.columns._sequence[1]
        self.src.columns._sequence.remove(tmp)
        self.src.columns._sequence.insert(2, tmp)

        for i, (p,r) in enumerate(syncdb.sync_modified_columns(self.src.columns, self.dest.columns, sync_comments=True)):
            if i == 0:
                self.assertEqual(p, "MODIFY COLUMN `customer_id` SMALLINT(5) UNSIGNED NOT NULL FIRST")
                self.assertEqual(r, "MODIFY COLUMN `customer_id` SMALLINT(5) UNSIGNED NOT NULL AFTER `inventory_id`")

            if i == 1:
                self.assertEqual(p, "MODIFY COLUMN `inventory_id` MEDIUMINT(8) UNSIGNED NOT NULL AFTER `customer_id`")
                self.assertEqual(r, "MODIFY COLUMN `inventory_id` MEDIUMINT(8) UNSIGNED NOT NULL AFTER `rental_date`")

            if i == 2:
                self.assertEqual(p, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `inventory_id`")
                self.assertEqual(r, "MODIFY COLUMN `rental_date` DATETIME NOT NULL AFTER `rental_id`")

        self.assertEqual(i, 2)
if __name__ == "__main__":
    from test_all import get_database_url
    TestSyncColumns.database_url = get_database_url()
    unittest.main()