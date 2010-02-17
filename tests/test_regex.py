#!/usr/bin/python
import unittest
import re
from schemasync.utils import REGEX_TABLE_AUTO_INC, REGEX_TABLE_COMMENT
from schemasync.utils import REGEX_MULTI_SPACE, REGEX_DISTANT_SEMICOLIN, REGEX_FILE_COUNTER

class TestTableCommentRegex(unittest.TestCase):

    def test_single_column_comment_case_insensitive(self):
        """Test REGEX_TABLE_COMMENT lowercase (comment '*')"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL comment 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_single_column_comment_space_seperator(self):
        """Test REGEX_TABLE_COMMENT space seperator (COMMENT '*')"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_single_column_comment_space_seperator_multiple_spaces(self):
        """Test REGEX_TABLE_COMMENT multiple spaces as the seperator (COMMENT   '*')"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT   'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_single_column_comment_equals_seperator(self):
        """Test REGEX_TABLE_COMMENT = seperator (COMMENT='*')"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT='this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_single_column_comment_equals_seperator_with_spaces(self):
        """Test REGEX_TABLE_COMMENT = seperator surrounded by spaces (COMMENT = '*')"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT = 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_multiple_mixed_seperated_column_comment(self):
        """Test REGEX_TABLE_COMMENT multiple column comments (COMMENT '*', COMMENT='*')"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL COMMENT='this is your last name',`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 2)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL ,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_table_comment(self):
        """Test REGEX_TABLE_COMMENT Table comment"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB COMMENT 'table comment' DEFAULT CHARSET=utf8"""
        (sql, count) = re.subn(REGEX_TABLE_COMMENT, '', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8""")

    def test_multiple_column_comment_with_table_comment(self):
        """Test REGEX_TABLE_COMMENT multiple column comments and the Table comment"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL COMMENT='this is your last name',`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB COMMENT 'table comment' DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 3)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL ,`last_name` varchar(100) NOT NULL ,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8""")

    def test_no_comments(self):
        """Test REGEX_TABLE_COMMENT no comments"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL,`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_COMMENT.subn('', table)
        self.assertEqual(count, 0)
        self.assertEqual(sql, table)


class TestTableAutoIncrementRegex(unittest.TestCase):

    def test_auto_inc_regex_space_seperator(self):
        """Test REGEX_TABLE_AUTO_INC table option AUTO_INCREMENT 1"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) AUTO_INCREMENT 1 ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_AUTO_INC.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`))  ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_auto_inc_regex_space_seperator_with_multiple_spaces(self):
        """Test REGEX_TABLE_AUTO_INC table option AUTO_INCREMENT   1"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) AUTO_INCREMENT   1 ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_AUTO_INC.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`))  ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_auto_inc_regex_equals_seperator(self):
        """Test REGEX_TABLE_AUTO_INC table option AUTO_INCREMENT=1"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) AUTO_INCREMENT=1 ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_AUTO_INC.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`))  ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_auto_inc_regex_equals_seperator_with_spaces(self):
        """Test REGEX_TABLE_AUTO_INC table option AUTO_INCREMENT = 1"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) AUTO_INCREMENT  =  1 ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_AUTO_INC.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`))  ENGINE=InnoDB DEFAULT CHARSET=utf8""")

    def test_auto_inc_regex_case_insensitive(self):
        """Test REGEX_TABLE_AUTO_INC table option auto_increment=1"""
        table = """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`)) auto_increment=1 ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        (sql, count) = REGEX_TABLE_AUTO_INC.subn('', table)
        self.assertEqual(count, 1)
        self.assertEqual(sql, """CREATE TABLE `person` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(100) NOT NULL COMMENT 'this is your first name',`last_name` varchar(100) NOT NULL,`created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',PRIMARY KEY (`id`))  ENGINE=InnoDB DEFAULT CHARSET=utf8""")


class TestMultiSpaceRegex(unittest.TestCase):

    def test_multiple_spaces_in_string(self):
        """Test REGEX_MULTI_SPACE in string"""
        s = "hello,  world."
        matches = REGEX_MULTI_SPACE.findall(s)
        self.assertTrue(matches)
        self.assertEqual(matches, [' ' * 2])

    def test_multiple_spaces_leading_string(self):
        """Test REGEX_MULTI_SPACE leading string"""
        s = "     hello, world."
        matches = REGEX_MULTI_SPACE.findall(s)
        self.assertTrue(matches)
        self.assertEqual(matches, [' ' * 5])

    def test_multiple_spaces_trailing_string(self):
        """Test REGEX_MULTI_SPACE trailing string"""
        s = "hello, world.   "
        matches = REGEX_MULTI_SPACE.findall(s)
        self.assertTrue(matches)
        self.assertEqual(matches, [' ' * 3])

    def test_no_match(self):
        """Test REGEX_MULTI_SPACE no match"""
        s = "hello, world."
        matches = REGEX_MULTI_SPACE.findall(s)
        self.assertFalse(matches)


class TestDistantSemiColonRegex(unittest.TestCase):

    def test_single_space(self):
        """Test REGEX_DISTANT_SEMICOLIN with single space"""
        s = "CREATE DATABSE foobar ;"
        matches = REGEX_DISTANT_SEMICOLIN.search(s)
        self.assertTrue(matches)

    def test_multiple_spaces(self):
        """Test REGEX_DISTANT_SEMICOLIN with multiple spaces"""
        s = "CREATE DATABSE foobar    ;"
        matches = REGEX_DISTANT_SEMICOLIN.search(s)
        self.assertTrue(matches)

    def test_tabs(self):
        """Test REGEX_DISTANT_SEMICOLIN with tabs"""
        s = "CREATE DATABSE foobar      ;"
        matches = REGEX_DISTANT_SEMICOLIN.search(s)
        self.assertTrue(matches)

    def test_newline(self):
        """Test REGEX_DISTANT_SEMICOLIN with newline"""
        s = """CREATE DATABSE foobar
        ;"""
        matches = REGEX_DISTANT_SEMICOLIN.search(s)
        self.assertTrue(matches)

    def test_ignore_in_string(self):
        """Test REGEX_DISTANT_SEMICOLIN ignore when in string"""
        s = """ALTER TABLE `foo` COMMENT 'hello  ;'  ;"""
        matches = REGEX_DISTANT_SEMICOLIN.findall(s)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches, ['  ;'])
        
        s = """ALTER TABLE `foo` COMMENT 'hello  ;';"""
        matches = REGEX_DISTANT_SEMICOLIN.findall(s)
        self.assertFalse(matches)
            
    def test_no_match(self):
        """Test REGEX_DISTANT_SEMICOLIN with no spaces"""
        s = "CREATE DATABSE foobar;"
        matches = REGEX_DISTANT_SEMICOLIN.search(s)
        self.assertFalse(matches)

class TestFileCounterRegex(unittest.TestCase):

    def test_valid_numeric_matches_zero(self):
        """ Test REGEX_FILE_COUNTER valid numeric match 0"""
        test_str = "file_0.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertTrue(matches)
        self.assertEqual(matches.group('i'), '0')

    def test_valid_numeric_matches_single_digit(self):
        """ Test REGEX_FILE_COUNTER valid numeric match 1 digit"""
        test_str = "file_8.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertTrue(matches)
        self.assertEqual(matches.group('i'), '8')

    def test_valid_numeric_matches_two_digits(self):
        """ Test REGEX_FILE_COUNTER valid numeric match 2 digits"""
        test_str = "file_16.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertTrue(matches)
        self.assertEqual(matches.group('i'), '16')

    def test_valid_numeric_matches_three_digit(self):
        """ Test REGEX_FILE_COUNTER valid numeric match 3 digits"""
        test_str = "file_256.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertTrue(matches)
        self.assertEqual(matches.group('i'), '256')

    def test_valid_numeric_matches_four_digit(self):
        """ Test REGEX_FILE_COUNTER valid numeric match 4 digits"""
        test_str = "file_1024.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertTrue(matches)
        self.assertEqual(matches.group('i'), '1024')

    def test_sequence_simple(self):
        """ Test REGEX_FILE_COUNTER simplest valid sequence"""
        test_str = "_1.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertTrue(matches)
        self.assertEqual(matches.group('i'), '1')

    def test_sequence_repeated(self):
        """ Test REGEX_FILE_COUNTER repeated in sequence"""
        test_str = "hello_1._3.txt"
        matches = REGEX_FILE_COUNTER.findall(test_str)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches, ['3',])

    def test_sequence_underscore_ext(self):
        """ Test REGEX_FILE_COUNTER extention with underscore"""
        test_str = "hello_3._txt"
        matches = REGEX_FILE_COUNTER.findall(test_str)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches, ['3',])

    def test_sequence_numeric_ext_with_underscore(self):
        """ Test REGEX_FILE_COUNTER numeric extention with underscore"""
        test_str = "hello_3._123"
        matches = REGEX_FILE_COUNTER.findall(test_str)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches, ['3',])

    def test_no_match_invlaid_extention(self):
        """Test REGEX_FILE_COUNTER no match: invalid extention"""
        test_str = "_1."
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertFalse(matches)

    def test_no_match_missing_sequence(self):
        """Test REGEX_FILE_COUNTER no match: missing sequence"""
        test_str = "file.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertFalse(matches)

    def test_no_match_invalid_sequence(self):
        """Test REGEX_FILE_COUNTER no match: invalid sequence"""
        test_str = "file1.txt"
        matches = REGEX_FILE_COUNTER.search(test_str)
        self.assertFalse(matches)

    def test_no_match_sequence_not_at_end(self):
        """Test REGEX_FILE_COUNTER no match: sequence must be before extention"""
        test_str = "hello_3.x_x.txt"
        matches = REGEX_FILE_COUNTER.findall(test_str)
        self.assertFalse(matches)

if __name__ == '__main__':
    unittest.main()