#!/usr/bin/python
import unittest
import os
import glob
import datetime
from schemasync.utils import versioned, create_pnames, compare_version, PatchBuffer


class TestVersioned(unittest.TestCase):
    def setUp(self):
        filename = "/tmp/schemasync_util_testfile.txt"
        self.base_name, self.ext = os.path.splitext(filename)
        files = glob.glob(self.base_name + '*' + self.ext)

        for f in files:
            os.unlink(f)

    def tearDown(self):
        files = glob.glob(self.base_name + '*' + self.ext)

        for f in files:
            os.unlink(f)

    def test_inital_file(self):
        self.assertEqual(self.base_name + self.ext,
                         versioned(self.base_name + self.ext))

    def test_inc_sequence(self):
        open(self.base_name + self.ext, 'w').close()

        self.assertEqual(self.base_name + '_1' + self.ext,
                         versioned(self.base_name + self.ext))

    def test_inc_sequence_incomplete(self):
        open(self.base_name + self.ext, 'w').close()
        open(self.base_name + '_2' + self.ext, 'w').close()

        self.assertEqual(self.base_name + '_3' + self.ext,
                         versioned(self.base_name + self.ext))

    def test_inc_sequence_missing(self):
        open(self.base_name + '_4' + self.ext, 'w').close()

        self.assertEqual(self.base_name + '_5' + self.ext,
                         versioned(self.base_name + self.ext))


class TestPNames(unittest.TestCase):
    def test_no_tag(self):
        d = datetime.datetime.now().strftime("%Y%m%d")
        p = "mydb.%s.patch.sql" % d
        r = "mydb.%s.revert.sql" % d
        self.assertEqual((p,r), create_pnames("mydb", date_format="%Y%m%d"))

    def test_simple_tag(self):
        d = datetime.datetime.now().strftime("%Y%m%d")
        p = "mydb_tag.%s.patch.sql" % d
        r = "mydb_tag.%s.revert.sql" % d
        self.assertEqual((p,r), create_pnames("mydb",tag="tag", date_format="%Y%m%d"))

    def test_alphanumeric_tag(self):
        d = datetime.datetime.now().strftime("%Y%m%d")
        p = "mydb_tag123.%s.patch.sql" % d
        r = "mydb_tag123.%s.revert.sql" % d
        self.assertEqual((p,r), create_pnames("mydb",tag="tag123", date_format="%Y%m%d"))

    def test_tag_with_spaces(self):
        d = datetime.datetime.now().strftime("%Y%m%d")
        p = "mydb_mytag.%s.patch.sql" % d
        r = "mydb_mytag.%s.revert.sql" % d
        self.assertEqual((p,r), create_pnames("mydb",tag="my tag", date_format="%Y%m%d"))

    def test_tag_with_invalid_chars(self):
        d = datetime.datetime.now().strftime("%Y%m%d")
        p = "mydb_tag.%s.patch.sql" % d
        r = "mydb_tag.%s.revert.sql" % d
        self.assertEqual((p,r), create_pnames("mydb",tag="tag!@#$%^&*()+?<>:{},./|\[];", date_format="%Y%m%d"))

    def test_tag_with_valid_chars(self):
        d = datetime.datetime.now().strftime("%Y%m%d")
        p = "mydb_my-tag_123.%s.patch.sql" % d
        r = "mydb_my-tag_123.%s.revert.sql" % d
        self.assertEqual((p,r), create_pnames("mydb",tag="my-tag_123", date_format="%Y%m%d"))

class TestCompareVersion(unittest.TestCase):
    def test_basic_compare(self):
        self.assertTrue(compare_version('10.0.0-mysql', '5.0.0-mysql') > 0)
        self.assertTrue(compare_version('10.0.0-mysql', '5.0.0-log') > 0)
        self.assertTrue(compare_version('5.1.0-mysql', '5.0.1-log') > 0)
        self.assertTrue(compare_version('5.0.0-mysql', '5.0.0-log') == 0)
        self.assertTrue(compare_version('5.0.0-mysql', '5.0.1-log') < 0)

class TestPatchBuffer(unittest.TestCase):

    def setUp(self):
        self.p = PatchBuffer(name="patch.txt",
                             filters=[],
                             tpl="data in this file: %(data)s",
                             ctx={'x':'y'},
                             version_filename=True)

    def tearDown(self):
        if (os.path.isfile(self.p.name)):
            os.unlink(self.p.name)

    def test_loaded(self):
        self.assertEqual("patch.txt", self.p.name)
        self.assertEqual([], self.p.filters)
        self.assertEqual("data in this file: %(data)s", self.p.tpl)
        self.assertEqual({'x':'y'}, self.p.ctx)
        self.assertEqual(True, self.p.version_filename)
        self.assertEqual(False, self.p.modified)

    def test_write(self):
        self.assertEqual(False, self.p.modified)
        self.p.write("hello world")
        self.assertEqual(True, self.p.modified)

    def test_save(self):
        self.assertEqual(False, os.path.isfile(self.p.name))
        self.p.write("hello, world")
        self.p.save()
        self.assertEqual(True, os.path.isfile(self.p.name))
        f= open(self.p.name, 'r')
        self.assertEqual("data in this file: hello, world", f.readline())

    def test_save_versioned(self):
        self.p.version_filename = True
        self.assertEqual(False, os.path.isfile(self.p.name))
        self.p.write("hello, world")

        self.p.save()
        self.assertEqual(self.p.name, "patch.txt")
        self.assertEqual(True, os.path.isfile(self.p.name))
        f= open(self.p.name, 'r')
        self.assertEqual("data in this file: hello, world", f.readline())

        self.p.save()
        self.assertEqual(self.p.name, "patch_1.txt")
        self.assertEqual(True, os.path.isfile(self.p.name))
        f= open(self.p.name, 'r')
        self.assertEqual("data in this file: hello, world", f.readline())

        os.unlink("patch.txt")
        self.assertEqual(False, os.path.isfile("patch.txt"))

        os.unlink("patch_1.txt")
        self.assertEqual(False, os.path.isfile("patch_1.txt"))

    def test_delete(self):
        self.assertEqual(False, os.path.isfile(self.p.name))
        self.p.write("hello, world")
        self.p.save()
        self.assertEqual(self.p.name, "patch.txt")
        self.assertEqual(True, os.path.isfile(self.p.name))
        self.p.delete()
        self.assertEqual(False, os.path.isfile(self.p.name))

if __name__ == "__main__":
    unittest.main()