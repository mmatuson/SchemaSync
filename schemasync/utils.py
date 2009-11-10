"""Utility functions for Schema Sync"""

import re
import os
import datetime
import glob
import cStringIO

#REGEX_NO_TICKS = re.compile('`')
#REGEX_INT_SIZE = re.compile('int\(\d+\)')
REGEX_MULTI_SPACE = re.compile('\s\s+')
REGEX_DISTANT_SEMICOLIN = re.compile('\s;')
REGEX_FILE_COUNTER = re.compile("\_(?P<i>[0-9]+)\.")


def versioned(filename):
    """Return the versioned name for a file.
       If filename exists, the next available sequence # will be added to it.
       file.txt => file_1.txt => file_2.txt => ...
       If filename does not exist the original filename is returned.

       Args:
            filename: the filename to version (including path to file)

       Returns:
            String, New filename.
    """
    name, ext = os.path.splitext(filename)
    files = glob.glob(name + '*' + ext)
    if not files:
        return filename

    files= map(lambda x: REGEX_FILE_COUNTER.search(x, re.I), files)
    file_counters = [i.group('i') for i in files if i]

    if file_counters:
        i = int(max(file_counters)) + 1
    else:
        i = 1

    return name + ('_%d' % i) + ext


def create_pnames(db, tag=None, date_format="%Y%m%d"):
    """Returns a tuple of the filenames to use to create the migration scripts.
       Filename format: <db>[_<tag>].<date=DATE_FORMAT>.(patch|revert).sql

        Args:
            db: srting, databse name
            tag: string, optional, tag for the filenames
            date_format: string, the current date format
                         Default Format: 21092009

        Returns:
            tuple of strings (patch_filename, revert_filename)
    """
    d = datetime.datetime.now().strftime(date_format)
    if tag:
        tag = re.sub('[^A-Za-z0-9_-]', '', tag)
        basename = "%s_%s.%s" % (db, tag, d)
    else:
        basename = "%s.%s" % (db, d)

    return ("%s.%s" % (basename, "patch.sql"),
            "%s.%s" % (basename, "revert.sql"))


class PatchBuffer(object):
    """Class for creating patch files

        Attributes:
            name: String, filename to use when saving the patch
            filters: List of functions to map to the patch data
            tpl: The patch template where the data will be written
                 All data written to the PatchBuffer is palced in the
                template variable %(data)s.
            ctx: Dictionary of values to be put replaced in the template.
            version_filename: Bool, version the filename if it already exists?
            modified: Bool (default=False), flag to check if the
                      PatchBuffer has been written to.
    """

    def __init__(self, name, filters, tpl, ctx, version_filename=False):
        """Inits the PatchBuffer class"""
        self._buffer = cStringIO.StringIO()
        self.name = name
        self.filters = filters
        self.tpl = tpl
        self.ctx = ctx
        self.version_filename = version_filename
        self.modified = False

    def write(self, data):
        """Write data to the buffer."""
        self.modified = True
        self._buffer.write(data)

    def save(self):
        """Apply filters, template transformations and write buffer to disk"""
        data = self._buffer.getvalue()
        if not data:
            return False

        if self.version_filename:
            self.name = versioned(self.name)
        fh = open(self.name, 'w')

        for f in self.filters:
            data = f(data)

        self.ctx['data'] = data

        fh.write(self.tpl % self.ctx)
        fh.close()

        return True

    def delete(self):
        """Delete the patch once it has been writen to disk"""
        if os.path.isfile(self.name):
            os.unlink(self.name)

    def __del__(self):
        self._buffer.close()
