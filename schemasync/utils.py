"""Utility functions for Schema Sync"""

import re
import os
import datetime
import glob
import cStringIO

#REGEX_NO_TICKS = re.compile('`')
#REGEX_INT_SIZE = re.compile('int\(\d+\)')
REGEX_MULTI_SPACE = re.compile(r'\s\s+')
REGEX_DISTANT_SEMICOLIN = re.compile(r'(\s+;)$')
REGEX_FILE_COUNTER = re.compile(r"\_(?P<i>[0-9]+)\.(?:[^\.]+)$")
REGEX_TABLE_COMMENT = re.compile(r"COMMENT(?:(?:\s*=\s*)|\s*)'(.*?)'", re.I)
REGEX_TABLE_AUTO_INC = re.compile(r"AUTO_INCREMENT(?:(?:\s*=\s*)|\s*)(\d+)", re.I)
REGEX_SEMICOLON_EXPLODE_TO_NEWLINE = re.compile(r';\s+')


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
            db: string, database name
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

def compare_version(x, y, separator = r'[.-]'):
    """Return negative if version x<y, zero if x==y, positive if x>y.

        Args:
            x: string, version x to compare
            y: string, version y to compare

        Returns:
            integer representing the compare result of version x and y.
    """
    x_array = re.split(separator, x)
    y_array = re.split(separator, y)
    for index in range(min(len(x_array),len(y_array))):
        if x_array[index] != y_array[index]:
            try:
                return cmp(int(x_array[index]), int(y_array[index]))
            except ValueError:
                return 0
    return 0

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
