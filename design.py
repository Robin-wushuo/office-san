# This is a script, so it needs a script docstring.
"""usage: see the argparse module in python standard library."""


class Transmitor(object):
    """For extrating text.

    Methods:
        transmit: Transmit the text between files.
    """

    def __init__(self, fromfile, tofile):
        """Dependency injection.

        Args:
            fromfile: An implementor of different file format.
            tofile: Another implementor of different file format.
        """
        self.fromfile = fromfile
        self.tofile = tofile
        self.text = ''


    def transmit(self, pattern, pages):
        """Finds the text from a file then insert it into another file.

        Args:
            pattern: A regular expression pattern.
            pages: Counts of pages to be read.
        """
        self.text = self.fromfile.gettext(pages)
        # This needs additional effort in order to work.
        self.extract =  re.find(pattern)
        self.tofile.insert(self.extract)


class PdfFile(object):
    """PDF dependency."""

    def gettext(self, pages):
        """Gets text from pdf file by tika.

        Args:
            pages: see transmit method of Transmitor.
        """
        # TODO(tika@github.com): Learn the package.


class WordFile(object):
    """MS word dependency."""

    def gettext(self):
        """Gets text from word file by some module."""
        # TODO(Robin) Find a capable word package.


class ExcelFile(object):
    """MS excel dependency."""

    def insert(self):
        """Inserts text into excel file."""
        # TODO(Robin) Find a capable excel pakcage.
