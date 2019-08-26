"""License boilerplate."""


class AbstractIO(object):

    def __init__(self, file):
        self.file = file


class Reader(AbstractIO):
    """Reading interface of Bridge DP."""

    def read(self):
        """Delegates to read method of implementor class.

        Extracts unicode contents from file.
        Returns:
            A string of contents.
        """
        return self.file.read()


class PdfFile(object):
    """PDF implementor of Bridge."""

    def read(self):
        """See read method of Reader class."""

        # TODO(tika@github.com): Learn the package.


class WordFile(object):
    """MS word implementor of Bridge."""

    def read(self):
        """See read method of Reader class."""

        # TODO(Robin) Find a capable word package.


class Writer(AbstractIO):
    """Writing interface of Bridge DP."""

    def write(self):
        """Delegates to write method of implementor class.

        Inserts unicode contents into file.
        """
        self.file.write()


class ExcelFile(object):
    """MS excel implementor of Bridge."""

    def write(self):
        """See write method of Writer class."""

        # TODO(Robin) Find a capable excel pakcage.
