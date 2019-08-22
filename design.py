# todo: read, copy, paste, concurrent execution
# variables: file extention of reading and pasting.


class A:
    """Fisrt class for idea. Interface."""

    def __init__(self):
        """Create a instance."""

        self.text = ''
        self.words = ''

    def read(self, file):
        """Extract text from file"""

        # Read file
        file = self.create(file)
        # Return a short text

    def copy(self, file):
        """Copy the text."""

        # Update self.words
        self.text = self.read(file)
        # re self.text
        # Return the first match

    def create(self, file):
        # Factory method


class B:
    """About paste. Interface."""

    def paste(self, words, FileB):
        """Paste the words into the file."""

        # Paste self.words into file
        file = self.create(FileB)
        # Add some message about the execution

    def create(self, file):
        # Factory method


def main():
    """Run script."""
    if __name__ == '__main__':
        # Request email
        # Run instance A.copy
        # Run instance B.paste
        # Move the original file to folder 'OriginFile'
        # Delete OriginFile
        # Email the zip file
