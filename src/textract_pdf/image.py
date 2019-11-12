"""
This file is part of the textract project by Dean Malmgren <https://github.com/deanmalmgren/textract>.
Textract is licensed under the permissive MIT License, a copy of which can be found
 - in this repository (textract_pdf/LICENSE),
 - on Dean Malmgren's github <https://github.com/deanmalmgren/textract/blob/master/LICENSE>,
 - and as a public resource of the Open Source Initiative <https://opensource.org/licenses/MIT>

Copyright (c) 2014 Dean Malmgren

Process an image file using tesseract.
"""

import os

from .utils import ShellParser


class Parser(ShellParser):
    """Extract text from various image file formats using tesseract-ocr"""

    def extract(self, filename, **kwargs):

        # if language given as argument, specify language for tesseract to use
        if 'language' in kwargs:
            args = ['tesseract', filename, 'stdout', '-l', kwargs['language']]
        else:
            args = ['tesseract', filename, 'stdout']

        stdout, _ = self.run(args)
        return stdout