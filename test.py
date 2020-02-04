import ee

def test(file):
    text = ee.tikaparse(file)
    xls = ee.Excel(file, text)
    xls.export()
