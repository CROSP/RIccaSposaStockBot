from io import BytesIO
from urllib.request import urlopen
import xlsxwriter, zipfile, pprint, os

img_url  = "https://raw.githubusercontent.com/jmcnamara/XlsxWriter/master/examples/logo.png"
img_data = BytesIO(urlopen(img_url).read())

wb  = xlsxwriter.Workbook("probe.xlsx")
ws  = wb.add_worksheet()
ws.insert_image("B2", "logo.png", {"image_data": img_data})
wb.close()

with zipfile.ZipFile("probe.xlsx") as z:
    print("Images stored inside:", z.namelist())
