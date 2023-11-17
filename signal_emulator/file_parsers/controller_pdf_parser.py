import pdfplumber


class ControllerPdfParser:
    def __init__(self):
        pass

    def parse_pdf(self, pdf_path):
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[2]
            tables = first_page.extract_tables()
            print(first_page.chars[0])


if __name__ == "__main__":
    cpp = ControllerPdfParser()
    cpp.parse_pdf(r"resources/timing_sheets_pdf/00_000002_Junc.pdf")
