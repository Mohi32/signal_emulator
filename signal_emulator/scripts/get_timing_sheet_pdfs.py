import os
import urllib.request


class TimingSheetDownloader:

    def __init__(self, csv_path, timing_sheet_pdf_directory, sld_pdf_directory):
        self.csv_path = csv_path
        self.timing_sheet_pdf_path = timing_sheet_pdf_directory
        self.sld_pdf_path = sld_pdf_directory

    def site_generator(self):
        for filename in os.listdir(self.csv_path):
            timing_sheet_path = os.path.join(self.csv_path, filename)
            if filename.endswith("csv") and os.path.isfile(timing_sheet_path):
                filename_parts = filename.split("_")
                site_no = f"{filename_parts[0]}/{filename_parts[1]}"
                yield site_no, filename

    def get_timing_sheet_pdfs(self):
        for site_no, filename in self.site_generator():
            pdf_url = (
                    f"http://sfm/reports/rwservlet?sfmlivepdf&report=rep_10a&sheet_type=TIMING%20SHEET"
                    f"&report_type=UTC%20Micro&shelved=N&required_dt=01-MAY-2023&site_no={site_no}"
                )
            local_path = os.path.join(self.timing_sheet_pdf_path, filename.replace("csv", "pdf"))
            if not os.path.exists(local_path):
                self.save_pdf(pdf_url, local_path)

    def get_sld_pdfs(self):
        for site_no, filename in self.site_generator():
            sld_filename = f"{site_no.replace('/', '')}.pdf"
            pdf_url = f"http://sfm/sfmslds/{sld_filename}"
            local_path = os.path.join(self.sld_pdf_path, sld_filename)
            if not os.path.exists(local_path):
                self.save_pdf(pdf_url, local_path)

    @staticmethod
    def save_pdf(pdf_url, local_path):
        try:
            # Send an HTTP GET request to the URL and open the URL
            with urllib.request.urlopen(pdf_url) as response, open(local_path, 'wb') as out_file:
                # Read the PDF content and write it to the local file
                out_file.write(response.read())
            print(f'PDF saved as {local_path}')
        except Exception as e:
            print(f'Failed to download PDF: {e}')


if __name__ == "__main__":
    tsd = TimingSheetDownloader(
        csv_path=r"/resources/timing_sheets",
        timing_sheet_pdf_directory=r"D:\gitworks\signal_timings\resources\timing_sheets_pdf",
        sld_pdf_directory=r"D:\gitworks\signal_timings\resources\sld_pdf"
    )
    tsd.get_timing_sheet_pdfs()
    tsd.get_sld_pdfs()
