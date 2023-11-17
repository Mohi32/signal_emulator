import os
from collections import defaultdict
from signal_emulator.utilities.utility_functions import txt_file_to_list
from datetime import datetime
import re


class PlanChecker:
    AREAS = ["CNTR", "EAST", "NORT", "SOUT", "OUTR"]

    def __init__(self):
        pass

    def pln_filename_iterator(self):
        for area in self.AREAS:
            folder_path = f"resources/plans/{area}"
            # Iterate over files in the folder
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                # Check if the file is a pln file
                if filename.endswith(".pln"):
                    yield file_path, area

    def find_one_line_plans(self):
        area_count_dict = defaultdict(lambda: 0)
        for file_path, area in self.pln_filename_iterator():
            lines = txt_file_to_list(file_path)
            if len(lines) == 1:
                print(f"{file_path}")
                area_count_dict[area] += 1
        print(area_count_dict)

    def find_recently_updated_plans(self, cutoff_date):
        for file_path, area in self.pln_filename_iterator():
            lines = txt_file_to_list(file_path)
            for line in lines:
                update_date = self.get_update_date(line)
                if update_date and update_date > cutoff_date:
                    print(file_path, line)
                    break

    def is_update_line(self, line):
        return line.startswith(";") and "Object compiled by " in line

    def get_update_date(self, input_string):
        # Define a regular expression pattern to match the date format
        date_pattern = r'\d{2}-[A-Za-z]{3}-\d{4}'
        # Search for the date pattern in the input string
        match = re.search(date_pattern, input_string)
        if match:
            # Extract the matched date string
            date_string = match.group()
            # Define the format of the input date string
            format_string = '%d-%b-%Y'
            # Use datetime.strptime() to parse the string and convert it to a datetime object
            date_object = datetime.strptime(date_string, format_string)
            # Now, date_object contains the datetime representation of the extracted date
            return date_object
        else:
            return None

    def get_substring_between_words(self, input_string, word1, word2):
        # Find the starting and ending positions of the words
        start_pos = input_string.find(word1) + len(word1)
        end_pos = input_string.find(word2, start_pos)
        # Extract the substring between the two words
        substring = input_string[start_pos:end_pos]
        return substring


if __name__ == "__main__":
    pc = PlanChecker()
    pc.find_recently_updated_plans(cutoff_date=datetime(year=2023, month=5, day=1))
    # pc.find_one_line_plans()
