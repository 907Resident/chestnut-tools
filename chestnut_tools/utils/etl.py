# %% import necessary packages and modules

from datetime import datetime, timedelta
import os
import re

# %% finding the correct .zip folder in the eosAC data


def find_zip_files(base_dir, date_input):
    """
    Finds .zip files in a directory structure that match a given date or date range.

    Args:
        base_dir (str): The base directory to search for .zip files.
        date_input (str): The date input in the format:
            - "YYYYMM" for a specific month.
            - "YYYYMMDD" for a specific day.
            - "YYYYMMDD-YYYYMMDD" for a date range.

    Returns:
        list of str: A list of paths to the matching .zip files.
    """

    def parse_date_input(date_input):
        """
        Parses the date input to determine the start and end dates.

        Args:
            date_input (str): The date input in "YYYYMM", "YYYYMMDD", or "YYYYMMDD-YYYYMMDD" format.

        Returns:
            tuple of datetime: A tuple containing start_date and end_date.

        Raises:
            ValueError: If the date_input format is invalid.
        """
        # check if input is a range
        if "-" in date_input:
            start_date, end_date = map(str.strip, date_input.split("-"))
            return datetime.strptime(start_date, "%Y%m%d"), datetime.strptime(
                end_date, "%Y%m%d"
            )
        # check if input is a specific month
        elif len(date_input) == 6:
            start_date = datetime.strptime(date_input, "%Y%m")
            end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(
                days=1
            )
            return start_date, end_date
        # check if input is a specific day
        elif len(date_input) == 8:
            date = datetime.strptime(date_input, "%Y%m%d")
            return date, date
        else:
            raise ValueError(
                "invalid date input format. use YYYYMM, YYYYMMDD, or YYYYMMDD-YYYYMMDD."
            )

    def matches_range(filename, start_date, end_date):
        """
        Checks if a file's name matches a given date range.

        Args:
            filename (str): The name of the file to check.
            start_date (datetime): The start of the date range.
            end_date (datetime): The end of the date range.

        Returns:
            bool: True if the file matches the range, False otherwise.
        """
        # match files with exact dates (YYYYMMDD.zip)
        date_match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
        # match files with date ranges (e.g., 01-12Feb2020.zip)
        range_match = re.search(r"(\d{2})-(\d{2})([A-Za-z]+)\d{4}", filename)

        if date_match:
            file_date = datetime.strptime(date_match.group(0), "%Y%m%d")
            return start_date <= file_date <= end_date
        elif range_match:
            month_str = range_match.group(3)
            month_number = datetime.strptime(month_str, "%b").month
            year = int(re.search(r"\d{4}", filename).group(0))
            range_start = datetime(year, month_number, int(range_match.group(1)))
            range_end = datetime(year, month_number, int(range_match.group(2)))
            return range_start <= end_date and start_date <= range_end
        return False

    # parse user input into start and end dates
    start_date, end_date = parse_date_input(date_input)
    matching_files = []

    # walk through the directory structure
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".zip") and matches_range(file, start_date, end_date):
                matching_files.append(os.path.join(root, file))

    return matching_files
