# %% import necessary packages and modules

# pyjanitor // dataframe cleaner
import janitor

# zipfile // manipulates zipped directories
from zipfile import ZipFile

# python essentials
from datetime import datetime, timedelta

import pandas as pd

import tempfile
import shutil
import os


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
        if "-" in date_input:
            start_date, end_date = map(str.strip, date_input.split("-"))
            return datetime.strptime(start_date, "%Y%m%d"), datetime.strptime(
                end_date, "%Y%m%d"
            )
        elif len(date_input) == 6:
            start_date = datetime.strptime(date_input, "%Y%m")
            end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(
                days=1
            )
            return start_date, end_date
        elif len(date_input) == 8:
            date = datetime.strptime(date_input, "%Y%m%d")
            return date, date
        else:
            raise ValueError(
                "Invalid date input format. Use YYYYMM, YYYYMMDD, or YYYYMMDD-YYYYMMDD."
            )

    def is_directory_in_range(dir_name, start_date, end_date):
        """
        Checks if a directory name is within the date range.

        Args:
            dir_name (str): The name of the directory (e.g., "201909").
            start_date (datetime): The start date of the range.
            end_date (datetime): The end date of the range.

        Returns:
            bool: True if the directory matches the range, False otherwise.
        """
        try:
            dir_date = datetime.strptime(dir_name, "%Y%m")
            return start_date <= dir_date <= end_date
        except ValueError:
            return False

    # TODO: delete this subfunction after comfort is given from the current version
    # def matches_range(filename, start_date, end_date):
    #     """
    #     Checks if a file's name matches a given date range.

    #     Args:
    #         filename (str): The name of the file to check.
    #         start_date (datetime): The start date of the range.
    #         end_date (datetime): The end date of the range.

    #     Returns:
    #         bool: True if the file matches the range, False otherwise.
    #     """
    #     date_match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    #     range_match = re.search(r"(\d{2})-(\d{2})([A-Za-z]+)\d{4}", filename)

    #     if date_match:
    #         file_date = datetime.strptime(date_match.group(0), "%Y%m%d")
    #         return start_date <= file_date <= end_date
    #     elif range_match:
    #         month_str = range_match.group(3)
    #         month_number = datetime.strptime(month_str, "%b").month
    #         year = int(re.search(r"\d{4}", filename).group(0))
    #         range_start = datetime(year, month_number, int(range_match.group(1)))
    #         range_end = datetime(year, month_number, int(range_match.group(2)))
    #         return range_start <= end_date and start_date <= range_end
    #     return False

    # parse the user input into start and end dates
    start_date, end_date = parse_date_input(date_input)
    matching_files = []

    # filter directories at the top level
    for dir_entry in os.scandir(base_dir):
        if dir_entry.is_dir() and is_directory_in_range(
            dir_entry.name, start_date, end_date
        ):
            print(f"{dir_entry.path}: Passed directory range test")
            # explore subfolders and collect .zip files
            for root, _, files in os.walk(dir_entry.path):
                for file in files:
                    if file.endswith(".zip"):
                        matching_files.append(os.path.join(root, file))

    return matching_files


# %% flatten nested lists


def flatten_iteratively(nested_list):
    """
    Flattens a nested list iteratively, ensuring elements are unnested only if they are lists.

    Args:
        nested_list (list): A potentially nested list.

    Returns:
        list: A flattened version of the input list.
    """
    stack = nested_list[::-1]  # reverse list for stack-like processing
    flattened = []

    while stack:
        item = stack.pop()  # pop the last item
        if isinstance(item, list):  # check if the item is a list
            stack.extend(item[::-1])  # add the list's items to the stack in reverse
        else:
            flattened.append(item)  # append non-list items directly

    return flattened


# %% unpack zip files and place data into dataframe


def process_zipped_dat_files_with_fwf(zip_path, keep_unzipped=False, output_dir=None):
    """
    Processes a zip file containing .dat files using fixed-width parsing (`pd.read_fwf`),
    concatenates the data horizontally, and returns a dataframe. It optionally retains the unzipped files.

    Args:
    - zip_path (str): Path to the zip file.
    - keep_unzipped (bool): Whether to retain unzipped files. Default is False.
    - output_dir (str): Directory to save unzipped files if `keep_unzipped` is True. Defaults to 'tmp/'.

    Returns:
    - pd.DataFrame: Combined dataframe from all .dat files.
    """

    # define temporary directory for unzipping
    unzip_dir = tempfile.mkdtemp() if not keep_unzipped else (output_dir or "./tmp/")
    os.makedirs(unzip_dir, exist_ok=True)

    # Extract the zip file
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(unzip_dir)

    # Locate all .dat files in the directory tree
    dat_files = []
    for root, dirs, files in os.walk(unzip_dir):
        for file in files:
            if file.endswith(".dat"):
                dat_files.append(os.path.join(root, file))

    # if no .dat files are found, raise an error
    if not dat_files:
        raise FileNotFoundError("No .dat files found in the provided zip file.")

    # parse each .dat file into a dataframe and store them in a list
    dataframes = []
    for f in dat_files:
        with open(f, "r") as f:
            # fixed-width parsing with inferred colspecs
            df = pd.read_fwf(f, colspecs="infer")
        dataframes.append(df)

    # attempt to concatenate the dataframes vertically
    try:
        combined_df = pd.concat(dataframes, axis=0)
    except ValueError as e:
        raise ValueError(
            "Error concatenating .dat files: ensure consistent structure."
        ) from e

    # handle unzipped files based on the keep_unzipped flag
    if keep_unzipped:
        if not output_dir:
            output_dir = "tmp/"
        os.makedirs(output_dir, exist_ok=True)
        shutil.move(unzip_dir, output_dir)
    else:
        shutil.rmtree(unzip_dir)

    return combined_df


# %% standard cleaning of the unpacked data file


def standard_df_clean(df: pd.DataFrame):

    # convert all names to lower case while also converting the camel case to be separated by an underscore
    df = df.clean_names(case_type="snake")

    return df
