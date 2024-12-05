# %% import necessary packages and modules

# pyjanitor // dataframe cleaner
import janitor

# zipfile // manipulates zipped directories
from zipfile import ZipFile

# python essentials
from datetime import datetime, timedelta

import pandas as pd

import subprocess
import tempfile
import fnmatch
import shutil
import os

# %% find files by a given extension and directory

def find_files_by_extension(directory, extension, substring=None):
    """
    Find all files in a directory (and subdirectories) with a specific extension
    and optionally containing a specific substring in their names.
    
    Args:
        directory (str): The path to the directory to search.
        extension (str): The file extension to look for (e.g., ".txt").
        substring (str, optional): The substring to match in the filenames. Defaults to None.

    Returns:
        list: A list of matching file paths.
    """
    matching_files = []
    
    # Traverse the directory tree
    for root, _, files in os.walk(directory):
        for file in files:
            # Check for the extension and substring (if given)
            if file.endswith(extension) and (substring is None or substring in file):
                matching_files.append(os.path.join(root, file))
    
    return matching_files

# %% finding the correct .zip folder in the eosAC data


def find_files_by_date_and_extension(base_dir, date_input, matching_extension:str=".zip"):
    """
    Finds .zip files in a directory structure that match a given date or date range.

    Args:
        base_dir (str): The base directory to search for .zip files.
        date_input (str): The date input in the format:
            - "YYYYMM" for a specific month.
            - "YYYYMMDD" for a specific day.
            - "YYYYMMDD-YYYYMMDD" for a date range.
        matching_extension (str): The desired extension of the files in question.

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
                    if file.endswith(matching_extension):
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

    # step 01: convert all names to lower case while also converting the camel case to be separated by an underscore
    df = df.clean_names(case_type="snake")

    # step 02: convert the date column to a date
    if "date" not in df.columns:
        raise KeyError(
            "'date' is not found as the name for the date column. Perhaps the it was not appropriately changed to lower case or this is not the right .dat file for this toolset. Please inspect the column names of your dataframe further."
        )
    else:
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    if "time" not in df.columns:
        raise KeyError(
            "'time' is not found as the name for the date column. Perhaps the it was not appropriately changed to lower case or this is not the right .dat file for this toolset. Please inspect the column names of your dataframe further."
        )
    else:
        df["time"] = pd.to_datetime(
            df["time"], format="%H:%M:%S.%f", errors="coerce"
        ).dt.time

    # step 03: merge date and time into `datetime`
    df["datetime"] = df.apply(
        lambda row: pd.Timestamp.combine(row["date"], row["time"]), axis=1
    )
    cols = list(df.columns)
    cols.insert(2, cols.pop(cols.index("datetime")))
    df = df[cols]

    # step 04: sort by datetime
    df = df.sort_values(by="datetime", ascending=False).reset_index(drop=True)

    return df


# %% does the table in question already exist


def table_exists(schema, table_name, engine):
    query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
    );
    """
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            query
        ).scalar()  # `.scalar()` returns a single value
    return result


# %% write large dataframes to the same sql table


def write_and_clean_chunk_to_sql(chunk, table_name, engine, schema, batch_size):
    """
    Process a data chunk, clean it, and write it to a SQL database.

    This function performs the following operations:
    1. Computes the chunk if it's a lazy object.
    2. Converts 'date' and 'time' columns to datetime objects.
    3. Creates a 'datetime' column by combining 'date' and 'time'.
    4. Reorders columns, moving 'datetime' to the third position.
    5. Drops the original 'date' and 'time' columns.
    6. Writes the processed chunk to a SQL database.

    Args:
        chunk (pandas.DataFrame or dask.dataframe.DataFrame): The data chunk to process.
        table_name (str): Name of the SQL table to write to.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.
        schema (str): The database schema to use.
        batch_size (int): Number of rows to write in each batch.

    Returns:
        None

    Raises:
        ValueError: If the chunk doesn't contain required columns or if data types are incompatible.
        SQLAlchemyError: If there's an error writing to the database.
    """

    chunk = chunk.compute()

    chunk["date"] = pd.to_datetime(chunk["date"], format="%Y-%m-%d", errors="coerce")
    chunk["time"] = pd.to_timedelta(chunk["time"])
    chunk["datetime"] = chunk["date"] + chunk["time"]

    # move the last column to the third position
    last_column = chunk.columns[-1]
    chunk = chunk[[col for col in chunk.columns if col != last_column] + [last_column]]

    # move the last column to the third position from the left
    cols = list(chunk.columns)
    datetime_col = cols.pop(cols.index("datetime"))
    cols.insert(2, datetime_col)
    chunk = chunk[cols]

    # drop date and time
    chunk.drop(columns=["date", "time"], inplace=True)

    chunk.info()

    chunk.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=batch_size,
    )

# %% parse times to create datetime

def parse_datetime(row):
    try:
        return pd.to_datetime(row["date"] + " " + row["time"])
    except:
        return pd.NaT
    