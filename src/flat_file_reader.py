"""Backward-compatibility shim for flat_file_reader."""
try:
    from skypoints.ingestion.flat_file import read_pipe_delimited_file, extract_date_from_file_path
    from skypoints.ingestion.csv_reader import read_csv_file
    from skypoints.ingestion.xlsx_reader import read_xlsx_file
except ImportError:
    from src.skypoints.ingestion.flat_file import read_pipe_delimited_file, extract_date_from_file_path
    from src.skypoints.ingestion.csv_reader import read_csv_file
    from src.skypoints.ingestion.xlsx_reader import read_xlsx_file
