import pandas as pd
import io
import ijson
from typing import Literal
import pyarrow.parquet as pq
import hashlib
import random


def obfuscate_fields_in_df(
        df: pd.DataFrame, fields_list: list,
        method: str = 'replace') -> pd.DataFrame:
    '''
    Obfuscates the specified fields in the provided Dataframe

    Args:
        df (pd.DataFrame): Dataframe to obfuscate
        fields_list (list): fields to be obfuscated
        method (str) ['mask'/'hash'/'random_hash'/'replace']:
            how to obfuscate the data, default to be 'replace'
            Available methods:
            - 'mask': Masks all characters except the first and last
                      (e.g., "j********e").
            - 'hash': Applies SHA-256 hashing, producing a
                      deterministic fixed-length hash.
            - 'random_hash': Applies SHA-256 hashing with a random salt,
               producing different hashes on each run.
            - 'replace': Replaces all values in the specified fields
                         with '***'.

    Returns:
        pd.DataFrame: Dataframe with specified fields obfuscated
    '''
    for field in fields_list:
        if field in df.columns:
            try:
                if method == 'mask':
                    df[field] = df[field].apply(
                            lambda x: x[0] + '*'*(len(x)-2) + x[-1]
                            if len(x) > 2 else '*'*len(x)
                        )
                elif method == 'hash':
                    df[field] = df[field].apply(
                            lambda x: hashlib.sha256(
                                x.encode('utf-8')).hexdigest()
                        )
                elif method == 'random_hash':
                    salt = str(random.randint(0, 99999))
                    df[field] = df[field].apply(
                            lambda x: hashlib.sha256(
                                (x+salt).encode('utf-8')).hexdigest()
                        )
                elif method == 'replace':
                    df[field] = '***'
                else:
                    raise ValueError
            except ValueError:
                raise ValueError(f"Unknown method: {method}." +
                                 "Only 'mask', 'hash'," +
                                 "'random_hash', or 'replace' are accepted")
            except Exception:
                df[field] = '***'
        else:
            raise KeyError(f"Field '{field}' not" +
                           "found in the data.")
    return df


def process_df_chunk(
        chunk: pd.DataFrame, fields_list: list[str],
        output: io.BytesIO, is_first_chunk: bool,
        obfuscate_method: str = 'replace'
        ):
    '''
    Process df, obfuscating the specified fields

    Args:
        chunk (pd.DataFrame): DataFrame chunk of the CSV file
        fields_list (list): fields to be obfuscated
        output (io.BytesIO): Byte system to write the output
        is_first_chunk (bool): Whether this is the first chunk
        obfuscate_method (str) ['mask'/'hash'/'random_hash'/'replace']:
            how to obfuscate the data, default to be 'repalce'
            Available methods:
            - 'mask': Masks all characters except the first and last
                (e.g., "j********e").
            - 'hash': Applies SHA-256 hashing, producing a deterministic
                fixed-length hash.
            - 'random_hash': Applies SHA-256 hashing with a random salt,
               producing different hashes on each run.
            - 'replace': Replaces all values in the specified fields
                with '***'.
    '''
    obfuscated_df = obfuscate_fields_in_df(chunk, fields_list)
    obfuscated_df.to_csv(output, index=False, header=is_first_chunk)


def process_json_chunk(
        file_content: str, fields_list: list[str],
        output: io.BytesIO, chunk_size: int,
        obfuscate_method: str = 'replace'
        ):
    '''
    Process JSON data in chunk, obfuscating the specified fields

    Args:
        file_content (str): raw data as a string
        fields_list (list): fields to be obfuscated
        output (io.BytesIO): Byte system to write the output
        chunk_size (int): number of rows to process at a time
        obfuscate_method (str) ['mask'/'hash'/'random_hash'/'replace']:
            how to obfuscate the data, default to be 'repalce'
            Available methods:
            - 'mask': Masks all characters except the first and last
                (e.g., "j********e").
            - 'hash': Applies SHA-256 hashing, producing a deterministic
                fixed-length hash.
            - 'random_hash': Applies SHA-256 hashing with a random salt,
               producing different hashes on each run.
            - 'replace': Replaces all values in the specified fields
                with '***'.
    '''
    json_objects = ijson.items(file_content.encode('utf8'), 'item')
    chunk = []
    is_first_chunk = True
    for obj in json_objects:
        chunk.append(obj)
        if len(chunk) == chunk_size:
            step_df = pd.DataFrame(chunk)
            process_df_chunk(step_df, fields_list,
                             output, is_first_chunk, obfuscate_method)
            is_first_chunk = False
            chunk = []
    if chunk:
        step_df = pd.DataFrame(chunk)
        process_df_chunk(step_df, fields_list, output,
                         is_first_chunk, obfuscate_method)


def process_parquet_chunk(
        file_content: str, fields_list: list[str],
        output: io.BytesIO, chunk_size: int,
        obfuscate_method: str = 'replace'
        ):
    '''
    Process a parquet data in chunk, obfuscating the specified fields

    Args:
        file_content (str): raw data as a string
        fields_list (list): fields to be obfuscated
        output (io.BytesIO): Byte system to write the output
        chunk_size (int): number of rows to process at a time
        obfuscate_method (str) ['mask'/'hash'/'random_hash'/'replace']:
            how to obfuscate the data, default to be 'repalce'
            Available methods:
            - 'mask': Masks all characters except the first and last
                (e.g., "j********e").
            - 'hash': Applies SHA-256 hashing, producing a deterministic
                fixed-length hash.
            - 'random_hash': Applies SHA-256 hashing with a random salt,
               producing different hashes on each run.
            - 'replace': Replaces all values in the specified fields
                with '***'.
    '''
    parquet_file = pq.ParquetFile(file_content)
    is_first_chunk = True

    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        chunk_df = batch.to_pandas()
        process_df_chunk(chunk_df, fields_list, output,
                         is_first_chunk, obfuscate_method)
        is_first_chunk = False


def convert_str_file_content_to_obfuscated_csv(
        file_content: str, fields_list: list[str],
        file_type: Literal['csv', 'json', 'parquet'] = 'csv',
        chunk_size: int = 5000,
        obfuscate_method: str = 'replace'
        ) -> io.BytesIO:
    '''
    Obfuscate the specified field in the file content

    Args:
        file_content (str): raw data as a string
        fields_list (list): fields to be obfuscated
        file_type (str): file type (csv/json/parquet) in the output byte system
        chunk_size (int): number of rows to process at a time, 5000 by default
        obfuscate_method (str) ['mask'/'hash'/'random_hash'/'replace']:
            how to obfuscate the data, default to be 'repalce'
            Available methods:
            - 'mask': Masks all characters except the first and last
                (e.g., "j********e").
            - 'hash': Applies SHA-256 hashing, producing a deterministic
                fixed-length hash.
            - 'random_hash': Applies SHA-256 hashing with a random salt,
               producing different hashes on each run.
            - 'replace': Replaces all values in the specified fields
                with '***'.

    Returns:
        io.BytesIO: Obfuscated file as csv in a byte system
    '''
    if file_type not in ['csv', 'json', 'parquet']:
        raise ValueError(f"Sorry that {file_type} is not supported. " +
                         "This tool currently only support csv/json/parquet")
    output = io.BytesIO()
    is_first_chunk = True
    if file_type == 'csv':
        chunk_iter = pd.read_csv(io.StringIO(file_content),
                                 chunksize=chunk_size)
        for chunk in chunk_iter:
            process_df_chunk(chunk, fields_list, output,
                             is_first_chunk, obfuscate_method)
            is_first_chunk = False
    elif file_type == 'json':
        process_json_chunk(file_content, fields_list, output,
                           chunk_size, obfuscate_method)
    elif file_type == 'parquet':
        process_parquet_chunk(file_content, fields_list, output,
                              chunk_size, obfuscate_method)

    output.seek(0)
    return output


def convert_csv_to_output_format(
        csv_bytes: io.BytesIO, output_format: Literal['json', 'parquet']
        ) -> io.BytesIO:
    '''
    Convert an obfuscated CSV stored in io.BytesIO to JSON or PARQUET format

    Args:
        csv_bytes (io.BytesIO): Obfuscated CSV file in bytes
        output_format (str): Desired output format ('json' or 'parquet')

    Returns:
        io.BytesIO: Converted file in json or parquet
    '''
    csv_bytes.seek(0)
    df = pd.read_csv(csv_bytes)
    output = io.BytesIO()
    if output_format == 'json':
        df.to_json(output, orient='records', lines=True)
    elif output_format == 'parquet':
        df.to_parquet(output, index=False, engine='pyarrow')
    else:
        raise ValueError("Unsupported format." +
                         " Only 'json' and 'parquet' are allowed.")
    output.seek(0)
    return output


def obfuscate_file(
        file_content: str, fields_list: list, file_type: str = 'csv',
        output_format: str = None, chunk_size: int = 5000,
        obfuscate_method: str = 'replace'
        ) -> io.BytesIO:
    '''
    Obfuscate the specified field in the file content

    Args:
        file_content (str): raw data as a string
        fields_list (list): fields to be obfuscated
        file_type (str): file type (e.g. csv) in the output byte system
        output_format (str): Desired ourput format (csv/json/parquet)
                             ,same as file_type by default
        chunk_size (int): number of rows to process at a time, 5000 by default
        obfuscate_method (str) ['mask'/'hash'/'random_hash'/'replace']:
            how to obfuscate the data, default to be 'repalce'
            Available methods:
            - 'mask': Masks all characters except the first and last
                (e.g., "j********e").
            - 'hash': Applies SHA-256 hashing, producing a deterministic
                fixed-length hash.
            - 'random_hash': Applies SHA-256 hashing with a random salt,
               producing different hashes on each run.
            - 'replace': Replaces all values in the specified fields
                with '***'.

    Returns:
        io.BytesIO: Obfuscated file (as specified in file_type,
        csv by default) in a byte system
    '''
    try:
        file_type = file_type.lower()
        output = convert_str_file_content_to_obfuscated_csv(
                file_content, fields_list, file_type,
                chunk_size, obfuscate_method)
        if output_format is None:
            output_format = file_type
        elif output_format not in ['csv', 'json', 'parquet']:
            raise ValueError(f"Sorry that {output_format} is not supported. " +
                             "This tool currently only support " +
                             "csv/json/parquet")
        if output_format != 'csv':
            output = convert_csv_to_output_format(output, output_format)
        return output
    except KeyError as ke:
        raise KeyError(f'Error processing data chunk: {str(ke)}')
    except ValueError as ve:
        raise ValueError(f'Error reading the file: {str(ve)}')
    except Exception as e:
        raise Exception(f"Unexpected error in obfuscating file: {str(e)}")
