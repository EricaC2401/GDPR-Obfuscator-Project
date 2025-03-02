import pandas as pd
import io
import ijson
from typing import Literal
import pyarrow.parquet as pq
import hashlib
import random
from src.setup_logger import setup_logger


logger = setup_logger(__name__)

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
    logger.info(f"Obfuscating fields: {fields_list} with method: {method}")
    valid_methods = ['mask', 'hash', 'random_hash', 'replace']
    if method not in valid_methods:
        logger.error(f"Invalid method: {method}. Accepted methods are {valid_methods}.")
        raise ValueError(f"Unknown method: {method}. " +
                         "Only 'mask', 'hash', 'random_hash', or 'replace' are accepted.")
    for field in fields_list:
        if field in df.columns:
            try:
                if method == 'mask':
                    logger.debug(f"Masking field: {field}")
                    df[field] = df[field].apply(
                            lambda x: x[0] + '*'*(len(x)-2) + x[-1]
                            if len(x) > 2 else '*'*len(x)
                        )
                elif method == 'hash':
                    logger.debug(f"Hashing field: {field}")
                    df[field] = df[field].apply(
                            lambda x: hashlib.sha256(
                                x.encode('utf-8')).hexdigest()
                        )
                elif method == 'random_hash':
                    salt = str(random.randint(0, 99999))
                    logger.debug(f"Random hashing field: {field} with salt {salt}")
                    df[field] = df[field].apply(
                            lambda x: hashlib.sha256(
                                (x+salt).encode('utf-8')).hexdigest()
                        )
                elif method == 'replace':
                    logger.debug(f"Replacing field: {field} with '***'")
                    df[field] = '***'
            except Exception:
                logger.error(f"Unexpected error occurred while processing field: {field} - {str(e)}")
                df[field] = '***'
        else:
            logger.warning(f"Field '{field}' not found in the DataFrame.")
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
    logger.info(f"Processing chunk of size {len(chunk)}")
    try:
        obfuscated_df = obfuscate_fields_in_df(chunk, fields_list, obfuscate_method)
        obfuscated_df.to_csv(output, index=False, header=is_first_chunk)
        logger.info("Chunk processed and written to output.")
    except Exception as e:
        logger.error(f"Error processing chunk: {str(e)}")
        raise


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
    logger.info(f"Processing JSON data with chunk size {chunk_size}")
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
        logger.info("Processed remaining JSON objects.")


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
    logger.info(f"Processing Parquet data with chunk size {chunk_size}")
    parquet_file = pq.ParquetFile(file_content)
    is_first_chunk = True

    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        chunk_df = batch.to_pandas()
        process_df_chunk(chunk_df, fields_list, output,
                         is_first_chunk, obfuscate_method)
        is_first_chunk = False
    logger.info("Finished processing Parquet chunks.")


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
    logger.info(f"Converting file of type {file_type} with chunk size {chunk_size}")
    if file_type not in ['csv', 'json', 'parquet']:
        logger.error(f"Unsupported file type: {file_type}")
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
    logger.info("File successfully converted and obfuscated.")
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
    logger.info(f"Converting CSV to {output_format} format.")
    csv_bytes.seek(0)
    df = pd.read_csv(csv_bytes)
    output = io.BytesIO()
    if output_format == 'json':
        df.to_json(output, orient='records', lines=True)
    elif output_format == 'parquet':
        df.to_parquet(output, index=False, engine='pyarrow')
    else:
        logger.error(f"Unsupported output format: {output_format}")
        raise ValueError("Unsupported format." +
                         " Only 'json' and 'parquet' are allowed.")
    output.seek(0)
    logger.info(f"Conversion to {output_format} completed.")
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
    logger.info(f"Obfuscating file of type {file_type}" +
                " with obfuscation method: {obfuscate_method}")
    try:
        file_type = file_type.lower()
        output = convert_str_file_content_to_obfuscated_csv(
                file_content, fields_list, file_type,
                chunk_size, obfuscate_method)
        if output_format is None:
            output_format = file_type
        elif output_format not in ['csv', 'json', 'parquet']:
            logger.error(f"Unsupported output format: {output_format}")
            raise ValueError(f"Sorry that {output_format} is not supported. " +
                             "This tool currently only support " +
                             "csv/json/parquet")
        if output_format != 'csv':
            output = convert_csv_to_output_format(output, output_format)
        logger.info(f"File obfuscation completed successfully.")
        return output
    except KeyError as ke:
        logger.error(f"KeyError occurred: {str(ke)}")
        raise
    except ValueError as ve:
        logger.error(f"ValueError occurred: {str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        raise