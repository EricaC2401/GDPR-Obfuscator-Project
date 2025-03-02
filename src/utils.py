import boto3
import io
import json
import pyarrow.parquet as pq
from src.setup_logger import setup_logger


logger = setup_logger(__name__)

def read_s3_file(s3_bucket: str, file_key: str) -> tuple[str, str]:
    '''
    Load and read a file from the specified s3_bucket
    and returns its content and file type as a tuple of str

    Args:
        s3_bucket (str): name of the s3_bucket where the file is stored
        file_key (str): name of the file to be obfuscated, e.g filename.csv

    Returns:
        tuple [str,str]: File content as a str and its file type
    '''
    logger.debug(f"Reading file '{file_key}' from bucket '{s3_bucket}'")

    s3_client = boto3.client('s3')

    obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    file_extension = file_key.split('.')[-1].lower()

    content = obj['Body'].read()

    try:
        if file_extension in ['csv', 'json']:
            content_str = content.decode('utf8')
        elif file_extension == 'parquet':
            table = pq.read_table(io.BytesIO(content))
            content_str = table.to_pandas().to_csv(index=False)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
        logger.info(f"Successfully read '{file_key}' ({file_extension}) from S3")
        return (content_str, file_extension)
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise
    except Exception:
        logger.exception("Unexpected error occurred while reading S3 file")
        raise


def write_s3_file(s3_bucket: str, file_key: str, file_content: io.BytesIO):
    '''
    Write a file back to s3, currently support csv/json/parquet.

    Args:
        s3_bucket (str): name of the s3_bucket where the file is stored
        file_key (str): name of the file to be obfuscated
        file_content (io.BytesIO): Content to write in a byte system
    '''
    logger.debug(f"Writing file '{file_key}' to bucket '{s3_bucket}'")

    s3_client = boto3.client("s3")

    file_extension = file_key.split(".")[-1].lower()

    try:
        if file_extension in ['csv', 'json']:
            body_content = file_content.getvalue().decode('utf8')
        elif file_extension == 'parquet':
            body_content = file_content.getvalue()
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        s3_client.put_object(
                Bucket=s3_bucket,
                Key=file_key,
                Body=body_content
            )
        logger.info(f"Successfully uploaded '{file_key}' to S3 bucket '{s3_bucket}'")
        return (f"{file_key} has been successfully "
                f"uploaded to s3 bucket {s3_bucket}")
    except ValueError:
        logger.info(f"Successfully uploaded '{file_key}' to S3 bucket '{s3_bucket}'")
        raise
    except Exception:
        logger.exception("Unexpected error occurred while writing to S3")
        raise

def json_input_handler(json_input: str) -> tuple[str, str, list]:
    '''
    Handle the JSON input containing s3_url and pii_fields

    Args:
        json_input (str): A json string contraining 2 pairs -
        "file_to_obfuscate" as a str and
        "pii_fields" as a list

    Returns:
        tuple[str,str,list]: S3 Bucket Name, S3 Key Name, ppi_fields
    '''
    logger.info("Parsing JSON input")

    try:
        json_dict = json.loads(json_input)

        if 'file_to_obfuscate' not in json_dict or \
                'pii_fields' not in json_dict:
            raise ValueError("Missing required keys in JSON input")

        s3_url = json_dict['file_to_obfuscate']
        s3_bucket, file_key = s3_url.replace('s3://', '').split('/', 1)

        fields_list = json_dict['pii_fields']

        logger.debug(f"Extracted S3 bucket: {s3_bucket}, file key: {file_key}")
        return (s3_bucket, file_key, fields_list)
    except json.JSONDecodeError:
        logger.error("Invalid JSON input: Unable to decode JSON")
        raise
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise
    except Exception as e:
        logger.exception("Unexpected error occurred while parsing JSON input")
        raise
