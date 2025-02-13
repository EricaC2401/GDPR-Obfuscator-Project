import boto3
import io


def read_s3_file(s3_bucket: str, file_key: str) -> tuple[str, str]:
    '''
    Load and read a file from the specified s3_bucket
    and returns its content as a tuple of str

    Args:
        s3_bucket (str): name of the s3_bucket where the file is stored
        file_key (str): name of the file to be obfuscated, e.g filename.csv

    Returns:
        tuple [str,str]: File content as a str and its file type
    '''

    s3_client = boto3.client('s3')

    obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    file_extension = file_key.split('.')[-1].lower()

    content = obj['Body'].read()

    try:
        if file_extension == 'csv':
            content_str = content.decode('utf8')
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
        return (content_str, file_extension)
    except ValueError as ve:
        raise ve
    except Exception as e:
        raise Exception(f'Unexpected Error: {e}')


def write_s3_file(s3_bucket: str, file_key: str, file_content: io.BytesIO):
    '''
    Write an obfuscated file back to s3.

    Args:
        s3_bucket (str): name of the s3_bucket where the file is stored
        file_key (str): name of the file to be obfuscated
        file_content (io.BytesIO): Byte system of the obfuscated file
                                   e.g filename.csv
    '''

    s3_client = boto3.client("s3")

    file_extension = file_key.split(".")[-1].lower()

    try:
        if file_extension == 'csv':
            body_content = file_content.getvalue().decode('utf8')
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
        s3_client.put_object(
                Bucket=s3_bucket,
                Key=file_key,
                Body=body_content
            )
        return (f"{file_key} has been successfully "
                f"uploaded to s3 bucket {s3_bucket}")
    except ValueError as ve:
        raise ve
    except Exception as e:
        raise Exception(f'Unexpected Error: {e}')
