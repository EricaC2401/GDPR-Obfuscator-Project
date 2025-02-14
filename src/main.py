from src.obfuscator import obfuscate_file
from src.utils import read_s3_file, write_s3_file, json_input_handler


def read_obfuscate_write_s3(json_string: str):
    '''
    Process the file obfuscation

    Args:
        json_input (str): A json string contraining 2 pairs -
        "file_to_obfuscate" as a str and
        "pii_fields" as a list
    '''
    try:
        s3_bucket, file_key, fields_list = json_input_handler(json_string)

        content_str, file_extension = read_s3_file(s3_bucket, file_key)

        content_BytesIO = obfuscate_file(
            content_str, fields_list, file_extension)

        output_file_key = file_key.replace('new_data', 'processed_data')
        write_s3_file(s3_bucket, output_file_key, content_BytesIO)

        return 'Obfuscated file saved to s3://{s3_bucket}/{output_file_key}'
    except Exception as e:
        raise Exception({e})
