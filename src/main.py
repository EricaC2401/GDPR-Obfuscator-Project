from src.obfuscator import obfuscate_file
from src.utils import read_s3_file, write_s3_file, json_input_handler
from src.pii_detection import detect_if_pii
from src.pii_detection_ai import detect_if_pii_with_gpt
from typing import Literal
import pandas as pd
import io
from src.setup_logger import setup_logger
import argparse


logger = setup_logger(__name__)


def handle_file_obfuscation(
    json_string: str,
    if_output_different_format: bool = False,
    output_format: Literal["csv", "json", "parquet", None] = None,
    chunk_size: int = 5000,
    if_save_to_s3: bool = True,
    auto_detect_pii: bool = False,
    auto_detect_pii_gpt: bool = False
):
    """
    Process the file obfuscation

    Args:
        json_input (str): A json string contraining 2 pairs -
            "file_to_obfuscate" as a str and
            "pii_fields" as a list

        if_output_different_format (bool):
            If the output is in a different format as input,
            default to be False

        output_format (str): If if_output_different_format is True,
        which format to output, default to be None
        Currently support csv/ json/ parquet

        chunk_size (int): number of rows to process at a time, 5000 by default

        if_save_to_s3 (bool): If True, save the obfuscated file to S3.
                              Otherwise, return the byte-stream object
                              instead of saving to S3.

        auto_detect_pii (bool):
            If True, automatically detect PII fields in the dataset.
            If False, use the fields list from the JSON string.

        auto_detect_pii_gpt (bool):
            If True, automatically detect PII fields in the dataset using GPT.
            If False, detect PII fields using heuristic model.
    """
    try:
        s3_bucket, file_key, fields_list = json_input_handler(json_string)
        logger.info(f"Processing file: s3://{s3_bucket}/{file_key}")

        content_str, file_extension = read_s3_file(s3_bucket, file_key)

        if auto_detect_pii:
            df_step = pd.read_csv(io.StringIO(content_str))
            if auto_detect_pii_gpt:
                gpt_result = detect_if_pii_with_gpt(list(df_step.columns))
                fields_list = [item['column_name'] for item in gpt_result
                               if item['score'] > 0.6]
                logger.info(f"Detected PII fields (GPT): {fields_list}")
            else:
                fields_list = [col_name for col_name in
                               df_step.columns if detect_if_pii(col_name)]
                logger.info(f"Detected PII fields (heuristic): {fields_list}")

        if if_output_different_format:
            logger.info(f"Obfuscating file to {output_format} format")
            content_BytesIO = obfuscate_file(
                content_str, fields_list, file_extension,
                output_format, chunk_size)
        else:
            logger.info("Obfuscating file in original format")
            content_BytesIO = obfuscate_file(
                content_str, fields_list, file_extension,
                chunk_size=chunk_size)

        if if_save_to_s3:
            input_folder_name = file_key.split('/')[0]
            output_file_key = file_key.replace(input_folder_name,
                                               "processed_data")
            write_s3_file(s3_bucket, output_file_key, content_BytesIO)
            logger.info("Saving obfuscated file to s3:" +
                        f"//{s3_bucket}/{output_file_key}")
            return ('Obfuscated file saved to s3://' +
                    f'{s3_bucket}/{output_file_key}')
        else:
            return content_BytesIO
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise Exception(str(e))


def main():

    parser = argparse.ArgumentParser('File Obfuscation Tool')
    parser.add_argument(
            'json_string',
            type=str,
            help='A JSON string specifying "file_to_obfuscate"' +
                 ' and "pii_fields". '
                 'Example:\'{"file_to_obfuscate": "s3://buc_name/file.csv"' +
                 ', "pii_fields": ["name", "email"]}\''
        )
    parser.add_argument(
            '--if_output_different_format',
            action='store_true',
            help='If set, output format will be different from input format.'
        )
    parser.add_argument(
            "--output_format",
            type=str,
            choices=["csv", "json", "parquet"],
            default=None,
            help="Output file format. Choose from csv, json, parquet."
        )
    parser.add_argument(
            '--chunk_size',
            type=int,
            default=5000,
            help='Number of rows to process at a time. Default is 5000.'
        )
    parser.add_argument(
            '--if_not_save_to_s3',
            action='store_false',
            help='If set, will not save obfuscated file back to S3.'
        )
    parser.add_argument(
            '--auto_detect_pii',
            action='store_true',
            help='Automatically detect PII fields using heuristic model.'
        )
    parser.add_argument(
            '--auto_detect_pii_gpt',
            action='store_true',
            help='Automatically detect PII fields using GPT model.'
        )

    try:
        args = parser.parse_args()

        handle_file_obfuscation(
                json_string=args.json_string,
                if_output_different_format=args.if_output_different_format,
                output_format=args.output_format,
                chunk_size=args.chunk_size,
                if_save_to_s3=args.if_not_save_to_s3,
                auto_detect_pii=args.auto_detect_pii,
                auto_detect_pii_gpt=args.auto_detect_pii_gpt
            )
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        print(f"Error occurred: {str(e)}")


if __name__ == "__main__":
    main()
