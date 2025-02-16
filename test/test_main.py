import boto3
from moto import mock_aws
import pytest
from unittest.mock import patch
import os
from src.main import read_obfuscate_write_s3
import json
import io


@pytest.fixture()
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def s3_client(aws_credentials):
    with mock_aws():
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket='test_bucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        test_file_content = "student_id,name,course,graduation_date,"\
                            "email_address\n"\
                            "1234,John Smith,Software,2024-03-31,"\
                            "j.smith@email.com\n"\
                            "5678,Steve Lee,DE,2024-06-31,"\
                            "sl123@email.com\n"
        s3_client.put_object(
            Bucket='test_bucket',
            Key='test_file.csv',
            Body=io.BytesIO(test_file_content.encode('utf8'))
        )
        yield boto3.client('s3')


@pytest.fixture
def test_csv_output_file_content():
    content = "student_id,name,course,graduation_date," + \
                "email_address\n" + \
                "1234,***,Software,2024-03-31," + \
                "***\n" + \
                "5678,***,DE,2024-06-31," + \
                "***\n"
    return io.BytesIO(content.encode('utf8'))


class TestROW:
    @pytest.mark.it('Test if read_obfuscate_write_s3 correctly' +
                    'invoke the inner functions')
    def test_correctly_invoke_inner_functions(
            self, s3_client, test_csv_output_file_content):
        test_file_content = "student_id,name,course,graduation_date,"\
                            "email_address\n"\
                            "1234,John Smith,Software,2024-03-31,"\
                            "j.smith@email.com\n"\
                            "5678,Steve Lee,DE,2024-06-31,"\
                            "sl123@email.com\n"
        test_file_type = 'csv'
        with patch('src.main.read_s3_file',
                   return_value=(test_file_content,
                                 test_file_type)) as mock_read, \
            patch('src.main.obfuscate_file',
                  return_value=test_csv_output_file_content)\
            as mock_obfuscate, \
                patch('src.main.write_s3_file') as mock_write:
            json_dict = {
                        "file_to_obfuscate": "s3://test_bucket" +
                                             "/new_data/test_file.csv",
                        "pii_fields": ["name", "email_address"]
                    }
            json_str = json.dumps(json_dict)
            read_obfuscate_write_s3(json_str)

            mock_read.assert_called_once_with('test_bucket',
                                              'new_data/test_file.csv')
            mock_obfuscate.assert_called_once_with(test_file_content,
                                                   ["name", "email_address"],
                                                   test_file_type)
            mock_write.assert_called_once_with('test_bucket',
                                               'processed_data/test_file.csv',
                                               test_csv_output_file_content)
