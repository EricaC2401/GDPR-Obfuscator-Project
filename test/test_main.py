import boto3
from moto import mock_aws
import pytest
from unittest.mock import patch
import os
from src.main import handle_file_obfuscation
import json
import io
import pandas as pd


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
            Key='new_data/test_file.csv',
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


class TestHFO:
    @pytest.mark.it('Test if handle_file_obfuscation correctly' +
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
            handle_file_obfuscation(json_str)

            mock_read.assert_called_once_with('test_bucket',
                                              'new_data/test_file.csv')
            mock_obfuscate.assert_called_once_with(test_file_content,
                                                   ["name", "email_address"],
                                                   test_file_type,
                                                   chunk_size=5000)
            mock_write.assert_called_once_with('test_bucket',
                                               'processed_data/test_file.csv',
                                               test_csv_output_file_content)

    @pytest.mark.it('Test if handle_file_obfuscation return BytesIO when ' +
                    'if_save_to_s3 is not True')
    def test_return_ByesIO(self, s3_client):
        json_dict = {
                        "file_to_obfuscate": "s3://test_bucket" +
                                             "/new_data/test_file.csv",
                        "pii_fields": ["name", "email_address"]
                    }
        json_str = json.dumps(json_dict)
        result = handle_file_obfuscation(json_str, if_save_to_s3=False)

        assert isinstance(result, io.BytesIO)

    @pytest.mark.it('Test when auto_detect_pii is True')
    def test_auto_detect_pii(self, s3_client):
        json_dict = {
                        "file_to_obfuscate": "s3://test_bucket" +
                                             "/new_data/test_file.csv",
                        "pii_fields": []
                    }
        json_str = json.dumps(json_dict)
        result = handle_file_obfuscation(json_str,
                                         if_save_to_s3=False,
                                         auto_detect_pii=True)
        result_df = pd.read_csv(result)
        assert result_df['name'].iloc[0] == '***'
        assert result_df['email_address'].iloc[0] == '***'
        assert result_df['course'].iloc[0] != '***'

    @pytest.mark.it('Test if corrent field_list with gpt')
    @patch('src.main.detect_if_pii_with_gpt')
    def test_correct_field_list_with_gpt(self, mock_auto_gpt, s3_client):
        mock_auto_gpt.return_value = [
                                        {"column_name": "name", "score": 0.9},
                                        {"column_name": "email_address",
                                            "score": 0.95},
                                        {"column_name": "course", "score": 0.1}
                                    ]

        json_dict = {
                        "file_to_obfuscate": "s3://test_bucket" +
                                             "/new_data/test_file.csv",
                        "pii_fields": []
                    }
        json_str = json.dumps(json_dict)
        result = handle_file_obfuscation(json_str,
                                         if_save_to_s3=False,
                                         auto_detect_pii=True,
                                         auto_detect_pii_gpt=True)
        result_df = pd.read_csv(result)
        assert result_df['name'].iloc[0] == '***'
        assert result_df['email_address'].iloc[0] == '***'
        assert result_df['course'].iloc[0] != '***'

    @pytest.mark.it('Test if corrent field_list auto without gpt')
    @patch('src.main.detect_if_pii_with_gpt')
    def test_correct_field_list_auto_without_gpt(
            self, mock_auto_gpt, s3_client):
        mock_auto_gpt.return_value = [
                                        {"column_name": "name", "score": 0.1}
                                    ]

        json_dict = {
                        "file_to_obfuscate": "s3://test_bucket" +
                                             "/new_data/test_file.csv",
                        "pii_fields": []
                    }
        json_str = json.dumps(json_dict)
        result = handle_file_obfuscation(json_str,
                                         if_save_to_s3=False,
                                         auto_detect_pii=True)
        result_df = pd.read_csv(result)
        assert result_df['name'].iloc[0] == '***'
