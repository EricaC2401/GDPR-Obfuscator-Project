import pytest
from src.utils import read_s3_file, write_s3_file, json_input_handler
import boto3
from moto import mock_aws
import os
import io
import pandas as pd
import json


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
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={'LocationConstraint': "eu-west-2"}
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
        s3_client.put_object(
            Bucket='test_bucket',
            Key='test_file.xlsx',
            Body=io.BytesIO(b'Dummy binary data')
        )
        yield boto3.client('s3')


@pytest.fixture
def test_output_file_content():
    content = "student_id,name,course,graduation_date," + \
                "email_address\n" + \
                "1234,***,Software,2024-03-31," + \
                "***\n" + \
                "5678,***,DE,2024-06-31," + \
                "***\n"
    return io.BytesIO(content.encode('utf8'))


class TestReadCsv:
    @pytest.mark.it('Test if the output type is tuple[str, str]')
    def test_output_type_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.csv')
        assert isinstance(result, tuple)
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)

    @pytest.mark.it('Test if the output has correct content_str')
    def test_output_content_file_content_str_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.csv')
        df_test = pd.read_csv(io.StringIO(result[0]))
        assert df_test.shape == (2, 5)
        assert df_test.iloc[0]['student_id'] == 1234
        assert df_test.iloc[0]['name'] == 'John Smith'
        assert df_test.iloc[0]['course'] == 'Software'
        assert df_test.iloc[0]['graduation_date'] == '2024-03-31'
        assert df_test.iloc[0]['email_address'] == 'j.smith@email.com'

    @pytest.mark.it('Test if the output has correct file_extension')
    def test_output_content_file_extension_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.csv')
        assert result[1] == 'csv'

    @pytest.mark.it('Test ValueError when an unsupported type is inputed')
    def test_unsupported_file_type(self, s3_client):
        with pytest.raises(ValueError):
            read_s3_file('test_bucket', 'test_file.xlsx')


class TestWriteCsv:
    @pytest.mark.it('Test if the correct message would be ' +
                    'return when the file is successfully uploaded')
    def test_correct_message_returned_when_successful(
            self, s3_client, test_output_file_content):
        message = write_s3_file(
            'test_bucket', 'test_output_file.csv', test_output_file_content)
        assert message == "test_output_file.csv has been "\
            "successfully uploaded to s3 bucket test_bucket"

    @pytest.mark.it('Test if the correct content is uploaded')
    def test_correct_content_is_uploaded(
            self, s3_client, test_output_file_content):
        test_file_content = "student_id,name,course,graduation_date," + \
                "email_address\n" + \
                "1234,***,Software,2024-03-31," + \
                "***\n" + \
                "5678,***,DE,2024-06-31," + \
                "***\n"

        write_s3_file(
            'test_bucket', 'test_output_file.csv', test_output_file_content)

        response = s3_client.get_object(Bucket='test_bucket',
                                        Key='test_output_file.csv')
        retrieved_content = response['Body'].read().decode('utf8')
        assert retrieved_content.strip() == test_file_content.strip()

    @pytest.mark.it('Test ValueError when a file_key '
                    'with unsupported type of extension is inputed')
    def test_unsupported_file_type(self, s3_client, test_output_file_content):
        with pytest.raises(ValueError):
            write_s3_file(
                'test_bucket', 'test_output_file.xlsx',
                test_output_file_content)


class TestJsonHandler:
    @pytest.mark.it('Test if return the correct type of output')
    def test_correct_output_type(self):
        json_dict = {
                        "file_to_obfuscate": "s3://my_ingestion_bucket" +
                                             "/new_data/file1.csv",
                        "pii_fields": ["name", "email_address"]
                    }
        json_string = json.dumps(json_dict)
        result = json_input_handler(json_string)
        assert isinstance(result, tuple)
        assert isinstance(result[0],str)
        assert isinstance(result[1],str)
        assert isinstance(result[2],list)

    @pytest.mark.it('Test if return the correct content of output')
    def test_correct_output_content(self):
        json_dict = {
                        "file_to_obfuscate": "s3://my_ingestion_bucket" +
                                             "/new_data/file1.csv",
                        "pii_fields": ["name", "email_address"]
                    }
        json_string = json.dumps(json_dict)
        result = json_input_handler(json_string)
        assert len(result) == 3
        assert result[0] == "my_ingestion_bucket"
        assert result[1] == "new_data/file1.csv"
        assert result[2] == ["name", "email_address"]