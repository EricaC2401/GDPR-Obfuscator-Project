import pytest
from src.utils import read_s3_file, write_s3_file, json_input_handler
import boto3
from moto import mock_aws
import os
import io
import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq


@pytest.fixture()
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture()
def json_input():
    json_dict = {
                    "file_to_obfuscate": "s3://my_ingestion_bucket" +
                                         "/new_data/file1.csv",
                    "pii_fields": ["name", "email_address"]
                }
    json_string = json.dumps(json_dict)
    return json_string


@pytest.fixture()
def json_data():
    json_dict = [
                    {
                        "student_id": "1234",
                        "name": "John Smith",
                        "course": "Software",
                        "graduation_date": "2024-03-31",
                        "email_address": "j.smith@email.com"
                    },
                    {
                        "student_id": "5678",
                        "name": "Steve Lee",
                        "course": "DE",
                        "graduation_date": "2024-06-31",
                        "email_address": "sl123@email.com"
                    }
                ]
    json_string = json.dumps(json_dict)
    return io.BytesIO(json_string.encode('utf8'))


@pytest.fixture()
def s3_client(aws_credentials, json_data):
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
        s3_client.put_object(
            Bucket='test_bucket',
            Key='test_file.json',
            Body=json_data
        )
        json_data.seek(0)
        json_dict = json.load(json_data)
        df = pd.DataFrame(json_dict)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")
        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket='test_bucket',
            Key='test_file.parquet',
            Body=parquet_buffer.getvalue()
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


@pytest.fixture
def test_parquet_output_file_content():
    content = [
                {"student_id": "1234", "name": "John Smith",
                    "course": "Software", "graduation_date": "2024-03-31",
                    "email_address": "j.smith@email.com"},
                {"student_id": "5678", "name": "Steve Lee",
                    "course": "DE", "graduation_date": "2024-06-31",
                    "email_address": "sl123@email.com"}
              ]
    df = pd.DataFrame(content)
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)
    return parquet_buffer


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
        with pytest.raises(ValueError, match='Unsupported file type: xlsx'):
            read_s3_file('test_bucket', 'test_file.xlsx')


class TestReadJson:
    @pytest.mark.it('Test if the output type is tuple[str, str]')
    def test_output_type_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.json')
        assert isinstance(result, tuple)
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)

    @pytest.mark.it('Test if the output has correct content_str')
    def test_output_content_file_content_str_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.json')
        data_dict = json.loads(result[0])
        df_test = pd.json_normalize(data_dict)
        assert df_test.shape == (2, 5)
        assert df_test.iloc[0]['student_id'] == '1234'
        assert df_test.iloc[0]['name'] == 'John Smith'
        assert df_test.iloc[0]['course'] == 'Software'
        assert df_test.iloc[0]['graduation_date'] == '2024-03-31'
        assert df_test.iloc[0]['email_address'] == 'j.smith@email.com'

    @pytest.mark.it('Test if the output has correct file_extension')
    def test_output_content_file_extension_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.json')
        assert result[1] == 'json'


class TestReadParquet:
    @pytest.mark.it('Test if the output type is tuple[str, str]')
    def test_output_type_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.parquet')
        assert isinstance(result, tuple)
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)

    @pytest.mark.it('Test if the output has correct content_str')
    def test_output_content_file_content_str_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.parquet')
        df_test = pd.read_csv(io.StringIO(result[0]))
        assert df_test.shape == (2, 5)
        assert df_test.iloc[0]['student_id'] == 1234
        assert df_test.iloc[0]['name'] == 'John Smith'
        assert df_test.iloc[0]['course'] == 'Software'
        assert df_test.iloc[0]['graduation_date'] == '2024-03-31'
        assert df_test.iloc[0]['email_address'] == 'j.smith@email.com'

    @pytest.mark.it('Test if the output has correct file_extension')
    def test_output_content_file_extension_correct(self, s3_client):
        result = read_s3_file('test_bucket', 'test_file.parquet')
        assert result[1] == 'parquet'


class TestWriteCsv:
    @pytest.mark.it('Test if the correct message would be ' +
                    'returned when the file is successfully uploaded')
    def test_correct_message_returned_when_successful(
            self, s3_client, test_csv_output_file_content):
        message = write_s3_file(
            'test_bucket', 'test_output_file.csv',
            test_csv_output_file_content)
        assert message == "test_output_file.csv has been "\
            "successfully uploaded to s3 bucket test_bucket"

    @pytest.mark.it('Test if the correct content is uploaded')
    def test_correct_content_is_uploaded(
            self, s3_client, test_csv_output_file_content):
        test_file_content = "student_id,name,course,graduation_date," + \
                "email_address\n" + \
                "1234,***,Software,2024-03-31," + \
                "***\n" + \
                "5678,***,DE,2024-06-31," + \
                "***\n"
        write_s3_file(
            'test_bucket', 'test_output_file.csv',
            test_csv_output_file_content)

        response = s3_client.get_object(Bucket='test_bucket',
                                        Key='test_output_file.csv')
        retrieved_content = response['Body'].read().decode('utf8')
        assert retrieved_content.strip() == test_file_content.strip()

    @pytest.mark.it('Test ValueError when a file_key '
                    'with unsupported type of extension is inputed')
    def test_unsupported_file_type(
            self, s3_client, test_csv_output_file_content):
        with pytest.raises(ValueError):
            write_s3_file(
                'test_bucket', 'test_output_file.xlsx',
                test_csv_output_file_content)


class TestWriteJson:
    @pytest.mark.it('Test if the correct message would be ' +
                    'return when the file is successfully uploaded')
    def test_correct_message_returned_when_successful(
            self, s3_client, json_data):
        message = write_s3_file(
            'test_bucket', 'test_output_file.json', json_data)
        assert message == "test_output_file.json has been "\
            "successfully uploaded to s3 bucket test_bucket"

    @pytest.mark.it('Test if the correct content is uploaded')
    def test_correct_content_is_uploaded(
            self, s3_client, json_data):
        test_file_content = [
                                {
                                    "student_id": "1234",
                                    "name": "John Smith",
                                    "course": "Software",
                                    "graduation_date": "2024-03-31",
                                    "email_address": "j.smith@email.com"
                                },
                                {
                                    "student_id": "5678",
                                    "name": "Steve Lee",
                                    "course": "DE",
                                    "graduation_date": "2024-06-31",
                                    "email_address": "sl123@email.com"
                                }
                            ]
        write_s3_file(
            'test_bucket', 'test_output_file.json', json_data)

        response = s3_client.get_object(Bucket='test_bucket',
                                        Key='test_output_file.json')

        retrieved_content = response['Body'].read()
        assert json.loads(retrieved_content) == test_file_content


class TestWriteParquet:
    @pytest.mark.it('Test if the correct message would be ' +
                    'return when the file is successfully uploaded')
    def test_correct_message_returned_when_successful(
            self, s3_client, test_parquet_output_file_content):
        message = write_s3_file(
            'test_bucket', 'test_output_file.parquet',
            test_parquet_output_file_content)
        assert message == "test_output_file.parquet has been "\
            "successfully uploaded to s3 bucket test_bucket"

    @pytest.mark.it('Test if the correct content is uploaded')
    def test_correct_content_is_uploaded(
            self, s3_client, test_parquet_output_file_content):
        test_file_content = [
                                {"student_id": "1234", "name": "John Smith",
                                    "course": "Software",
                                    "graduation_date": "2024-03-31",
                                    "email_address": "j.smith@email.com"},
                                {"student_id": "5678", "name": "Steve Lee",
                                    "course": "DE",
                                    "graduation_date": "2024-06-31",
                                    "email_address": "sl123@email.com"}
                            ]
        write_s3_file(
            'test_bucket', 'test_output_file.parquet',
            test_parquet_output_file_content)

        response = s3_client.get_object(Bucket='test_bucket',
                                        Key='test_output_file.parquet')

        retrieved_content = response['Body'].read()
        parquet_buffer = io.BytesIO(retrieved_content)
        table = pq.read_table(parquet_buffer)
        df = table.to_pandas()
        expected_df = pd.DataFrame(test_file_content)
        pd.testing.assert_frame_equal(df, expected_df)


class TestJsonHandler:
    @pytest.mark.it('Test if return the correct type of output')
    def test_correct_output_type(self, json_input):
        result = json_input_handler(json_input)
        assert isinstance(result, tuple)
        assert isinstance(result[0], str)
        assert isinstance(result[1], str)
        assert isinstance(result[2], list)

    @pytest.mark.it('Test if return the correct content of output')
    def test_correct_output_content(self, json_input):
        result = json_input_handler(json_input)
        assert len(result) == 3
        assert result[0] == "my_ingestion_bucket"
        assert result[1] == "new_data/file1.csv"
        assert result[2] == ["name", "email_address"]

    @pytest.mark.it('Test ValueError when missing required keys')
    def test_missing_required_keys(self):
        json_dict = {
                        "file": "s3://my_ingestion_bucket" +
                                "/new_data/file1.csv",
                        "pii": ["name", "email_address"]
                    }
        json_string = json.dumps(json_dict)
        with pytest.raises(ValueError):
            json_input_handler(json_string)

    @pytest.mark.it('Test TypeError when an non-json string is given')
    def test_non_json_string(self):
        json_dict = {
                        "file_to_obfuscate": "s3://my_ingestion_bucket" +
                                             "/new_data/file1.csv",
                        "pii_fields": ["name", "email_address"]
                    }
        with pytest.raises(TypeError):
            json_input_handler(json_dict)
