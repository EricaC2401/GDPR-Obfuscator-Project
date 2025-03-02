import pytest
from src.obfuscator import obfuscate_fields_in_df, obfuscate_file, \
    process_df_chunk, process_json_chunk, process_parquet_chunk, \
    convert_str_file_content_to_obfuscated_csv, \
    convert_csv_to_output_format
import pandas as pd
import json
import io
from unittest.mock import patch
import pyarrow.parquet as pq
import pyarrow as pa


@pytest.fixture
def test_csv_data():
    content = """student_id,name,course,graduation_date,email_address\n
                1234,John Smith,Software,2024-03-31,j.smith@email.com\n
                5678,Steve Lee,DE,2024-06-31,sl123@email.com\n
                """

    fields = ['name', 'email_address']
    return content, fields


@pytest.fixture
def test_json_data():
    content = [
                {"student_id": "1234", "name": "John Smith",
                    "course": "Software", "graduation_date": "2024-03-31",
                    "email_address": "j.smith@email.com"},
                {"student_id": "5678", "name": "Steve Lee",
                    "course": "DE", "graduation_date": "2024-06-31",
                    "email_address": "sl123@email.com"}
              ]
    json_content = json.dumps(content)

    fields = ['name', 'email_address']
    return json_content, fields


@pytest.fixture
def test_parquet_data():
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

    fields = ['name', 'email_address']
    return parquet_buffer, fields


class TestObfuscateFieldsInDf:
    @pytest.mark.it('Test if obfuscated fields in the list' +
                    ' with method equals replace')
    def test_obfuscate_specified_fields_default(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        df_obfuscated = obfuscate_fields_in_df(test_content,
                                               test_fields, 'replace')
        assert all(df_obfuscated['name'] == '***')
        assert all(df_obfuscated['email_address'] == '***')

    @pytest.mark.it('Test if obfuscated fields in the list' +
                    ' with method equals mask')
    def test_obfuscate_specified_fields_mask(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        df_obfuscated = obfuscate_fields_in_df(test_content,
                                               test_fields, 'mask')
        assert df_obfuscated['name'].iloc[0] == 'J********h'
        assert df_obfuscated['email_address'].iloc[0] == 'j***************m'

    @pytest.mark.it('Test if obfuscated fields in the list' +
                    ' with method equals hash')
    def test_obfuscate_specified_fields_hash(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        df_obfuscated = obfuscate_fields_in_df(test_content,
                                               test_fields, 'hash')
        assert df_obfuscated['name'].iloc[0] == \
            'ef61a579c907bbed674c0dbcbcf7f7af8f851538eef7b8e58c5bee0b8cfdac4a'
        assert df_obfuscated['email_address'].iloc[0] == \
            '06977b82208c436b4479f511df34efca1e47dca37efaaba8dd0a5516d22f070b'

    @pytest.mark.it('Test if obfuscated fields in the list' +
                    ' with method equals random_hash')
    def test_obfuscate_specified_fields__random_hash(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        df_obfuscated = obfuscate_fields_in_df(test_content,
                                               test_fields, 'random_hash')
        assert df_obfuscated['name'].iloc[0] != '***'
        assert df_obfuscated['name'].iloc[0] != \
            'ef61a579c907bbed674c0dbcbcf7f7af8f851538eef7b8e58c5bee0b8cfdac4a'
        assert len(df_obfuscated['email_address'].iloc[0]) == \
            len('06977b82208c436b4479f511df34efca1e47' +
                'dca37efaaba8dd0a5516d22f070b')

    @pytest.mark.it('Test if the other fields remains the same')
    def test_other_fields_remains_unchanged(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        df_obfuscated = obfuscate_fields_in_df(test_content, test_fields)
        assert 'student_id' in df_obfuscated.columns
        assert df_obfuscated['student_id'].iloc[0] == 1234
        assert 'graduation_date' in df_obfuscated.columns
        assert df_obfuscated['graduation_date'].iloc[0] == '2024-03-31'

    @pytest.mark.it('Test if ValueError is raised if with an invalid method')
    def test_valueerror_with_invalid_input(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        with pytest.raises(ValueError,
                           match="Unknown method: other. " +
                           "Only 'mask', 'hash', 'random_hash'," +
                           " or 'replace' are accepted"):
            obfuscate_fields_in_df(test_content, test_fields, 'other')


class TestProcessCSVChunk:
    @pytest.mark.it('Test if Obfuscate_fields_in_df is called in the function')
    @patch('src.obfuscator.obfuscate_fields_in_df')
    def test_obfuscate_fields_is_called(
            self, mock_obfuscate_fields, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        output_buffer = io.BytesIO()
        mock_obfuscate_fields.return_value = test_content.copy()
        process_df_chunk(test_content, test_fields, output_buffer, True)
        mock_obfuscate_fields.assert_called_once_with(
            test_content, test_fields, 'replace')

    @pytest.mark.it('Test if the output is a valid csv')
    def test_output_is_valid_csv(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        output_buffer = io.BytesIO()
        process_df_chunk(test_content, test_fields, output_buffer, True)
        output_buffer.seek(0)
        csv_content = output_buffer.getvalue().decode('utf8')
        assert ',' in csv_content
        assert '\n' in csv_content
        try:
            df = pd.read_csv(output_buffer)
        except Exception:
            pytest.fail("Output is not a valid CSV")
        assert not df.empty
        assert 'name' in df.columns
        assert 'email_address' in df.columns
        assert df.shape == (2, 5)

    @pytest.mark.it('Test if it works for multiple chunks')
    def test_works_for_multiple_chunks(self, test_csv_data):
        test_content, test_fields = test_csv_data
        test_content = pd.read_csv(io.StringIO(test_content))
        output_buffer = io.BytesIO()
        process_df_chunk(test_content, test_fields, output_buffer, True)
        process_df_chunk(test_content, test_fields, output_buffer, False)
        output_buffer.seek(0)
        df = pd.read_csv(output_buffer)
        assert not df.empty
        assert 'name' in df.columns
        assert 'email_address' in df.columns
        assert df.shape == (4, 5)


class TestProcessJSONChunk:
    @pytest.mark.it('Test if process_df_chunk is called in the function')
    @patch('src.obfuscator.process_df_chunk')
    def test_obfuscate_fields_is_called(
            self, mock_process_df_chunk, test_json_data):
        test_content, test_fields = test_json_data
        output_buffer = io.BytesIO()
        process_json_chunk(test_content, test_fields, output_buffer, 1)
        assert mock_process_df_chunk.call_count == 2

    @pytest.mark.it('Test if the output is a valid csv')
    def test_output_is_valid_csv(self, test_json_data):
        test_content, test_fields = test_json_data
        output_buffer = io.BytesIO()
        process_json_chunk(test_content, test_fields, output_buffer, 2)
        output_buffer.seek(0)
        csv_content = output_buffer.getvalue().decode('utf8')
        assert ',' in csv_content
        assert '\n' in csv_content
        try:
            df = pd.read_csv(output_buffer)
        except Exception:
            pytest.fail("Output is not a valid CSV")
        assert not df.empty
        assert 'name' in df.columns
        assert 'email_address' in df.columns
        assert df.shape == (2, 5)

    @pytest.mark.it('Test if it works for multiple chunks')
    def test_works_for_multiple_chunks(self, test_json_data):
        test_content, test_fields = test_json_data
        output_buffer = io.BytesIO()
        process_json_chunk(test_content, test_fields, output_buffer, 1)
        output_buffer.seek(0)
        df = pd.read_csv(output_buffer)
        assert not df.empty
        assert 'name' in df.columns
        assert 'email_address' in df.columns
        assert df.shape == (2, 5)


class TestProcessPARQUETChunk:
    @pytest.mark.it('Test if process_df_chunk is called in the function')
    @patch('src.obfuscator.process_df_chunk')
    def test_obfuscate_fields_is_called(
            self, mock_process_df_chunk, test_parquet_data):
        test_content, test_fields = test_parquet_data
        output_buffer = io.BytesIO()
        process_parquet_chunk(test_content, test_fields, output_buffer, 1)
        assert mock_process_df_chunk.call_count == 2

    @pytest.mark.it('Test if the output is a valid csv')
    def test_output_is_valid_csv(self, test_parquet_data):
        test_content, test_fields = test_parquet_data
        output_buffer = io.BytesIO()
        process_parquet_chunk(test_content, test_fields, output_buffer, 2)
        output_buffer.seek(0)
        csv_content = output_buffer.getvalue().decode('utf8')
        assert ',' in csv_content
        assert '\n' in csv_content
        try:
            df = pd.read_csv(output_buffer)
        except Exception:
            pytest.fail("Output is not a valid CSV")
        assert not df.empty
        assert 'name' in df.columns
        assert 'email_address' in df.columns
        assert df.shape == (2, 5)

    @pytest.mark.it('Test if it works for multiple chunks')
    def test_works_for_multiple_chunks(self, test_parquet_data):
        test_content, test_fields = test_parquet_data
        output_buffer = io.BytesIO()
        process_parquet_chunk(test_content, test_fields, output_buffer, 2)
        output_buffer.seek(0)
        df = pd.read_csv(output_buffer)
        assert not df.empty
        assert 'name' in df.columns
        assert 'email_address' in df.columns
        assert df.shape == (2, 5)


class TestConvertStrFileToCSV:
    @pytest.mark.it('Test process_df_chunk is called when file_type is csv')
    @patch('src.obfuscator.process_df_chunk')
    def test_process_csv_called(self, mock_process_df_chunk, test_csv_data):
        test_content, test_fields = test_csv_data
        output = convert_str_file_content_to_obfuscated_csv(test_content,
                                                            test_fields,
                                                            'csv')
        assert isinstance(output, io.BytesIO)
        assert mock_process_df_chunk.called

    @pytest.mark.it('Test process_json_chunk is called when file_type is json')
    @patch('src.obfuscator.process_json_chunk')
    def test_process_json_called(
            self, mock_process_json_chunk, test_json_data):
        test_content, test_fields = test_json_data
        output = convert_str_file_content_to_obfuscated_csv(test_content,
                                                            test_fields,
                                                            'json')
        assert isinstance(output, io.BytesIO)
        assert mock_process_json_chunk.called

    @pytest.mark.it('Test process_parquet_chunk is called when ' +
                    'file_type is parquet')
    @patch('src.obfuscator.process_parquet_chunk')
    def test_process_parquet_called(
            self, mock_process_parquet_chunk, test_parquet_data):
        test_content, test_fields = test_parquet_data
        output = convert_str_file_content_to_obfuscated_csv(test_content,
                                                            test_fields,
                                                            'parquet')
        assert isinstance(output, io.BytesIO)
        assert mock_process_parquet_chunk.called

    @pytest.mark.it("Ensures output is in CSV format")
    def test_output_is_valid_csv(self, test_csv_data):
        test_content, test_fields = test_csv_data
        output = convert_str_file_content_to_obfuscated_csv(test_content,
                                                            test_fields,
                                                            'csv')
        output.seek(0)
        try:
            df = pd.read_csv(output)
        except Exception:
            pytest.fail("Output is not a valid csv")
        assert 'name' in df.columns
        assert 'email_address' in df.columns

    @pytest.mark.it('Raises ValueError for unsupported file types')
    def test_unsupported_file_type(self):
        file_content = "dummy content"
        fields_list = ['name', 'email_address']

        with pytest.raises(ValueError,
                           match="Sorry that xml is not supported. " +
                                 "This tool currently only support " +
                                 "csv/json/parquet"):
            convert_str_file_content_to_obfuscated_csv(
                        file_content, fields_list, 'xml')


class TestConvertCsvToOutputFormat:
    @pytest.mark.it("Tests if Converts CSV to JSON format correctly")
    def test_convert_csv_to_json(self, test_csv_data):
        test_content, _ = test_csv_data
        test_content = io.BytesIO(test_content.encode('utf8'))

        output = convert_csv_to_output_format(test_content, 'json')
        data = output.read().decode('utf8').strip()
        lines = data.split("\n")
        try:
            step = [json.loads(line) for line in lines]
        except json.JSONDecodeError:
            pytest.fail("Output is not a valid JSON")
        assert isinstance(step, list)
        assert len(step) > 0
        assert "student_id" in step[0]

    @pytest.mark.it("Tests if Converts CSV to PARQUET format correctly")
    def test_convert_csv_to_parquet(self, test_csv_data):
        test_content, _ = test_csv_data
        test_content = io.BytesIO(test_content.encode('utf8'))
        output = convert_csv_to_output_format(test_content, 'parquet')
        try:
            pq.ParquetFile(output)
        except Exception:
            pytest.fail("Output is not a valid PARQUET")
        df = pd.read_parquet(output)
        assert "student_id" in df.columns
        assert df.shape[0] > 0

    @pytest.mark.it('Test if raises ValueError for unsupported file types')
    def test_unsupported_file_type(self, test_csv_data):
        test_content, _ = test_csv_data
        test_content = io.BytesIO(test_content.encode('utf8'))

        with pytest.raises(ValueError,
                           match="Unsupported format." +
                           " Only 'json' and 'parquet' are allowed."):
            convert_csv_to_output_format(test_content, 'xml')


class TestObfuscateFile:
    @pytest.mark.it('Test if inner functions are called')
    @patch('src.obfuscator.convert_str_file_content_to_obfuscated_csv')
    @patch('src.obfuscator.convert_csv_to_output_format')
    def test_inner_functions_are_called(
            self, mock_convert_csv_output,
            mock_convert_str_csv, test_csv_data):
        test_content, test_fields = test_csv_data
        mock_convert_str_csv.return_value = io.BytesIO(
                                            test_content.encode('utf8'))
        obfuscate_file(test_content, test_fields, 'csv', 'json')
        mock_convert_str_csv.assert_called_once_with(
            test_content, test_fields, 'csv', 5000, 'replace')
        mock_convert_csv_output.assert_called_once_with(
            mock_convert_str_csv.return_value, 'json'
        )

    @pytest.mark.it('Test ValueError when an unsupported type is inputed')
    def test_obfuscate_file_unsupported_file_type(self, test_csv_data):
        test_content, test_fields = test_csv_data
        with pytest.raises(ValueError):
            obfuscate_file(test_content, test_fields, 'xlsx')

    @pytest.mark.it('Test KeyError when a field in the fields_list' +
                    ' is not in the data')
    def test_obfuscate_file_unrelated_field(self, test_csv_data):
        test_content = test_csv_data[0]
        test_fields = ['name', 'cohort']
        with pytest.raises(KeyError):
            obfuscate_file(test_content, test_fields, 'csv')
