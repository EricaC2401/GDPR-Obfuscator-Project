# GDPR Obfuscator

## Project Overview
This project provides a pipeline to read files from an AWS S3 bucket, obfuscate specified personally identifiable information (PII) fields, and then write the obfuscated file back to S3. It currently supports CSV, JSON and PARQUET file formats and offers optional automatic PII detection via both heuristic and GPT-based methods.. 

## Features
- **Read file from s3**: Support CSV, JSON and PARQUET file format.
- **Obfuscate PII fields**: Replace specified sensitive fields with marked/ hashed values
- **Write obfuscated file back to S3**: The output file will be written back to S3. The output format is defaulted to have the same format as the input file but could be the other two available formats
- **Exception handling**: Manages errors, e.g. unsupported file formats or missing fields
- **Automatic PII detection**: It can automatically detect PII fields using either a heuristic model or GPT-based detection. This option is designed to assist users, though they can also manually input the fields to obfuscate if preferred.

## Installation
- 1. Clone the repository
```bash
git clone https://github.com/EricaC2401/GDPR-Obfuscator-Project.git
cd GDPR-Obfuscator-Project
```
- 2. Create and active a virtrul environment (optional but recommended):
``` bash
python -m venv venv
source venv/bin/activate # On macOS/ Linux
venv\Scripts\activate # On Windows
```

## Dependencies
- Python 3.11.1
- boto3
- pandas

The required dependencies are listed in 'requirement.txt'. Install them using
`pip install -r requirement.txt`

## Usage
To obfuscate a file stored in S3, please provide an input JSON string containing:
- `"file_to_obfuscate"`: the S3 location of the required CSV/ JSON/ PARQUET file for obfuscation
- `"pii_fields"`: the names of the fields that are required to be obfuscated; If there are no pii_fields, please input an empty list [] instead of `None` or omitting it.

For example:
```json
{
    "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
    "pii_fields": ["name", "email_address"]
}
```

Example of the target file:
```csv
student_id,name,course,cohort,graduation_date,email_address
...
1234,'John Smith','Software','2024-03-31','j.smith@email.com'
...
```

The output might be like:
```csv
student_id,name,course,cohort,graduation_date,email_address
...
1234,'***','Software','2024-03-31','***'
...
```

## Function: handle_file_obfuscation

This function processes the file obfuscation and provides options for different formats and automatic pII detection

**Parameters**:

- json_string (str): JSON string containing “file_to_obfuscate” and “pii_fields”.
- if_output_different_format (bool): If True, outputs in a different format (CSV, JSON, Parquet).
- output_format (str): If if_output_different_format is True, specify the output format, use if if_output_different_format is True.
- chunk_size (int): Number of rows to process at a time (default is 5000).
- if_save_to_s3 (bool): If True, saves the obfuscated file to S3 (default is True).
- auto_detect_pii (bool): If True, automatically detects PII fields using a heuristic model.
- auto_detect_pii_gpt (bool): If True, detects PII fields using GPT-based detection.


## PII Detection (Optional GPT API Integration)

This tool includes an **optional** feature to detect PII fields using the heuristic method or GPT API.
However, this is only a **tool** to assist with detection, and its accuracy is not guranteed.

**Optional GPT API Integration**
- The tool does **not** include an API key. To enable GPT-based PII detection, users need to provide their **own API key**.
- **Any API usage fees** incurred are the responsibility of the user.

To enable GPT-based detection, export your OpenAI API key before running the tool:
```bash
export OPENAI_API_KEY="your_api_key"
```

## File Structure
- `main.py`: The entry point for processing file obfuscation, where the function `handle_file_obfuscation` is located.
- `obfuscator.py`: Contains the logic for obfuscating the file.
- `pii_detection.py`: Heuristic model for detecting PII fields.
- `pii_detection_ai.py`: GPT-based model for detecting PII fields.
- `utils.py`: Utility functions for reading and writing files to S3.
- `settup_logger.py`: Helper function for setting logger

## Testing/ Running Checks
Run test using pytest:
```bash
pytest test/test_main.py
```

or to run all unit tests, use the following command:

```bash
make unit-test
```

To run unit tests along with security checks, formatting checks, and coverage reports, use:
```bash
make run-checks
```


## Continuous Integration & Deployment (CI/CD)
This project uses **GitHub Actions** for automated testing and checks.