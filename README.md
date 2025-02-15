# GDPR Obfuscator

## Project Overview
This project provides a pipeline to read files from an AWS S3 bucket, obfuscate specified personally identifiable information (PII) fields, and then write the obfuscated file back to S3. It currently supports CSV file format.

## Features
- **Read file from s3**: Support CSV file format.
- **Obfuscate PII fields**: Replace specified sensitive fields with marked value '***'
- **Write obfuscated file back to S3**: The output file will have the same format as the input file and will be written back to S3. 
- **Exception handling**: Manages errors, e.g. unsupported file formats or missing fields

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
- `"file_to_obfuscate"`: the S3 location of the required CSV file for obfuscation
- `"pii_fields"`: the names of the fields that are required to be obfuscated

For example:
```json
{
    "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
    "pii_fields": ["name", "email_address"]
}
```

## Testing
Run test using pytest