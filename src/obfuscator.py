import pandas as pd
import io


def obfuscate_file(
        file_content: str, fields_list: list, file_type: str = 'csv'
        ) -> io.BytesIO:
    '''
    Obfuscate the specified field in the file content

    Args:
        file_content (str): raw data as a string
        fields_list (list): fields to be obfuscated
        file_type (str): file type (e.g. csv) in the output byte system

    Output:
        io.BytesIO: Obfuscated file (as specified in file_type,
        csv by default) as a byte system
    '''
    df_step = pd.read_csv(io.StringIO(file_content))

    for field in fields_list:
        if field in df_step.columns:
            df_step[field] = '***'

    output = io.BytesIO()
    if file_type == 'csv':
        df_step.to_csv(output, index=False)
    output.seek(0)

    return output
