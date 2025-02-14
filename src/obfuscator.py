import pandas as pd
import io


def obfuscate_file(
        file_content: str, fields_list: list, file_type: str = 'csv',
        chunk_size: int = 5000
        ) -> io.BytesIO:
    '''
    Obfuscate the specified field in the file content

    Args:
        file_content (str): raw data as a string
        fields_list (list): fields to be obfuscated
        file_type (str): file type (e.g. csv) in the output byte system
        chunk_size (int): number of rows to process at a time, 5000 by default

    Returns:
        io.BytesIO: Obfuscated file (as specified in file_type,
        csv by default) as a byte system
    '''
    try:
        output = io.BytesIO()
        if file_type == 'csv':
            chunk_iter = pd.read_csv(io.StringIO(file_content),
                                     chunksize=chunk_size)
            for chunk in chunk_iter:
                for field in fields_list:
                    if field in chunk.columns:
                        chunk[field] = '***'
                    else:
                        raise KeyError(f"Field '{field}' not" +
                                       "found in the data.")
                chunk.to_csv(output, index=False, header=output.tell() == 0)
        else:
            raise ValueError(f"Sorry that {file_type} is not supported.")
    except KeyError as ke:
        raise KeyError(str(ke))
    except ValueError as ve:
        raise ValueError(f"Error reading the file: {str(ve)}")
    except Exception as e:
        raise Exception(f"Unexpected error: {str(e)}")

    output.seek(0)
    return output
