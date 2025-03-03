import json
import openai
from openai import OpenAI
from src.setup_logger import setup_logger

client = OpenAI()


logger = setup_logger(__name__)


def detect_if_pii_with_gpt(column_names: list[str]) -> list[dict[str, any]]:
    """
    Identify how likely the input column names contains PII using ChatGPT

    Args:
        column_name (list(str)): The column names to detect if ppi

    Returns:
        list[dict[str,Any]]: A list of dict where each dict contains:
        - 'column_name' (str): The name of column
        - 'score' (float): A likelihood score ranging from 0.0 (definitely
                           not PII) to 1.0 (definitely PII)
        - 'reason' (str): A brief explaination for the assigned score
    """
    logger.debug("Starting PII detection with GPT " +
                 f"for columns: {column_names}")

    formatted_columns = "\n".join([f"- {col}" for col in column_names])
    prompt = f"""
                Act as a data privacy expert.
                Given the list of column name below, classify how likely
                they contains Personally Identifiable Information (PII).

                Return a valid JSON array where each object contains:
                - 'column_name' (str): the name of the column
                - 'score' (float): A likelihood score from 0.0 (not PII)
                                   to 1.0 (definitely PII)
                - 'reason' (string): A short reason
                                     explaining your classification.

                Column: {formatted_columns}

                Example output:

                [{{'column_name':'email', 'score': 1.0, 'reason': 'xxx'}},,,]
            """
    try:
        logger.info("Sending request to GPT for PII detection.")
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=1000,
        )
        logger.info("Received response from GPT.")
        result_str = completion.choices[0].message.content
        logger.debug(f"Raw response from GPT: {result_str}")
        result_str = result_str.replace("'", '"')
        result = json.loads(result_str)
        logger.info("PII detection completed successfully.")
        return result
    except json.JSONDecodeError:
        logger.error("Invalid JSON input: Unable to decode JSON")
        raise
    except openai.OpenAIError as oe:
        logger.error(f"OpenAI API error: {str(oe)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        raise
