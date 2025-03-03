import re
from src.setup_logger import setup_logger


logger = setup_logger(__name__)

pii_dict = {
    "student_id": False,
    "name": True,
    "email_address": True,
    "phone_number": True,
    "address": True,
    "amount": False,
    "course": False,
    "cohort": False,
    "graduation_date": False,
    "birth_date": True,
    "passport_number": True,
    "credit_card_number": True,
    "cvv": True,
    "salary": False,
    "tax_id": True,
    "national_insurance": True,
    "ni_number": True,
    "employee_id": False,
    "department": False,
    "join_date": False,
    "last_login": False,
    "user_email": True,
    "address_line_1": True,
    "address_line_2": True,
    "city": False,
    "country": False,
    "postcode": True,
    "billing_address": True,
    "shipping_address": True,
    "first_name": True,
    "last_name": True,
    "middle_name": True,
    "gender": False,
    "marital_status": False,
    "education_level": False,
    "course_code": False,
    "enrollment_number": False,
    "bank_account_number": True,
    "iban": True,
    "routing_number": True,
    "device_id": True,
    "imei_number": True,
    "device_type": False,
    "device_os": False,
    "mac_address": True,
    "ip_address": True,
    "session_id": True,
    "login_time": False,
    "logout_time": False,
    "user_agent": True,
    "authentication_token": True,
    "subscription_id": False,
    "subscription_start_date": False,
    "subscription_end_date": False,
    "order_id": False,
    "product_id": False,
    "quantity": False,
    "total_amount": False,
    "shipping_method": False,
    "payment_method": False,
    "order_status": False,
    "customer_feedback": False,
    "support_ticket": False,
    "service_rating": False,
    "product_review": False,
    "payment_status": False,
    "transaction_id": False,
    "product_name": False,
    "shipping_status": False,
    "order_date": False,
    "payment_date": False,
    "customer_id": False,
    "customer_name": True,
}

non_pii_terms = ["course", "product", "item",
                 "company", "department", "category"]

ppi_terms = ["email", "phone", "contact", "name", "address",
             "dob", "birth", "passport"]

ppi_patterns = [r"\bni\b",
                r"(?=.*account)(?=.*number)",
                r"(?=.*credit)(?=.*card)"]


def is_pii_by_heuristic(column_name: str) -> bool:
    """
    Check if a column name is PII based on predefined partterns and exclusion

    Args:
        column_name (str): The column_name want to detect if ppi

    Returns:
        bool: True if detected is pii, False otherwise
    """
    logger.debug(f"Checking if column '{column_name}' " +
                 "is PII using heuristic method.")
    if any(term in column_name.lower() for term in ppi_terms):
        if any(term in column_name.lower() for term in non_pii_terms):
            return False
        return True
    else:
        for pattern in ppi_patterns:
            if re.search(pattern, column_name, re.IGNORECASE):
                logger.debug(f"Column '{column_name}' " +
                             "matches pattern: {pattern}.")
                return True
    logger.debug(f"Column '{column_name}' is not " +
                 "detected as PII by heuristic.")
    return False


def detect_if_pii(column_name: str) -> bool:
    """
    First check the dictionary, then apply heuristic if unknown

    Args:
        column_name (str): The column_name want to detect if ppi

    Returns:
        bool: True if detected is pii, False otherwise
    """
    logger.debug(f"Detecting if column '{column_name}' is PII.")
    if " " in column_name:
        column_name = column_name.replace(" ", "_")
        logger.debug(f"Replaced spaces with underscores: {column_name}")
    if column_name in pii_dict:
        logger.debug(
            f"Column '{column_name}' found in PII "
            + "dictionary with result: {pii_dict[column_name]}"
        )
        return pii_dict[column_name]
    else:
        logger.debug(
            f"Column '{column_name}' not found in PII dictionary."
            + " Applying heuristic."
        )
        return is_pii_by_heuristic(column_name)
