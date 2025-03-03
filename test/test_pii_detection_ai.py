import pytest
import openai
from unittest.mock import patch, MagicMock
import os
try:
    os.environ["OPENAI_API_KEY"] = "test_api_key"
except KeyError:
    pass
from src.pii_detection_ai import detect_if_pii_with_gpt


class TestDetectIfPiiWithGpt:
    @pytest.mark.it("Test if the output type are correct")
    @patch("src.pii_detection_ai.client.chat.completions.create")
    def test_correct_output_type(self, mock_openai):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="""
                        [
                            {"column_name": "email", "score": 1.0,
                            "reason": "Contains personal email addresses"},
                            {"column_name": "name", "score": 0.9,
                            "reason": "Usually contains full names"},
                            {"column_name": "course", "score": 0.0,
                            "reason": "Unlikely to be PII"}
                        ]
                        """
                )
            )
        ]

        mock_openai.return_value = mock_response

        test_column_names = ["email", "name", "course"]

        result = detect_if_pii_with_gpt(test_column_names)

        assert isinstance(result, list)

    @pytest.mark.it("Test if the output content are correct")
    @patch("src.pii_detection_ai.client.chat.completions.create")
    def test_correct_output_content(self, mock_openai):
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(
                message=MagicMock(
                    content="""
                        [
                            {"column_name": "email", "score": 1.0,
                            "reason": "Contains personal email addresses"},
                            {"column_name": "name", "score": 0.9,
                            "reason": "Usually contains full names"},
                            {"column_name": "course", "score": 0.0,
                            "reason": "Unlikely to be PII"}
                        ]
                        """
                )
            )
        ]

        mock_openai.return_value = mock_response

        test_column_names = ["email", "name", "course"]

        result = detect_if_pii_with_gpt(test_column_names)

        assert len(result) == 3
        assert result[0]["column_name"] == "email"
        assert result[0]["score"] == 1.0
        assert result[0]["reason"] == "Contains personal email addresses"

    @pytest.mark.it("Test if OpenAIError is raised")
    @patch("src.pii_detection_ai.client.chat.completions.create")
    def test_if_with_gpt_error(self, mock_opanai):
        mock_opanai.side_effect = openai.OpenAIError("API connection failed")

        test_column_names = ["name", "email"]

        with pytest.raises(openai.OpenAIError):
            detect_if_pii_with_gpt(test_column_names)
