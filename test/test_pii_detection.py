import pytest
from src.pii_detection import is_pii_by_heuristic, detect_if_pii


class TestIsPIIByHeruistic:
    @pytest.mark.it('Test if return True for ' +
                    'cols with pii terms and not in non-pii terms')
    def test_pii_terms_not_in_nonpii(self):
        assert is_pii_by_heuristic("email") is True
        assert is_pii_by_heuristic("email_address") is True

    @pytest.mark.it("Test if return False " + "for cols with non-PII terms")
    def test_nonpii_terms(self):
        assert is_pii_by_heuristic("course") is False
        assert is_pii_by_heuristic("course_name") is False

    @pytest.mark.it("Test if col matching pii patterns")
    def test_pii_pattern(self):
        assert is_pii_by_heuristic("ni") is True
        assert is_pii_by_heuristic("nino") is False
        assert is_pii_by_heuristic("account number") is True
        assert is_pii_by_heuristic("account_number") is True

    @pytest.mark.it("Test if others")
    def test_others(self):
        assert is_pii_by_heuristic("nonsense") is False


class TestDetectIfPII:
    @pytest.mark.it("Test if the correct bool is returned ")
    def test_correct_bool(self):
        assert detect_if_pii("cvv") is True
        assert detect_if_pii("email address") is True
        assert detect_if_pii("total_amount") is False
        assert detect_if_pii("nonsense") is False
