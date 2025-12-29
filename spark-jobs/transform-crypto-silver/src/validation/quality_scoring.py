from typing import List, Optional


class QualityScorer:
    """Calculate data quality scores."""

    ERROR_WEIGHT = 0.2  # Each error deducts 0.2 from max score
    MAX_SCORE = 1.0
    MIN_SCORE = 0.0

    @staticmethod
    def calculate(validation_errors: Optional[List[str]]) -> float:
        """Calculate data quality score (0.0-1.0)."""
        if not validation_errors or len(validation_errors) == 0:
            return QualityScorer.MAX_SCORE

        error_count = len(validation_errors)
        score = max(
            QualityScorer.MIN_SCORE,
            QualityScorer.MAX_SCORE
            - (error_count * QualityScorer.ERROR_WEIGHT),
        )
        return round(score, 2)

