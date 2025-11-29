"""
2_data_pipeline/data_processing/preprocessing.py

Text normalization utilities used before sentiment & thematic analysis.
This version includes:
    ✓ Unicode normalization
    ✓ URL removal
    ✓ Emoji + symbol stripping
    ✓ Whitespace cleanup
    ✓ Lowercasing
    ✓ Optional aggressive cleaning (toggleable)
"""

import re
import unicodedata

# Regex patterns
URL_PATTERN = re.compile(r"http\S+|www\S+")
EMOJI_PATTERN = re.compile("[\U00010000-\U0010ffff]", flags=re.UNICODE)
MULTISPACE_PATTERN = re.compile(r"\s+")
NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z0-9 .,!?'-]+")


def normalize_text(text: str, aggressive: bool = False) -> str:
    """
    Normalize review text for sentiment + topic modeling.

    Parameters
    ----------
    text : str
        Input text.
    aggressive : bool
        If True, removes more non-alphanumeric symbols.
        Recommended for topic modeling; OFF for sentiment.

    Returns
    -------
    str : cleaned text
    """

    if text is None or (isinstance(text, float) and str(text) == "nan"):
        return ""

    # Convert non-string types
    if not isinstance(text, str):
        text = str(text)

    # Unicode normalization
    text = unicodedata.normalize("NFKC", text)

    # Remove URLs
    text = URL_PATTERN.sub("", text)

    # Remove emojis
    text = EMOJI_PATTERN.sub("", text)

    # Replace line breaks with spaces
    text = text.replace("\n", " ").replace("\r", " ")

    # Aggressive cleaning mode for topic modeling
    if aggressive:
        text = NON_ALPHANUMERIC.sub(" ", text)

    # Normalize punctuation spacing & strip
    text = MULTISPACE_PATTERN.sub(" ", text).strip()

    # Lowercase for consistent processing
    text = text.lower()

    return text
