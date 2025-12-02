from __future__ import annotations

import re
import warnings
from typing import Final

from bs4 import BeautifulSoup, FeatureNotFound, XMLParsedAsHTMLWarning

XML_HINT_RE: Final[re.Pattern[str]] = re.compile(r"<(rss|feed|kml|svg|sitemap)\b", re.IGNORECASE)


def make_soup(markup: str, *, default_parser: str = "html.parser") -> BeautifulSoup:
    """
    Return a BeautifulSoup instance choosing an XML parser when the markup looks XML-ish.
    Falls back to the default parser if an XML parser is unavailable and suppresses the
    warning that BeautifulSoup would otherwise emit when XML gets parsed as HTML.
    """
    candidate = markup.lstrip()
    prefer_xml = candidate.startswith("<?xml") or XML_HINT_RE.search(candidate[:512]) is not None
    if prefer_xml:
        try:
            return BeautifulSoup(markup, features="xml")
        except FeatureNotFound:
            pass

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        return BeautifulSoup(markup, features=default_parser)
