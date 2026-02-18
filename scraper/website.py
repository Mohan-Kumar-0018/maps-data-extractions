"""Extract contact info (emails, phones, social links) from business websites."""

import logging
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_TIMEOUT = 10
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_PHONE_RE = re.compile(
    r"(?:\+?\d{1,3}[\s.-]?)?"        # optional country code
    r"(?:\(?\d{2,4}\)?[\s.-]?)?"      # optional area code
    r"\d{3,4}[\s.-]?\d{3,4}"          # main number
)
_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".ico"}
_JUNK_EMAILS = {"example.com", "sentry.io", "wixpress.com", "googleapis.com"}
_CONTACT_SLUGS = {"contact", "about", "about-us", "contact-us", "contactus", "aboutus"}
_SOCIAL_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "tiktok.com",
}


def _fetch_html(url: str) -> str | None:
    """GET a URL and return its HTML text, or None on failure."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        if "text/html" not in resp.headers.get("Content-Type", ""):
            return None
        return resp.text
    except Exception:
        return None


def _extract_emails(text: str) -> set[str]:
    """Find email addresses in raw text, filtering junk."""
    emails = set()
    for match in _EMAIL_RE.findall(text):
        email = match.lower()
        ext = email[email.rfind("."):]
        if ext in _IMAGE_EXTS:
            continue
        domain = email.split("@", 1)[1]
        if domain in _JUNK_EMAILS:
            continue
        emails.add(email)
    return emails


def _extract_phones(text: str) -> set[str]:
    """Find phone numbers in raw text."""
    phones = set()
    for match in _PHONE_RE.findall(text):
        cleaned = re.sub(r"[^\d+]", "", match)
        if len(cleaned) >= 7:
            phones.add(match.strip())
    return phones


def _extract_social_links(soup: BeautifulSoup) -> set[str]:
    """Find social media profile URLs from <a> tags."""
    social = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        try:
            parsed = urlparse(href)
        except Exception:
            continue
        domain = parsed.netloc.lower().lstrip("www.")
        if domain in _SOCIAL_DOMAINS and parsed.scheme in ("http", "https"):
            social.add(href)
    return social


def _find_contact_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Find internal links to contact/about pages (max 2)."""
    base_parsed = urlparse(base_url)
    links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        # Only follow internal links
        if parsed.netloc and parsed.netloc != base_parsed.netloc:
            continue

        path_lower = parsed.path.lower().rstrip("/")
        slug = path_lower.rsplit("/", 1)[-1]
        if slug in _CONTACT_SLUGS and full_url not in seen:
            seen.add(full_url)
            links.append(full_url)
            if len(links) >= 2:
                break

    return links


def extract_website_contacts(url: str) -> dict:
    """Crawl a website (homepage + up to 2 contact/about pages) and extract contacts.

    Returns {"emails": [...], "phones": [...], "social_media": [...]}.
    """
    all_emails: set[str] = set()
    all_phones: set[str] = set()
    all_social: set[str] = set()

    # Ensure URL has a scheme
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Fetch homepage
    html = _fetch_html(url)
    if not html:
        return {"emails": [], "phones": [], "social_media": []}

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(separator=" ")

    all_emails |= _extract_emails(page_text)
    all_phones |= _extract_phones(page_text)
    all_social |= _extract_social_links(soup)

    # Find and crawl contact/about pages
    contact_links = _find_contact_links(soup, url)
    for link in contact_links:
        sub_html = _fetch_html(link)
        if not sub_html:
            continue
        sub_soup = BeautifulSoup(sub_html, "html.parser")
        sub_text = sub_soup.get_text(separator=" ")

        all_emails |= _extract_emails(sub_text)
        all_phones |= _extract_phones(sub_text)
        all_social |= _extract_social_links(sub_soup)

    return {
        "emails": sorted(all_emails),
        "phones": sorted(all_phones),
        "social_media": sorted(all_social),
    }
