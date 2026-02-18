"""Playwright browser automation for Google Maps data extraction."""

import re
import time
import logging
from typing import List, Optional
from urllib.parse import quote

from playwright.sync_api import sync_playwright, Page

from scraper.models import Business

logger = logging.getLogger(__name__)

PAGE_LOAD_TIMEOUT = 30000  # ms
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def extract_place_details(google_maps_url: str) -> dict:
    """
    Visit a Google Maps detail page and extract phone, website, and total reviews.

    Launches its own browser instance so it's safe to call from multiple threads.
    Returns dict with keys: total_reviews (int|None), phone (str), website (str).
    """
    result = {"total_reviews": None, "phone": "", "website": "", "address": ""}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=USER_AGENT,
            )
            page = context.new_page()
            page.goto(google_maps_url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")

            # Wait for the detail panel to render
            try:
                page.wait_for_selector('[data-item-id^="phone:tel"], a[data-item-id="authority"], [data-item-id="address"]', timeout=10000)
            except Exception:
                logger.debug(f"Detail panel selectors not found for {google_maps_url}")

            # Allow extra time for dynamic content
            time.sleep(2)

            # Total reviews — limited view may not show count
            try:
                # Try aria-label on stars span (e.g. "4.4 stars 1,234 reviews")
                loc = page.locator('span.ceNzKf[aria-label]')
                if loc.count() > 0:
                    aria = loc.first.get_attribute("aria-label") or ""
                    if "review" in aria.lower():
                        parsed = _parse_review_count(aria.split("star")[1] if "star" in aria else aria)
                        if parsed:
                            result["total_reviews"] = parsed
                # Fallback: button with review count text
                if result["total_reviews"] is None:
                    for sel in ['button[jsaction*="review"]', 'span[aria-label*="reviews"]']:
                        loc = page.locator(sel)
                        if loc.count() > 0:
                            text = loc.first.inner_text().strip()
                            if text:
                                parsed = _parse_review_count(text)
                                if parsed is not None:
                                    result["total_reviews"] = parsed
                                    break
            except Exception:
                pass

            # Phone
            try:
                for sel in ['[data-item-id^="phone:tel"]', 'a[data-item-id^="phone:"]']:
                    loc = page.locator(sel)
                    if loc.count() > 0:
                        # The aria-label often has "Phone: +966 54 910 0210"
                        aria = loc.first.get_attribute("aria-label") or ""
                        if aria:
                            phone = re.sub(r"^[Pp]hone:\s*", "", aria).strip()
                            if phone:
                                result["phone"] = phone
                                break
                        # Fallback: inner text
                        text = loc.first.inner_text().strip()
                        if text:
                            result["phone"] = text
                            break
            except Exception:
                pass

            # Website
            try:
                loc = page.locator('a[data-item-id="authority"]')
                if loc.count() > 0:
                    href = loc.first.get_attribute("href") or ""
                    if href:
                        result["website"] = href
            except Exception:
                pass

            # Address
            try:
                loc = page.locator('[data-item-id="address"]')
                if loc.count() > 0:
                    aria = loc.first.get_attribute("aria-label") or ""
                    if aria:
                        address = re.sub(r"^[Aa]ddress:\s*", "", aria).strip()
                        if address:
                            result["address"] = address
            except Exception:
                pass

            context.close()
        finally:
            browser.close()

    return result


def search_and_extract(
    lat: float,
    lng: float,
    category: str,
    zoom: int,
    max_results: int = 50,
    on_extract: Optional[callable] = None,
) -> List[Business]:
    """
    Launch a browser, search Google Maps at the given coordinates, and
    extract business listings.

    Each call creates its own browser instance so it's safe to call from
    multiple threads concurrently.
    """
    encoded_query = quote(category)
    search_url = (
        f"https://www.google.com/maps/search/{encoded_query}/@{lat},{lng},{zoom}z?hl=en"
    )
    logger.info(f"Searching: {search_url}")

    businesses: List[Business] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=USER_AGENT,
            )
            page = context.new_page()

            # Navigate to search URL
            page.goto(search_url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")

            # Wait for results
            try:
                page.wait_for_selector('a[href*="/maps/place/"]', timeout=15000)
                logger.info("Results loaded")
            except Exception:
                logger.warning("Could not find results on page")
                context.close()
                return []

            # Scroll to load more results
            _scroll_results_panel(page, max_results)

            # Extract directly from search result cards (no page visits)
            businesses = _extract_from_cards(page, category, max_results)
            if not businesses:
                logger.warning("No businesses extracted from cards")
                context.close()
                return []

            for idx, biz in enumerate(businesses):
                if on_extract:
                    on_extract(biz)
                logger.info(f"Extracted ({idx+1}/{len(businesses)}): {biz.name}")

            context.close()
        finally:
            browser.close()

    logger.info(f"Extracted {len(businesses)} businesses from this search point")
    return businesses


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_from_cards(page: Page, category: str, max_results: int) -> List[Business]:
    """Extract business data directly from search result cards without visiting detail pages."""
    businesses: List[Business] = []

    # Find the <a> links to get URLs, but read data from parent containers
    links = page.locator('div[role="feed"] a[href*="/maps/place/"]').all()
    if not links:
        links = page.locator('a[href*="/maps/place/"]').all()

    seen_urls: set[str] = set()

    for link in links[:max_results]:
        try:
            href = link.get_attribute("href") or ""
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)

            # The parent container (typically a div wrapping the whole card) holds all the text
            card = link.locator("..").first

            # Name: aria-label on the <a> tag, or .qBF1Pd inside the card
            name = link.get_attribute("aria-label") or ""
            if not name:
                try:
                    name_loc = card.locator(".qBF1Pd")
                    if name_loc.count() > 0:
                        name = name_loc.first.inner_text().strip()
                except Exception:
                    pass
            if not name:
                continue

            biz = Business(name=name, category=category, google_maps_url=href)

            # Rating: span.MW4etd
            try:
                rating_loc = card.locator("span.MW4etd")
                if rating_loc.count() > 0:
                    rating_text = rating_loc.first.inner_text().strip()
                    if rating_text:
                        biz.rating = _parse_rating(rating_text)
            except Exception:
                pass

            # Review count: span.UY7F9 contains "(456)" or aria-label fallback
            try:
                review_loc = card.locator("span.UY7F9")
                if review_loc.count() > 0:
                    review_text = review_loc.first.inner_text().strip()
                    if review_text:
                        biz.total_reviews = _parse_review_count(review_text)
                if biz.total_reviews is None:
                    # Fallback: aria-label like "4.3 stars 456 Reviews"
                    review_loc2 = card.locator('span[role="img"][aria-label*="Review"]')
                    if review_loc2.count() > 0:
                        review_label = review_loc2.first.get_attribute("aria-label") or ""
                        if review_label:
                            biz.total_reviews = _parse_review_count(review_label)
            except Exception:
                pass

            # Phone: span.UsdlK contains the phone number
            try:
                phone_loc = card.locator("span.UsdlK")
                if phone_loc.count() > 0:
                    biz.phone = phone_loc.first.inner_text().strip()
            except Exception:
                pass

            # Address, hours, and other info from card body text
            # Each .W4Efsd div may contain multi-line text with category, address, hours
            # Split on newlines and classify each line independently
            try:
                info_loc = card.locator(".W4Efsd")
                for i in range(info_loc.count()):
                    block = info_loc.nth(i).inner_text().strip()
                    if not block:
                        continue
                    for line in block.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        # Hours status — check FIRST since hours lines contain digits too
                        if re.search(r"\b(?:Open|Closed|Opens|Closes)\b", line, re.IGNORECASE):
                            if not biz.opening_hours:
                                biz.opening_hours = line
                            continue
                        # Skip lines that are just the phone (already extracted)
                        if biz.phone and line == biz.phone:
                            continue
                        # Address: contains a digit or plus-code pattern, reasonably long
                        if not biz.address and len(line) > 3:
                            if re.search(r"\d", line) or re.match(r"^[A-Z0-9]{4}\+", line):
                                # Strip "Category · Price · " or "Category · " prefix
                                addr = re.sub(r"^(?:[^·]+·\s*)+", "", line).strip()
                                if not addr or len(addr) < 3:
                                    addr = line.strip()
                                biz.address = addr
            except Exception:
                pass

            # Clean phone number from opening_hours if it leaked in
            if biz.phone and biz.opening_hours and biz.phone in biz.opening_hours:
                biz.opening_hours = biz.opening_hours.replace(biz.phone, "").strip()
                biz.opening_hours = re.sub(r"\s*·\s*$", "", biz.opening_hours).strip()

            # Coordinates and place ID from the href URL
            biz.latitude, biz.longitude = _extract_coords(href)
            biz.place_id = _extract_place_id(href) or ""

            businesses.append(biz)

        except Exception as e:
            logger.debug(f"Error extracting card: {e}")
            continue

    logger.info(f"Extracted {len(businesses)} businesses from result cards")
    return businesses


def _scroll_results_panel(page: Page, max_results: int) -> None:
    """Scroll the results feed to load more listings."""
    scroll_selectors = [
        'div[role="feed"]',
        "div.m6QErb",
        'div[aria-label*="Results"]',
    ]

    scrollable = None
    for sel in scroll_selectors:
        try:
            elem = page.locator(sel).first
            if elem.is_visible():
                scrollable = elem
                break
        except Exception:
            continue

    if scrollable is None:
        logger.warning("Could not find scrollable container")
        return

    current = len(page.locator('a[href*="/maps/place/"]').all())
    if current >= max_results:
        logger.info(f"Already have {current} listings, skipping scroll")
        return

    for _ in range(10):
        scrollable.evaluate("el => el.scrollTop = el.scrollHeight")
        time.sleep(2)
        current = len(page.locator('a[href*="/maps/place/"]').all())
        logger.info(f"Loaded {current} listings so far...")
        if current >= max_results:
            break


# ---------------------------------------------------------------------------
# Parsing helpers (self-contained, no external dependency)
# ---------------------------------------------------------------------------


def _parse_rating(text: str) -> Optional[float]:
    m = re.search(r"(\d+[.,]\d+)", text)
    if m:
        return float(m.group(1).replace(",", "."))
    return None


def _parse_review_count(text: str) -> Optional[int]:
    m = re.search(r"([\d,]+)", text.replace(".", ""))
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def _extract_coords(url: str) -> tuple:
    # @lat,lng,zoom pattern
    m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+),\d+z", url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            return lat, lng

    # !3d / !4d pattern
    lat_m = re.search(r"!3d(-?\d+\.\d+)", url)
    lng_m = re.search(r"!4d(-?\d+\.\d+)", url)
    if lat_m and lng_m:
        lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            return lat, lng

    return None, None


def _extract_place_id(url: str) -> Optional[str]:
    # Extract hex ID like 0x3e45c522cc94fbc7:0x9ab76704481dd00f
    m = re.search(r"(0x[0-9a-f]+:0x[0-9a-f]+)", url)
    if m:
        return m.group(1)
    return None
