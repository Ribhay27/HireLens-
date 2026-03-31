"""
HireLens – Indeed Scraper
Scrapes job postings from Indeed's public search pages.

No API key required. Uses rotating user-agents + polite delays to avoid blocks.
Targets data/tech roles by default.
"""

import hashlib
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Generator, List, Optional
from urllib.parse import urlencode, urljoin

import httpx
from bs4 import BeautifulSoup
from loguru import logger

# ── Config ─────────────────────────────────────────────────────────────────────

INDEED_BASE = "https://www.indeed.com"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

DEFAULT_QUERIES = [
    "data engineer",
    "data scientist",
    "machine learning engineer",
    "data analyst",
    "analytics engineer",
    "MLOps engineer",
    "AI engineer",
    "business intelligence analyst",
]

DEFAULT_LOCATIONS = ["United States", "Remote"]


# ── Data Classes ───────────────────────────────────────────────────────────────

@dataclass
class RawJob:
    external_id: str
    source: str
    title: str
    company: str
    location: str
    is_remote: bool
    salary_raw: str
    salary_min: Optional[float]
    salary_max: Optional[float]
    description_raw: str
    url: str
    posted_date: Optional[datetime]
    scraped_at: datetime = field(default_factory=datetime.utcnow)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _random_delay(min_s: float = 2.0, max_s: float = 5.0):
    time.sleep(random.uniform(min_s, max_s))


def _make_job_id(title: str, company: str, url: str) -> str:
    raw = f"{title}|{company}|{url}"
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_salary(salary_text: str):
    """Extract min/max floats from a salary string like '$80,000 - $120,000 a year'."""
    if not salary_text:
        return None, None
    numbers = re.findall(r"\d[\d,]*", salary_text)
    numbers = [float(n.replace(",", "")) for n in numbers]
    if not numbers:
        return None, None
    # Convert hourly to annual estimate
    if "hour" in salary_text.lower() and numbers:
        numbers = [n * 2080 for n in numbers]
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    return min(numbers), max(numbers)


def _parse_posted_date(text: str) -> Optional[datetime]:
    """Parse relative date like 'Posted 3 days ago' into a datetime."""
    if not text:
        return None
    text = text.lower()
    now = datetime.utcnow()
    if "today" in text or "just posted" in text:
        return now
    if "yesterday" in text:
        return now - timedelta(days=1)
    match = re.search(r"(\d+)\s+day", text)
    if match:
        return now - timedelta(days=int(match.group(1)))
    match = re.search(r"(\d+)\s+hour", text)
    if match:
        return now - timedelta(hours=int(match.group(1)))
    return None


# ── Scraper Class ──────────────────────────────────────────────────────────────

class IndeedScraper:
    """
    Scrapes job listings from Indeed's public search results.

    Usage:
        scraper = IndeedScraper()
        for job in scraper.search("data engineer", location="United States", max_jobs=50):
            print(job.title, job.company)
    """

    def __init__(
        self,
        delay_min: float = float(os.getenv("SCRAPE_DELAY_MIN", 2)),
        delay_max: float = float(os.getenv("SCRAPE_DELAY_MAX", 5)),
        max_retries: int = 3,
    ):
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.max_retries = max_retries
        self._client = httpx.Client(follow_redirects=True, timeout=15)

    # ── Private ────────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.indeed.com/",
        }

    def _get(self, url: str, params: dict = None) -> Optional[BeautifulSoup]:
        for attempt in range(1, self.max_retries + 1):
            try:
                _random_delay(self.delay_min, self.delay_max)
                response = self._client.get(url, params=params, headers=self._headers())
                response.raise_for_status()
                return BeautifulSoup(response.text, "html.parser")
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP {e.response.status_code} on attempt {attempt}: {url}")
                if e.response.status_code == 429:
                    time.sleep(30 * attempt)   # back off harder on rate limit
            except Exception as e:
                logger.warning(f"Request error attempt {attempt}: {e}")
            if attempt == self.max_retries:
                logger.error(f"Failed after {self.max_retries} attempts: {url}")
        return None

    def _build_search_url(self, query: str, location: str, start: int = 0) -> tuple:
        params = {
            "q": query,
            "l": location,
            "start": start,
            "fromage": 30,    # jobs posted in last 30 days
            "sort": "date",
        }
        return f"{INDEED_BASE}/jobs", params

    def _parse_job_cards(self, soup: BeautifulSoup) -> List[dict]:
        """Extract job card data from a search results page."""
        jobs = []
        # Indeed uses multiple possible card selectors across redesigns
        cards = soup.find_all("div", {"class": re.compile(r"job_seen_beacon|jobsearch-SerpJobCard|tapItem")})
        if not cards:
            cards = soup.find_all("li", {"class": re.compile(r"css-.*JobCard")})

        for card in cards:
            try:
                # Title
                title_el = card.find(["h2", "a"], {"class": re.compile(r"jobTitle|title")})
                title = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    continue

                # Company
                company_el = card.find(["span", "a"], {"class": re.compile(r"companyName|company")})
                company = company_el.get_text(strip=True) if company_el else "Unknown"

                # Location
                loc_el = card.find(["div", "span"], {"class": re.compile(r"companyLocation|location")})
                location_text = loc_el.get_text(strip=True) if loc_el else ""
                is_remote = "remote" in location_text.lower()

                # Salary
                salary_el = card.find(["div", "span"], {"class": re.compile(r"salary|salaryText")})
                salary_raw = salary_el.get_text(strip=True) if salary_el else ""
                sal_min, sal_max = _parse_salary(salary_raw)

                # URL / ID
                link_el = card.find("a", href=True)
                href = link_el["href"] if link_el else ""
                url = urljoin(INDEED_BASE, href) if href else ""
                # Extract jk param as external_id
                jk_match = re.search(r"jk=([a-f0-9]+)", url)
                external_id = jk_match.group(1) if jk_match else _make_job_id(title, company, url)

                # Posted date
                date_el = card.find(["span", "div"], {"class": re.compile(r"date|posted")})
                posted_date = _parse_posted_date(date_el.get_text(strip=True) if date_el else "")

                jobs.append({
                    "external_id": f"indeed_{external_id}",
                    "title": title,
                    "company": company,
                    "location": location_text,
                    "is_remote": is_remote,
                    "salary_raw": salary_raw,
                    "salary_min": sal_min,
                    "salary_max": sal_max,
                    "url": url,
                    "posted_date": posted_date,
                })
            except Exception as e:
                logger.debug(f"Error parsing card: {e}")
                continue

        return jobs

    def _fetch_description(self, job_url: str) -> str:
        """Fetch the full job description from a job's detail page."""
        if not job_url:
            return ""
        soup = self._get(job_url)
        if not soup:
            return ""
        desc_el = soup.find("div", {"id": "jobDescriptionText"}) or \
                  soup.find("div", {"class": re.compile(r"jobsearch-jobDescriptionText|description")})
        return desc_el.get_text(separator="\n", strip=True) if desc_el else ""

    def _get_total_results(self, soup: BeautifulSoup) -> int:
        """Parse total result count from search page."""
        count_el = soup.find(["div", "span"], {"class": re.compile(r"jobCount|searchCount|resultCount")})
        if count_el:
            match = re.search(r"[\d,]+", count_el.get_text())
            if match:
                return int(match.group().replace(",", ""))
        return 0

    # ── Public ─────────────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        location: str = "United States",
        max_jobs: int = 100,
        fetch_descriptions: bool = True,
    ) -> Generator[RawJob, None, None]:
        """
        Yield RawJob objects for the given search query + location.

        Args:
            query: Job search term (e.g. "data engineer")
            location: Location string (e.g. "United States" or "Remote")
            max_jobs: Maximum number of jobs to scrape
            fetch_descriptions: Whether to fetch full job descriptions (slower)

        Yields:
            RawJob dataclass instances
        """
        logger.info(f"Searching Indeed: '{query}' in '{location}' (max {max_jobs})")
        scraped = 0
        start = 0
        page_size = 15  # Indeed typically returns 15 per page

        while scraped < max_jobs:
            url, params = self._build_search_url(query, location, start)
            soup = self._get(url, params)
            if not soup:
                logger.warning("Empty response — stopping search.")
                break

            cards = self._parse_job_cards(soup)
            if not cards:
                logger.info("No more job cards found.")
                break

            for card_data in cards:
                if scraped >= max_jobs:
                    break

                description = ""
                if fetch_descriptions and card_data["url"]:
                    description = self._fetch_description(card_data["url"])

                yield RawJob(
                    source="indeed",
                    description_raw=description,
                    **{k: v for k, v in card_data.items()},
                )
                scraped += 1
                logger.debug(f"  [{scraped}/{max_jobs}] {card_data['title']} @ {card_data['company']}")

            start += page_size

        logger.info(f"Indeed search complete: {scraped} jobs scraped for '{query}'")

    def scrape_all_roles(
        self,
        queries: List[str] = None,
        locations: List[str] = None,
        max_per_query: int = 50,
    ) -> Generator[RawJob, None, None]:
        """
        Run searches across all default role queries and locations.

        Args:
            queries: List of search terms; defaults to DEFAULT_QUERIES
            locations: List of location strings; defaults to DEFAULT_LOCATIONS
            max_per_query: Max jobs per (query, location) combination

        Yields:
            RawJob instances
        """
        queries = queries or DEFAULT_QUERIES
        locations = locations or DEFAULT_LOCATIONS
        seen_ids = set()

        for query in queries:
            for location in locations:
                for job in self.search(query, location, max_jobs=max_per_query):
                    if job.external_id not in seen_ids:
                        seen_ids.add(job.external_id)
                        yield job

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
