"""
HireLens – Indeed Scraper
Scrapes job postings from Indeed public search pages. No API key required.
"""
 
import hashlib
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Generator, List, Optional
from urllib.parse import urljoin
 
import httpx
from bs4 import BeautifulSoup
from loguru import logger
 
INDEED_BASE = "https://www.indeed.com"
 
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]
 
DEFAULT_QUERIES = [
    "data engineer", "data scientist", "machine learning engineer",
    "data analyst", "analytics engineer", "MLOps engineer",
    "AI engineer", "business intelligence analyst",
]
 
DEFAULT_LOCATIONS = ["United States", "Remote"]
 
 
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
 
 
def _random_delay(min_s=2.0, max_s=5.0):
    time.sleep(random.uniform(min_s, max_s))
 
 
def _make_job_id(title, company, url):
    return hashlib.md5(f"{title}|{company}|{url}".encode()).hexdigest()
 
 
def _parse_salary(text):
    if not text:
        return None, None
    numbers = [float(n.replace(",", "")) for n in re.findall(r"\d[\d,]*", text)]
    if not numbers:
        return None, None
    if "hour" in text.lower():
        numbers = [n * 2080 for n in numbers]
    return (min(numbers), max(numbers)) if len(numbers) > 1 else (numbers[0], numbers[0])
 
 
def _parse_posted_date(text):
    if not text:
        return None
    text = text.lower()
    now = datetime.utcnow()
    if "today" in text or "just posted" in text:
        return now
    if "yesterday" in text:
        return now - timedelta(days=1)
    m = re.search(r"(\d+)\s+day", text)
    if m:
        return now - timedelta(days=int(m.group(1)))
    return None
 
 
class IndeedScraper:
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
 
    def _headers(self):
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.indeed.com/",
        }
 
    def _get(self, url, params=None):
        for attempt in range(1, self.max_retries + 1):
            try:
                _random_delay(self.delay_min, self.delay_max)
                r = self._client.get(url, params=params, headers=self._headers())
                r.raise_for_status()
                return BeautifulSoup(r.text, "html.parser")
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP {e.response.status_code} attempt {attempt}: {url}")
                if e.response.status_code == 429:
                    time.sleep(30 * attempt)
            except Exception as e:
                logger.warning(f"Request error attempt {attempt}: {e}")
        logger.error(f"Failed after {self.max_retries} attempts: {url}")
        return None
 
    def _parse_job_cards(self, soup):
        jobs = []
        cards = soup.find_all("div", {"class": re.compile(r"job_seen_beacon|jobsearch-SerpJobCard|tapItem")})
        if not cards:
            cards = soup.find_all("li", {"class": re.compile(r"css-.*JobCard")})
        for card in cards:
            try:
                title_el = card.find(["h2", "a"], {"class": re.compile(r"jobTitle|title")})
                title = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    continue
                company_el = card.find(["span", "a"], {"class": re.compile(r"companyName|company")})
                company = company_el.get_text(strip=True) if company_el else "Unknown"
                loc_el = card.find(["div", "span"], {"class": re.compile(r"companyLocation|location")})
                location_text = loc_el.get_text(strip=True) if loc_el else ""
                is_remote = "remote" in location_text.lower()
                salary_el = card.find(["div", "span"], {"class": re.compile(r"salary|salaryText")})
                salary_raw = salary_el.get_text(strip=True) if salary_el else ""
                sal_min, sal_max = _parse_salary(salary_raw)
                link_el = card.find("a", href=True)
                href = link_el["href"] if link_el else ""
                url = urljoin(INDEED_BASE, href) if href else ""
                jk_match = re.search(r"jk=([a-f0-9]+)", url)
                external_id = jk_match.group(1) if jk_match else _make_job_id(title, company, url)
                date_el = card.find(["span", "div"], {"class": re.compile(r"date|posted")})
                posted_date = _parse_posted_date(date_el.get_text(strip=True) if date_el else "")
                jobs.append({
                    "external_id": f"indeed_{external_id}", "title": title, "company": company,
                    "location": location_text, "is_remote": is_remote, "salary_raw": salary_raw,
                    "salary_min": sal_min, "salary_max": sal_max, "url": url, "posted_date": posted_date,
                })
            except Exception as e:
                logger.debug(f"Card parse error: {e}")
        return jobs
 
    def _fetch_description(self, job_url):
        if not job_url:
            return ""
        soup = self._get(job_url)
        if not soup:
            return ""
        desc_el = soup.find("div", {"id": "jobDescriptionText"}) or \
                  soup.find("div", {"class": re.compile(r"jobsearch-jobDescriptionText|description")})
        return desc_el.get_text(separator="\n", strip=True) if desc_el else ""
 
    def search(self, query, location="United States", max_jobs=100, fetch_descriptions=True):
        logger.info(f"Searching Indeed: '{query}' in '{location}' (max {max_jobs})")
        scraped = 0
        start = 0
        while scraped < max_jobs:
            params = {"q": query, "l": location, "start": start, "fromage": 30, "sort": "date"}
            soup = self._get(INDEED_BASE + "/jobs", params)
            if not soup:
                break
            cards = self._parse_job_cards(soup)
            if not cards:
                break
            for card_data in cards:
                if scraped >= max_jobs:
                    break
                description = self._fetch_description(card_data["url"]) if fetch_descriptions and card_data["url"] else ""
                yield RawJob(source="indeed", description_raw=description, **card_data)
                scraped += 1
            start += 15
        logger.info(f"Done: {scraped} jobs scraped for '{query}'")
 
    def scrape_all_roles(self, queries=None, locations=None, max_per_query=50):
        queries = queries or DEFAULT_QUERIES
        locations = locations or DEFAULT_LOCATIONS
        seen = set()
        for query in queries:
            for location in locations:
                for job in self.search(query, location, max_jobs=max_per_query):
                    if job.external_id not in seen:
                        seen.add(job.external_id)
                        yield job
 
    def close(self):
        self._client.close()
 
    def __enter__(self):
        return self
 
    def __exit__(self, *_):
        self.close()
 
