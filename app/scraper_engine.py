"""
Scraper Engine — core logic adapted from scrape_v2.py for web service use.
"""

import asyncio
import aiohttp
import csv
import re
import json
import time
from html import unescape
from urllib.parse import urlparse
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════════
#  CONFIG (same as scrape_v2.py)
# ═══════════════════════════════════════════════════════════════════

SEED_PATHS = [
    "/", "/about", "/about-us", "/team", "/our-team", "/leadership",
    "/contact", "/contact-us", "/company", "/people", "/staff",
]

CRAWL_KEYWORDS = {
    "leadership": 3, "team": 3, "management": 3, "executives": 3,
    "founders": 3, "owner": 3, "officers": 3, "directors": 3,
    "our-team": 3, "our-leadership": 3, "meet-the-team": 3,
    "meet-the": 3, "bios": 3,
    "about": 2, "about-us": 2, "who-we-are": 2, "our-story": 2,
    "company": 2, "people": 2, "staff": 2, "contact": 2,
    "contact-us": 2, "careers": 2,
    "press": 1, "news": 1, "media": 1, "investors": 1,
    "partners": 1, "portfolio": 1,
}

MAX_DISCOVERED_PAGES = 10
MAX_RETRIES = 2
RETRY_BACKOFF_BASE = 2

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
LINKEDIN_PERSONAL_RE = re.compile(r'https?://(?:www\.)?linkedin\.com/in/([a-zA-Z0-9\-_%]+)/?', re.I)
LINKEDIN_COMPANY_RE = re.compile(r'https?://(?:www\.)?linkedin\.com/company/([a-zA-Z0-9\-_%]+)/?', re.I)

NOISE_PREFIXES = {
    "noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon",
    "postmaster", "abuse", "webmaster", "example", "test", "user", "email",
    "your", "name", "username", "sentry", "wix", "wordpress", "jquery",
    "bootstrap", "cloudflare", "google", "facebook", "twitter", "github",
    "root", "daemon", "nobody", "null", "bounce", "unsubscribe",
}

NOISE_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "wordpress.org", "jquery.com",
    "w3.org", "schema.org", "googleapis.com", "google.com", "facebook.com",
    "twitter.com", "github.com", "cloudflare.com", "gravatar.com", "wp.com",
    "yoursite.com", "yourdomain.com", "domain.com", "email.com", "test.com",
    "placeholder.com", "company.com", "acme.com", "sampledomain.com",
}

GENERIC_PREFIXES = {
    "info", "contact", "hello", "admin", "office", "general", "support",
    "sales", "service", "customerservice", "help", "enquiries", "inquiries",
    "mail", "team", "feedback", "hr", "careers", "jobs", "press", "media",
    "leasing", "management", "billing", "accounting", "reception",
    "reservations", "events", "catering", "marketing", "operations",
}

SKIP_EXTENSIONS = re.compile(
    r'\.(png|jpg|jpeg|gif|svg|css|js|webp|ico|pdf|zip|woff|woff2|ttf|eot|'
    r'mp4|mp3|avi|mov|xml|json|rss|atom|txt|map|webmanifest)$', re.I
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

TITLE_KEYWORDS = (
    r'CEO|CFO|COO|CTO|CMO|CIO|CRO|'
    r'Chief\s+\w+\s+Officer|'
    r'(?:Co-?)?Founder(?:\s*(?:&|and)\s*CEO)?|'
    r'(?:Co-?)?Owner|Partner|Managing Partner|Principal|'
    r'President|Vice\s*President|[ES]VP|'
    r'(?:Managing|Executive|Regional|Area|District)?\s*Director(?:\s+of\s+\w+(?:\s+\w+){0,3})?|'
    r'General\s+Manager|GM|'
    r'(?:Regional|Area|District|Operations|Property|Portfolio|Marketing|Sales|Business\s+Development)\s+Manager'
)

NAME_PATTERN = r'([A-Z][a-z]{1,20}(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]{1,20}(?:-[A-Z][a-z]{1,20})?)'
NAME_THEN_TITLE = re.compile(NAME_PATTERN + r'\s*[,\-–—|]\s*(' + TITLE_KEYWORDS + r')', re.I | re.MULTILINE)
TITLE_THEN_NAME = re.compile(r'(' + TITLE_KEYWORDS + r')\s*[:\-–—|]\s*' + NAME_PATTERN, re.I | re.MULTILINE)

EMAIL_FORMATS = [
    "{first}.{last}", "{first}{last}", "{f}{last}", "{first}_{last}",
    "{first}", "{last}.{first}", "{f}.{last}", "{first}{l}", "{f}{l}",
]

NOT_NAMES = {
    "privacy policy", "terms conditions", "read more", "learn more", "click here",
    "view all", "see more", "contact us", "about us", "sign up", "log in",
    "get started", "join us", "our team", "main menu", "all rights", "quick links",
    "new york", "los angeles", "san francisco", "san diego", "las vegas",
    "fort worth", "kansas city", "salt lake",
}

NAME_BLACKLIST_WORDS = {
    'click', 'page', 'menu', 'link', 'view', 'rights', 'reserved', 'copyright',
    'policy', 'terms', 'cookie', 'location', 'service', 'powered', 'download',
    'subscribe', 'newsletter', 'sitemap', 'search', 'login', 'register',
}

TITLE_SCORES = {
    'ceo': 100, 'chief executive officer': 100,
    'founder': 95, 'co-founder': 95, 'owner': 95, 'co-owner': 95,
    'president': 90,
    'coo': 85, 'chief operating officer': 85,
    'cfo': 80, 'chief financial officer': 80,
    'cto': 75, 'cmo': 75, 'cro': 75,
    'managing partner': 85, 'partner': 80, 'principal': 80,
    'evp': 70, 'svp': 70, 'vice president': 65, 'vp': 65,
    'managing director': 70, 'executive director': 70,
    'regional director': 60, 'director': 55,
    'general manager': 50, 'gm': 50,
    'regional manager': 45, 'operations manager': 40,
    'property manager': 40, 'marketing manager': 35, 'sales manager': 35,
}


# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════

def normalize_domain(raw):
    d = raw.strip().strip('"').strip("'").lower()
    d = re.sub(r'^https?://', '', d)
    d = re.sub(r'^www\.', '', d)
    d = re.sub(r'/.*$', '', d)
    return d


def clean_email(email):
    lower = email.lower().strip(".")
    prefix, _, domain = lower.partition("@")
    if not domain: return None
    if domain in NOISE_DOMAINS: return None
    if prefix in NOISE_PREFIXES: return None
    if SKIP_EXTENSIONS.search(lower): return None
    if len(prefix) > 40 or len(domain) > 60: return None
    if re.match(r'^[0-9]+$', prefix): return None
    if re.search(r'[0-9a-f]{16,}', prefix): return None
    if '..' in lower: return None
    return lower


def classify_email(email):
    prefix = email.split("@")[0]
    return "generic" if prefix in GENERIC_PREFIXES else "personal"


def clean_name(name):
    if not name: return None
    name = name.strip()
    if len(name) < 4 or len(name) > 45: return None
    if name.lower() in NOT_NAMES: return None
    parts = name.split()
    if len(parts) < 2 or len(parts) > 4: return None
    for p in parts:
        cleaned = p.replace(".", "").replace("-", "").replace("'", "")
        if not cleaned or not cleaned[0].isupper(): return None
        if not re.match(r'^[A-Za-z\.\-\']+$', p): return None
    lower = name.lower()
    if any(w in lower for w in NAME_BLACKLIST_WORDS): return None
    return name


def strip_html(html):
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.I)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.I)
    text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text


def discover_urls(html, domain):
    urls_scored = {}
    for match in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
        raw_url = match.group(1).strip()
        if raw_url.startswith("/"):
            full_url = f"https://{domain}{raw_url}"
        elif raw_url.startswith("http"):
            full_url = raw_url
        else:
            continue
        parsed = urlparse(full_url)
        url_domain = parsed.netloc.lower().replace("www.", "")
        if url_domain != domain: continue
        path = parsed.path.rstrip("/").lower()
        if not path or path == "/" or len(path) > 100: continue
        if SKIP_EXTENSIONS.search(path): continue
        score = 0
        path_parts = path.replace("/", " ").replace("-", " ").replace("_", " ").split()
        for keyword, pts in CRAWL_KEYWORDS.items():
            kw_parts = keyword.replace("-", " ").split()
            if all(kp in path_parts for kp in kw_parts):
                score += pts
        if score > 0:
            clean_url = f"https://{domain}{parsed.path.rstrip('/')}"
            if clean_url not in urls_scored or urls_scored[clean_url] < score:
                urls_scored[clean_url] = score
    sorted_urls = sorted(urls_scored.items(), key=lambda x: -x[1])
    return [url for url, _ in sorted_urls[:MAX_DISCOVERED_PAGES]]


def extract_structured_people(html):
    people = []
    for block in re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.I):
        try:
            data = json.loads(block)
            items = data if isinstance(data, list) else [data]
            for item in items:
                _walk_jsonld(item, people)
        except (json.JSONDecodeError, TypeError):
            pass
    # vCard
    for block in re.findall(r'class=["\'][^"\']*vcard[^"\']*["\'].*?(?=class=["\'][^"\']*vcard|$)', html, re.DOTALL | re.I):
        fn = re.search(r'class=["\'][^"\']*fn[^"\']*["\'][^>]*>([^<]+)', block, re.I)
        title = re.search(r'class=["\'][^"\']*title[^"\']*["\'][^>]*>([^<]+)', block, re.I)
        email = re.search(r'href=["\']mailto:([^"\'?]+)["\']', block, re.I)
        if fn:
            name = clean_name(fn.group(1).strip())
            if name:
                people.append({"name": name, "title": title.group(1).strip() if title else "", "email": clean_email(email.group(1)) if email else "", "source": "vcard"})
    return people


def _walk_jsonld(item, people):
    if not isinstance(item, dict): return
    t = item.get("@type", "")
    types = t if isinstance(t, list) else [t]
    if "Person" in types:
        name = item.get("name", "")
        if name:
            people.append({"name": name, "title": item.get("jobTitle", ""), "email": item.get("email", ""), "source": "jsonld"})
    for key in ("employee", "member", "founder", "alumni", "author"):
        val = item.get(key)
        if isinstance(val, dict): _walk_jsonld(val, people)
        elif isinstance(val, list):
            for v in val: _walk_jsonld(v, people)
    for g in item.get("@graph", []):
        _walk_jsonld(g, people)


def extract_people_from_text(text):
    people = []
    seen = set()
    for m in NAME_THEN_TITLE.finditer(text):
        name = clean_name(m.group(1))
        if name and name.lower() not in seen:
            seen.add(name.lower())
            people.append({"name": name, "title": m.group(2).strip()})
    for m in TITLE_THEN_NAME.finditer(text):
        name = clean_name(m.group(2))
        if name and name.lower() not in seen:
            seen.add(name.lower())
            people.append({"name": name, "title": m.group(1).strip()})
    return people


def extract_linkedin_urls(html):
    personal, company = set(), set()
    for m in LINKEDIN_PERSONAL_RE.finditer(html):
        slug = m.group(1).lower()
        if slug not in ('login', 'signup', 'share', 'pulse', 'feed'):
            personal.add(f"https://linkedin.com/in/{slug}")
    for m in LINKEDIN_COMPANY_RE.finditer(html):
        slug = m.group(1).lower()
        company.add(f"https://linkedin.com/company/{slug}")
    return personal, company


def extract_mailto(html):
    emails = set()
    for m in re.finditer(r'href=["\']mailto:([^"\'?]+)', html, re.I):
        cleaned = clean_email(m.group(1))
        if cleaned: emails.add(cleaned)
    return emails


def match_email_format(known_emails, names, domain):
    target_emails = {e for e in known_emails if e.split("@")[1] == domain}
    if not target_emails: return None
    format_hits = defaultdict(int)
    for name in names:
        parts = name.split()
        if len(parts) < 2: continue
        first, last = parts[0].lower(), parts[-1].lower()
        f, l = first[0], last[0]
        for fmt in EMAIL_FORMATS:
            try:
                candidate = fmt.format(first=first, last=last, f=f, l=l) + f"@{domain}"
                if candidate in target_emails:
                    format_hits[fmt] += 1
            except (KeyError, IndexError):
                continue
    return max(format_hits, key=format_hits.get) if format_hits else None


def score_lead(email, email_type, name, title, verified, has_linkedin):
    score = 0
    if title:
        title_lower = title.lower().strip()
        for key, pts in TITLE_SCORES.items():
            if key in title_lower:
                score = max(score, pts)
                break
        if score == 0: score = 20
    if email and email_type == "personal": score += 15
    elif email and email_type == "generic": score += 5
    if name: score += 10
    if verified == "valid": score += 10
    elif verified == "invalid": score -= 30
    elif verified == "catch_all": score += 3
    if has_linkedin: score += 5
    return min(max(score, 0), 100)


# ═══════════════════════════════════════════════════════════════════
#  FETCH WITH RETRY
# ═══════════════════════════════════════════════════════════════════

async def fetch_with_retry(session, url, timeout, max_retries=MAX_RETRIES):
    for attempt in range(max_retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                   allow_redirects=True, ssl=False, max_redirects=5) as resp:
                if resp.status == 200:
                    ct = resp.headers.get("Content-Type", "")
                    if "text/html" in ct or "text/plain" in ct:
                        text = await resp.text(errors='ignore')
                        if len(text) > 100:
                            return text
                elif resp.status in (429, 500, 502, 503, 504) and attempt < max_retries:
                    await asyncio.sleep(RETRY_BACKOFF_BASE ** (attempt + 1))
                    continue
                else:
                    return None
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < max_retries:
                await asyncio.sleep(RETRY_BACKOFF_BASE ** (attempt + 1))
                continue
        except Exception:
            return None
    return None


# ═══════════════════════════════════════════════════════════════════
#  DOMAIN SCRAPER
# ═══════════════════════════════════════════════════════════════════

async def scrape_domain(session, domain, timeout, semaphore):
    result = {
        "emails": {}, "people": [], "linkedin_personal": set(),
        "linkedin_company": set(), "pages_scraped": 0, "inferred_emails": {},
    }
    seen_names = set()
    all_html_pages = []

    async with semaphore:
        # Phase 1: Seed pages
        seed_htmls = {}
        for path in SEED_PATHS:
            for scheme in ["https", "http"]:
                html = await fetch_with_retry(session, f"{scheme}://{domain}{path}", timeout)
                if html:
                    seed_htmls[path] = html
                    all_html_pages.append((path, html))
                    result["pages_scraped"] += 1
                    break

        # Phase 2: Crawl discovery
        homepage = seed_htmls.get("/", "")
        if homepage:
            fetched_paths = set(seed_htmls.keys())
            for url in discover_urls(homepage, domain):
                path = urlparse(url).path.rstrip("/") or "/"
                if path in fetched_paths: continue
                html = await fetch_with_retry(session, url, timeout)
                if html:
                    all_html_pages.append((path, html))
                    result["pages_scraped"] += 1
                    fetched_paths.add(path)

        # Phase 3: Extract from all pages
        for path, html in all_html_pages:
            for e in EMAIL_REGEX.findall(html):
                cleaned = clean_email(e)
                if cleaned and cleaned not in result["emails"]:
                    result["emails"][cleaned] = {"type": classify_email(cleaned), "source": path}
            for e in extract_mailto(html):
                if e not in result["emails"]:
                    result["emails"][e] = {"type": classify_email(e), "source": path + " (mailto)"}
            personal_li, company_li = extract_linkedin_urls(html)
            result["linkedin_personal"].update(personal_li)
            result["linkedin_company"].update(company_li)
            for p in extract_structured_people(html):
                name = clean_name(p["name"]) if p["name"] else None
                if name and name.lower() not in seen_names:
                    seen_names.add(name.lower())
                    result["people"].append({"name": name, "title": p.get("title", ""), "email": p.get("email", ""), "source": path})
            text = strip_html(html)
            for p in extract_people_from_text(text):
                if p["name"].lower() not in seen_names:
                    seen_names.add(p["name"].lower())
                    result["people"].append({"name": p["name"], "title": p["title"], "email": "", "source": path})

        # Phase 4: Email pattern inference
        known_personal = [e for e, info in result["emails"].items()
                          if info["type"] == "personal" and e.split("@")[1] == domain]
        known_names = [p["name"] for p in result["people"]]
        if known_personal and known_names:
            fmt = match_email_format(known_personal, known_names, domain)
            if fmt:
                for person in result["people"]:
                    if person["email"]: continue
                    parts = person["name"].split()
                    if len(parts) >= 2:
                        first, last = parts[0].lower(), parts[-1].lower()
                        try:
                            inferred = fmt.format(first=first, last=last, f=first[0], l=last[0]) + f"@{domain}"
                            if inferred not in result["emails"]:
                                result["inferred_emails"][inferred] = {"person": person["name"], "format": fmt}
                        except (KeyError, IndexError):
                            pass

    return domain, result


# ═══════════════════════════════════════════════════════════════════
#  CSV LOADING
# ═══════════════════════════════════════════════════════════════════

def load_domains_from_file(filepath, column_name=None):
    domains = set()
    domain_meta = {}
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        if not headers:
            raise ValueError("Empty CSV file.")

        col = None
        if column_name:
            for h in headers:
                if h.strip().lower() == column_name.strip().lower():
                    col = h
                    break
            if not col:
                raise ValueError(f"Column '{column_name}' not found. Available: {headers}")
        else:
            for h in headers:
                if re.search(r'domain|website|url|site', h, re.I):
                    col = h
                    break
            if not col:
                raise ValueError(f"Couldn't auto-detect domain column. Available: {headers}")

        for row in reader:
            d = normalize_domain(row.get(col, ""))
            if d and "." in d and d not in domains:
                domains.add(d)
                domain_meta[d] = {
                    "company_name": row.get("Company_Name", row.get("companyName", "")),
                    "category": row.get("Source_File", row.get("Category", "")),
                }

    return sorted(domains), domain_meta


# ═══════════════════════════════════════════════════════════════════
#  MAIN JOB RUNNER
# ═══════════════════════════════════════════════════════════════════

async def run_scraper_job(domains, domain_meta, output_path, workers, timeout,
                          enable_dork, enable_verify, progress_callback=None):
    semaphore = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers * 2, ttl_dns_cache=300, enable_cleanup_closed=True)

    total = len(domains)
    completed = 0
    total_emails = 0
    total_people = 0
    total_inferred = 0
    domains_with_data = 0

    all_results = []

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        batch_size = workers * 4
        for batch_start in range(0, total, batch_size):
            batch = domains[batch_start:batch_start + batch_size]
            tasks = [scrape_domain(session, d, timeout, semaphore) for d in batch]

            for coro in asyncio.as_completed(tasks):
                domain, result = await coro
                completed += 1
                all_results.append((domain, result))

                has_data = bool(result["emails"] or result["people"]
                               or result["linkedin_personal"] or result["inferred_emails"])
                if has_data:
                    domains_with_data += 1
                total_emails += len(result["emails"])
                total_people += len(result["people"])
                total_inferred += len(result["inferred_emails"])

                if progress_callback:
                    progress_callback(completed, total_emails, total_people,
                                      total_inferred, domains_with_data)

    # Write results with scoring
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "Score", "Domain", "Company_Name", "Category",
            "Email", "Email_Type", "Email_Source", "Inferred",
            "Verified", "Person_Name", "Person_Title", "Person_Source",
            "LinkedIn_Personal", "LinkedIn_Company", "Pages_Scraped",
        ])

        rows = []
        for domain, result in all_results:
            meta = domain_meta.get(domain, {})
            company = meta.get("company_name", "")
            category = meta.get("category", "")
            li_personal = sorted(result["linkedin_personal"])
            li_company = sorted(result["linkedin_company"])
            pages = result["pages_scraped"]

            email_list = list(result["emails"].items())
            inferred_list = list(result["inferred_emails"].items())
            people = result["people"]
            domain_rows = []
            used_people = set()

            for email, info in email_list:
                matched_person, matched_title, matched_psource = "", "", ""
                for i, p in enumerate(people):
                    if p.get("email") == email and i not in used_people:
                        matched_person, matched_title, matched_psource = p["name"], p["title"], p["source"]
                        used_people.add(i)
                        break
                score = score_lead(email, info["type"], matched_person, matched_title, "", bool(li_personal))
                domain_rows.append([
                    score, domain, company, category,
                    email, info["type"], info["source"], "no", "",
                    matched_person, matched_title, matched_psource,
                    "; ".join(li_personal) if not domain_rows else "",
                    "; ".join(li_company) if not domain_rows else "",
                    pages if not domain_rows else "",
                ])

            for email, info in inferred_list:
                person_name = info["person"]
                title = ""
                for p in people:
                    if p["name"] == person_name: title = p["title"]; break
                score = score_lead(email, "personal", person_name, title, "", bool(li_personal))
                domain_rows.append([
                    score, domain, company, category,
                    email, "personal", f"inferred ({info['format']})", "yes", "",
                    person_name, title, "",
                    "; ".join(li_personal) if not domain_rows else "",
                    "; ".join(li_company) if not domain_rows else "",
                    pages if not domain_rows else "",
                ])

            for i, p in enumerate(people):
                if i in used_people: continue
                if any(inf["person"] == p["name"] for inf in result["inferred_emails"].values()): continue
                score = score_lead("", "", p["name"], p["title"], "", bool(li_personal))
                domain_rows.append([
                    score, domain, company, category,
                    "", "", "", "", "",
                    p["name"], p["title"], p["source"],
                    "; ".join(li_personal) if not domain_rows else "",
                    "; ".join(li_company) if not domain_rows else "",
                    pages if not domain_rows else "",
                ])

            if not domain_rows:
                domain_rows.append([
                    0, domain, company, category,
                    "", "", "", "", "", "", "", "",
                    "; ".join(li_personal), "; ".join(li_company), pages,
                ])

            rows.extend(domain_rows)

        rows.sort(key=lambda r: -r[0])
        for row in rows:
            writer.writerow(row)
