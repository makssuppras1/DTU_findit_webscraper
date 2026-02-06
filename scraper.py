"""Selenium browser automation: login, pagination, PDF download."""

import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from config import (
    BASE_URL,
    DELAY_BETWEEN_PAGES,
    DOWNLOAD_WAIT,
    PAGE_WAIT,
    PER_PAGE,
    TARGET_URL,
    WAIT_TIMEOUT,
    CHROME_PROFILE_DIR,
    START_RECORD,
)

log = logging.getLogger(__name__)


def get_driver(download_dir: str, worker_id: int | None = None):
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
        "profile.managed_default_content_settings.images": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    if CHROME_PROFILE_DIR:
        profile = CHROME_PROFILE_DIR if worker_id is None else f"{CHROME_PROFILE_DIR}_w{worker_id}"
        opts.add_argument(f"--user-data-dir={os.path.abspath(profile)}")
    return webdriver.Chrome(options=opts)


def _click_if_found(driver, by, value):
    try:
        el = driver.find_element(by, value)
        if el.is_displayed():
            try:
                el.click()
            except Exception:
                driver.execute_script("arguments[0].click();", el)
            time.sleep(0.3)
            return True
    except Exception:
        pass
    return False


def handle_dtu_findit_entry(driver):
    time.sleep(1)
    _click_if_found(driver, By.LINK_TEXT, "I am not a robot")
    time.sleep(1)
    if _click_if_found(driver, By.CSS_SELECTOR, "button.js-consent-selected"):
        log.info("Accepted cookie consent")
    time.sleep(0.5)
    _click_if_found(driver, By.CSS_SELECTOR, "a.id-login-button")
    time.sleep(1)
    for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/users/auth/cas']"):
        href = a.get_attribute("href")
        if href:
            driver.execute_script("""
                var f = document.createElement('form');
                f.method = 'POST';
                f.action = arguments[0];
                var tok = document.querySelector('meta[name="csrf-token"]');
                if (tok) {
                    var inp = document.createElement('input');
                    inp.name = 'authenticity_token';
                    inp.value = tok.content;
                    inp.type = 'hidden';
                    f.appendChild(inp);
                }
                document.body.appendChild(f);
                f.submit();
            """, href)
            log.info("Submitted DTU CAS login")
            break
    time.sleep(3)
    if "sign_in/select" in (driver.current_url or ""):
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/users/auth/cas']"))
        )
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/users/auth/cas']"):
            href = a.get_attribute("href")
            if href:
                driver.execute_script("""
                    var f = document.createElement('form');
                    f.method = 'POST';
                    f.action = arguments[0];
                    var tok = document.querySelector('meta[name="csrf-token"]');
                    if (tok) {
                        var inp = document.createElement('input');
                        inp.name = 'authenticity_token';
                        inp.value = tok.content;
                        inp.type = 'hidden';
                        f.appendChild(inp);
                    }
                    document.body.appendChild(f);
                    f.submit();
                """, href)
                break
        time.sleep(4)
    user = os.environ.get("DTU_USERNAME")
    pwd = os.environ.get("DTU_PASSWORD")
    if not user or not pwd:
        return False
    def password_visible(d):
        try:
            pw = d.find_elements(By.CSS_SELECTOR, "input[type='password']")
            if pw and any(e.is_displayed() for e in pw):
                return True
        except Exception:
            pass
        for frame in d.find_elements(By.TAG_NAME, "iframe"):
            try:
                d.switch_to.frame(frame)
                pw = d.find_elements(By.CSS_SELECTOR, "input[type='password']")
                if pw and any(e.is_displayed() for e in pw):
                    return True
            except Exception:
                pass
            d.switch_to.default_content()
        return False

    try:
        WebDriverWait(driver, 20).until(password_visible)
    except Exception:
        return False
    for usel in ["#userNameInput", "input[name='UserName']", "input[name='username']"]:
        try:
            un = driver.find_element(By.CSS_SELECTOR, usel)
            pw = driver.find_element(By.CSS_SELECTOR, "#passwordInput, input[name='Password']")
            if un.is_displayed() and pw.is_displayed():
                driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', { bubbles: true }));",
                    un, user
                )
                driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', { bubbles: true }));",
                    pw, pwd
                )
                pw.send_keys(Keys.ENTER)
                log.info("Submitted DTU credentials")
                time.sleep(3)
                return True
        except Exception:
            continue
    return False


def extract_detail_urls_from_soup(soup) -> list[str]:
    seen = set()
    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        full = urljoin(BASE_URL, href)
        parsed = urlparse(full)
        if parsed.netloc != "findit.dtu.dk":
            continue
        path = parsed.path.rstrip("/")
        m = re.match(r"^/en/catalog/([^/]+)$", path)
        if m and "?" not in m.group(1) and m.group(1) != "export":
            rid = m.group(1)
            if rid not in seen:
                seen.add(rid)
                out.append(rid)
    return out


def wait_for_results_list(driver):
    def has_result_links(d):
        return bool(re.search(r'/en/catalog/[a-f0-9]{24}(?:"|\?|$)', (d.page_source or ""), re.I))
    WebDriverWait(driver, WAIT_TIMEOUT).until(has_result_links)


def get_url_with_retry(driver, url: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            driver.get(url)
            return
        except WebDriverException as e:
            if "ERR_CONNECTION_RESET" in str(e) or "Connection reset" in str(e):
                wait = 10 * (attempt + 1)
                log.warning("Connection reset on %s, retry %d/%d in %ds", url[:50], attempt + 1, max_retries, wait)
                time.sleep(wait)
            else:
                raise
    raise WebDriverException("Connection reset after retries")


def catalog_page_url(page: int) -> str:
    """Build catalog URL using page parameter (1-based). Site ignores start when navigating directly."""
    sep = "&" if "?" in TARGET_URL else "?"
    return f"{TARGET_URL}{sep}per_page={PER_PAGE}&page={page}"


def find_download_in_element(parent):
    for by, val in [
        (By.CSS_SELECTOR, "a[href*='download-fulltext']"),
        (By.CSS_SELECTOR, "a[href*='fulltext-gateway']"),
        (By.CSS_SELECTOR, "a[href*='fulltext']"),
        (By.CSS_SELECTOR, "a[href*='.pdf']"),
        (By.LINK_TEXT, "Download"),
        (By.PARTIAL_LINK_TEXT, "Download"),
        (By.PARTIAL_LINK_TEXT, "Download fulltext"),
        (By.PARTIAL_LINK_TEXT, "PDF"),
        (By.PARTIAL_LINK_TEXT, "Full text"),
        (By.XPATH, ".//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]"),
        (By.XPATH, ".//a[contains(@class,'download')]"),
        (By.XPATH, ".//a[contains(@class,'fulltext')]"),
    ]:
        try:
            el = parent.find_element(by, val)
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def get_records_with_downloads_on_page(driver, catalog_ids: set[str] | None = None) -> list[tuple[str, str, object]]:
    """Return (record_id, title, btn) for main catalog records that have a download. If catalog_ids given, only those."""
    if catalog_ids is not None:
        main_ids = catalog_ids
    else:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        main_ids = set(extract_detail_urls_from_soup(soup))
    out = []
    seen = set()
    for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='download-fulltext'], a[href*='fulltext']"):
        try:
            href = a.get_attribute("href") or ""
            record_id = None
            if "download-fulltext" in href:
                try:
                    ancestors = driver.execute_script(
                        "var e=arguments[0], out=[]; while(e){e=e.parentElement; if(e) out.push(e);} return out;",
                        a
                    ) or []
                    for anc in ancestors:
                        req = anc.get_attribute("data-request-url") or ""
                        m = re.search(r"document=([a-f0-9]{24})", req)
                        if m:
                            record_id = m.group(1)
                            break
                        rid = anc.get_attribute("data-document-id") or anc.get_attribute("data-id")
                        if rid and re.match(r"^[a-f0-9]{24}$", rid, re.I):
                            record_id = rid
                            break
                except Exception:
                    pass
            if not record_id and "download-fulltext" not in href:
                m = re.search(r"(?:targetid|target_id|id)=([a-f0-9]{24})", href, re.I) or re.search(
                    r"/catalog/([a-f0-9]{24})", href, re.I
                )
                if m:
                    record_id = m.group(1)
            if record_id and len(record_id) == 24 and record_id in main_ids and record_id not in seen:
                seen.add(record_id)
                try:
                    title_el = driver.find_element(By.CSS_SELECTOR, f"a[href*='/catalog/{record_id}']")
                    title = title_el.text.strip() if title_el else ""
                except Exception:
                    title = ""
                out.append((record_id, title, a))
        except Exception:
            pass
    for record_id in main_ids:
        if record_id in seen:
            continue
        try:
            link = driver.find_element(By.CSS_SELECTOR, f"a[href*='/catalog/{record_id}']")
            title = link.text.strip() or ""
            for tag in ("document", "result", "item", "row", "index", "col", "list", "blacklight"):
                try:
                    row = link.find_element(By.XPATH, f"./ancestor::*[contains(@class,'{tag}')][1]")
                    btn = find_download_in_element(row)
                    if btn:
                        seen.add(record_id)
                        out.append((record_id, title, btn))
                        break
                except Exception:
                    continue
        except Exception:
            pass
    return out


def iterate_pages_with_records(driver, allowed_starts: set[int] | None = None):
    all_pages = list(range(1, 2001))  # page 1, 2, 3, ...
    if allowed_starts is not None:
        pages = sorted((s // PER_PAGE) + 1 for s in allowed_starts)
        pages = [p for p in pages if 1 <= p <= 2000]
    else:
        pages = all_pages

    if START_RECORD and START_RECORD > 1:
        start_page = (START_RECORD - 1) // PER_PAGE + 1
        skip_on_first = (START_RECORD - 1) % PER_PAGE
        pages = [p for p in pages if p >= start_page]
        if pages:
            log.info("Starting from record ~%d (page %d, skip first %d on page)", START_RECORD, start_page, skip_on_first)
    else:
        skip_on_first = 0

    seen_ids = set()
    def load_page(url):
        get_url_with_retry(driver, url)
        WebDriverWait(driver, PAGE_WAIT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(3)

    for page in pages:
        url = catalog_page_url(page)
        load_page(url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        records = extract_detail_urls_from_soup(soup)
        if not records:
            log.info("Page %d: empty (possible session expiry), re-login", page)
            handle_dtu_findit_entry(driver)
            load_page(url)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            records = extract_detail_urls_from_soup(soup)
        catalog_ids = set(records)
        page_records = get_records_with_downloads_on_page(driver, catalog_ids)
        if not page_records and records:
            log.info("Page %d: no downloads (possible session expiry), re-login", page)
            handle_dtu_findit_entry(driver)
            load_page(url)
            page_records = get_records_with_downloads_on_page(driver, catalog_ids)
        if not page_records:
            log.warning("Page %d: no records with download found", page)
        for i, (record_id, title, btn) in enumerate(page_records):
            if skip_on_first and page == start_page and i < skip_on_first:
                continue
            if skip_on_first and page == start_page:
                skip_on_first = 0
            if record_id not in seen_ids:
                seen_ids.add(record_id)
                yield record_id, title, btn
        if not records or len(records) < PER_PAGE:
            break
        time.sleep(DELAY_BETWEEN_PAGES)


def wait_for_downloaded_pdf(download_dir: Path, timeout: int = DOWNLOAD_WAIT) -> Path | None:
    poll_interval = 0.2
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pdfs = list(download_dir.glob("*.pdf"))
        crdownload = list(download_dir.glob("*.crdownload"))
        if crdownload:
            time.sleep(poll_interval)
            continue
        if pdfs:
            return pdfs[0]
        time.sleep(poll_interval)
    log.warning("PDF download timed out after %ds", timeout)
    return None


def click_download_and_wait(driver, download_btn, download_dir: Path, main_handle: str, results_url: str = "") -> Path | None:
    for f in download_dir.glob("*"):
        f.unlink(missing_ok=True)
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", download_btn)
    except Exception:
        pass
    time.sleep(0.1)
    handles_before = set(driver.window_handles)
    try:
        download_btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", download_btn)
    time.sleep(1)
    handles_after = set(driver.window_handles)
    new_handles = handles_after - handles_before
    if new_handles:
        driver.switch_to.window(new_handles.pop())
        pdf_path = wait_for_downloaded_pdf(download_dir)
        try:
            driver.close()
        except NoSuchWindowException:
            pass  # tab may have closed itself after download
        if main_handle in driver.window_handles:
            driver.switch_to.window(main_handle)
        elif driver.window_handles:
            driver.switch_to.window(driver.window_handles[0])
    else:
        pdf_path = wait_for_downloaded_pdf(download_dir)
        if results_url and driver.current_url != results_url:
            driver.get(results_url)
            time.sleep(0.5)
    return pdf_path
