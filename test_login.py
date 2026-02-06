"""Standalone DTU Findit login test. Run: uv run python test_login.py [--headed]"""

from dotenv import load_dotenv
load_dotenv()

import os
import sys
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

URL = "https://findit.dtu.dk/en/catalog?dtu=student_theses&q=&type=thesis_master"


def click(driver, by, value):
    try:
        el = driver.find_element(by, value)
        if el.is_displayed():
            el.click()
            time.sleep(0.3)
            return True
    except Exception:
        pass
    return False


def main():
    user = os.environ.get("DTU_USERNAME")
    pwd = os.environ.get("DTU_PASSWORD")
    if not user or not pwd:
        print("Set DTU_USERNAME and DTU_PASSWORD in .env")
        return

    opts = Options()
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    if "--headed" not in sys.argv:
        opts.add_argument("--headless=new")
    driver = webdriver.Chrome(options=opts)
    try:
        driver.get(URL)
        print("1. Page loaded...")
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1)

        print("2. Clicking 'I am not a robot'...")
        click(driver, By.LINK_TEXT, "I am not a robot")
        time.sleep(1)

        print("3. Cookie: Allow selected...")
        click(driver, By.CSS_SELECTOR, "button.js-consent-selected")
        time.sleep(0.5)

        print("4. Opening login popover...")
        click(driver, By.CSS_SELECTOR, "a.id-login-button")
        time.sleep(1)
        def post_cas_link(d):
            for a in d.find_elements(By.CSS_SELECTOR, "a[href*='/users/auth/cas']"):
                href = a.get_attribute("href")
                if href:
                    d.execute_script("""
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
                    return True
            return False

        print("5. Submitting DTU CAS login (POST)...")
        post_cas_link(driver)
        time.sleep(3)
        # May land on sign_in/select; click "From DTU" again
        if "sign_in/select" in (driver.current_url or ""):
            print("5b. On sign-in select, clicking From DTU...")
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/users/auth/cas']"))
            )
            post_cas_link(driver)
            time.sleep(5)

        print("6. Filling credentials...")
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
        time.sleep(2)
        # Password may be in main doc or in iframe (login can stay on findit or redirect)
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
                        return True  # stay in frame for form fill
                except Exception:
                    pass
                d.switch_to.default_content()
            return False
        WebDriverWait(driver, 20).until(password_visible)

        for usel in ["#userNameInput", "input[name='UserName']", "input[name='username']"]:
            try:
                un = driver.find_element(By.CSS_SELECTOR, usel)
                pw = driver.find_element(By.CSS_SELECTOR, "#passwordInput, input[name='Password']")
                if un.is_displayed() and pw.is_displayed():
                    driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", un, user)
                    driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", pw, pwd)
                    pw.send_keys(Keys.ENTER)
                    print("7. Submitted. Waiting 3s...")
                    time.sleep(3)
                    break
            except Exception:
                continue
        else:
            print("7. Could not find login form. Check page structure.")

        print("Done.")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
