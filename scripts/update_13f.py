#!/usr/bin/env python3
"""Fetch the latest two 13F-HR reports for each tracked investor from SEC EDGAR,
compare them, map CUSIPs to tickers (OpenFIGI), and write data/13f.json.

Run:  python3 scripts/update_13f.py
Env:  SEC_USER_AGENT  (required by SEC, e.g. "whale-watch you@example.com")
      OPENFIGI_API_KEY (optional, makes ticker mapping faster)
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
UA = os.environ.get("SEC_USER_AGENT", "whale-watch personal-research contact@example.com")
FIGI_KEY = os.environ.get("OPENFIGI_API_KEY", "")

_last = [0.0]


def sec_get(url, as_json=False, tries=4):
    for k in range(tries):
        wait = 0.15 - (time.time() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
            with urllib.request.urlopen(req, timeout=60) as r:
                body = r.read()
            return json.loads(body) if as_json else body
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            time.sleep(2 * (k + 1))
        except Exception:
            time.sleep(2 * (k + 1))
    raise RuntimeError(f"failed: {url}")


def strip_ns(root):
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return root


def txt(el, path, default=""):
    f = el.find(path)
    return f.text.strip() if f is not None and f.text else default


def filing_docs(cik, acc):
    base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/"
    idx = sec_get(base + "index.json", as_json=True)
    names = [i["name"] for i in idx["directory"]["item"]]
    return base, names


def amendment_type(cik, acc):
    base, names = filing_docs(cik, acc)
    prim = next((n for n in names if n.lower() == "primary_doc.xml"), None)
    if not prim:
        return "RESTATEMENT"
    root = strip_ns(ET.fromstring(sec_get(base + prim)))
    return (txt(root, ".//amendmentType") or "RESTATEMENT").upper()


def info_table(cik, acc):
    """Return {(cusip, putCall): {issuer, cls, sh, val}} for one filing."""
    base, names = filing_docs(cik, acc)
    xmls = [n for n in names if n.lower().endswith(".xml") and n.lower() != "primary_doc.xml"]
    rows = {}
    for n in xmls:
        try:
            root = strip_ns(ET.fromstring(sec_get(base + n)))
        except ET.ParseError:
            continue
        tables = root.findall(".//infoTable")
        if not tables:
            continue
        for it in tables:
            cusip = txt(it, "cusip").upper()
            pc = txt(it, "putCall").upper()[:1]  # "", "P", "C"
            typ = txt(it, "shrsOrPrnAmt/sshPrnamtType").upper()
            if typ and typ != "SH":
                continue  # skip bonds / principal amounts
            try:
                sh = int(float(txt(it, "shrsOrPrnAmt/sshPrnamt", "0")))
                val = int(float(txt(it, "value", "0")))
            except ValueError:
                continue
            key = (cusip, pc)
            r = rows.setdefault(key, {"issuer": txt(it, "nameOfIssuer"), "cls": txt(it, "titleOfClass"), "sh": 0, "val": 0})
            r["sh"] += sh
            r["val"] += val
        break
    return rows


def periods_for(cik):
    """Return list of (period, {issuer...rows}, filed_date) newest first (max 2)."""
    sub = sec_get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json", as_json=True)
    r = sub["filings"]["recent"]
    by_period = {}
    for i, form in enumerate(r["form"]):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        by_period.setdefault(r["reportDate"][i], []).append(
            {"form": form, "acc": r["accessionNumber"][i], "filed": r["filingDate"][i]})
    out = []
    for period in sorted(by_period, reverse=True)[:2]:
        fs = sorted(by_period[period], key=lambda f: f["filed"])
        base = None
        extra = []
        for f in fs:
            if f["form"] == "13F-HR":
                base = f
                extra = []
            else:
                t = amendment_type(cik, f["acc"])
                if t.startswith("RESTATE"):
                    base = f
                    extra = []
                else:
                    extra.append(f)
        if base is None:
            continue
        rows = info_table(cik, base["acc"])
        for f in extra:  # "NEW HOLDINGS" amendments add positions
            for k, v in info_table(cik, f["acc"]).items():
                if k in rows:
                    rows[k]["sh"] += v["sh"]
                    rows[k]["val"] += v["val"]
                else:
                    rows[k] = v
        # Some filers still report value in $1000s (pre-2023 convention). Detect via implied price.
        px = sorted(v["val"] / v["sh"] for (c, pc), v in rows.items() if v["sh"] and not pc)
        if px and px[len(px) // 2] < 1.0:
            for v in rows.values():
                v["val"] *= 1000
        out.append((period, rows, max(f["filed"] for f in fs)))
    return out, sub.get("name", "")


# ---------- CUSIP -> ticker ----------

def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


US_EXCH = {"US", "UN", "UW", "UQ", "UA", "UR", "UP", "UV", "UF"}


def figi_map(cusips, cache):
    todo = [c for c in cusips if c not in cache]
    _figi(todo, cache, True)
    retry = [c for c in todo if not cache.get(c)]
    _figi(retry, cache, False)


def _figi(todo, cache, us_only):
    if not todo:
        return
    batch = 100 if FIGI_KEY else 10
    pause = 0.3 if FIGI_KEY else 2.6
    print(f"OpenFIGI: mapping {len(todo)} CUSIPs (us_only={us_only})", flush=True)
    for i in range(0, len(todo), batch):
        chunk = todo[i:i + batch]
        jobs = [{"idType": "ID_CUSIP", "idValue": c, **({"exchCode": "US"} if us_only else {})} for c in chunk]
        headers = {"Content-Type": "application/json"}
        if FIGI_KEY:
            headers["X-OPENFIGI-APIKEY"] = FIGI_KEY
        for k in range(5):
            try:
                req = urllib.request.Request("https://api.openfigi.com/v3/mapping",
                                             data=json.dumps(jobs).encode(), headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=60) as r:
                    res = json.loads(r.read())
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(30)
                    continue
                res = None
                break
            except Exception:
                time.sleep(5)
                res = None
        if res is None:
            continue
        for c, item in zip(chunk, res):
            data = item.get("data") or []
            eq = [d for d in data if d.get("marketSector") == "Equity" and d.get("ticker")]
            pick = next((d for d in eq if d.get("exchCode") in US_EXCH), None) or (eq[0] if us_only and eq else None)
            if pick:
                cache[c] = pick["ticker"].replace("/", ".")
            elif "warning" in item:
                cache[c] = None
        time.sleep(pause)


def name_key(s):
    s = s.upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\b(INC|CORP|CORPORATION|CO|COS|COMPANY|COMPANIES|LTD|LIMITED|PLC|HLDG|HLDGS|HOLDING|HOLDINGS|GROUP|GRP|THE|COM|NEW|CL|CLASS|A|B|SA|NV|N V|AG|LP|LLC|SE|ORD|SHS|ADR|SPON|SPONSORED)\b", " ", s)
    return " ".join(s.split())


def main():
    os.makedirs(DATA, exist_ok=True)
    filers = load_json(os.path.join(ROOT, "scripts", "filers.json"), [])
    cache_path = os.path.join(DATA, "cusip_map.json")
    cache = load_json(cache_path, {})
    out_filers, holdings = [], {}

    for f in filers:
        cik = f["cik"]
        try:
            per, edgar_name = periods_for(cik)
        except Exception as e:
            print(f"!! {f['fund']} ({cik}): {e}", file=sys.stderr)
            continue
        if not per:
            print(f"!! {f['fund']}: no 13F-HR found", file=sys.stderr)
            continue
        cur_p, cur, filed = per[0]
        prev_p, prev = (per[1][0], per[1][1]) if len(per) > 1 else (None, {})
        rows = []
        for key in set(cur) | set(prev):
            c, p = cur.get(key), prev.get(key)
            src = c or p
            rows.append([None, src["issuer"], key[0], key[1],
                         c["sh"] if c else 0, c["val"] if c else 0,
                         p["sh"] if p else 0, p["val"] if p else 0])
        rows.sort(key=lambda r: (-r[5], -r[7]))
        total = sum(r[5] for r in rows if not r[3])
        holdings[str(cik)] = rows
        out_filers.append({**f, "edgar": edgar_name, "period": cur_p, "prev": prev_p, "filed": filed,
                           "total": total, "n": sum(1 for r in rows if r[4] > 0)})
        print(f"ok {f['fund']}: {cur_p} vs {prev_p}, {len(rows)} rows", flush=True)

    all_cusips = sorted({r[2] for rows in holdings.values() for r in rows})
    figi_map(all_cusips, cache)

    # fallback: issuer name -> SEC ticker list
    try:
        tick = sec_get("https://www.sec.gov/files/company_tickers.json", as_json=True)
        by_name = {}
        for v in tick.values():
            by_name.setdefault(name_key(v["title"]), v["ticker"])
    except Exception:
        by_name = {}
    ov = {k.upper(): v for k, v in load_json(os.path.join(ROOT, "scripts", "ticker_overrides.json"), {}).items() if not k.startswith("_")}
    for rows in holdings.values():
        for r in rows:
            t = ov.get(r[1].upper()) or cache.get(r[2])
            if not t:
                t = by_name.get(name_key(r[1]))
            r[0] = t

    with open(cache_path, "w", encoding="utf-8") as fp:
        json.dump(cache, fp, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    old = load_json(os.path.join(DATA, "13f.json"), {})
    unchanged = old.get("filers") == out_filers and old.get("h") == holdings
    gen = old["generated"] if unchanged and old.get("generated") else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    out = {"generated": gen,
           "cols": ["ticker", "issuer", "cusip", "putCall", "shares", "value", "prevShares", "prevValue"],
           "filers": out_filers, "h": holdings}
    with open(os.path.join(DATA, "13f.json"), "w", encoding="utf-8") as fp:
        json.dump(out, fp, ensure_ascii=False, separators=(",", ":"))
    print(f"wrote data/13f.json ({len(out_filers)} filers)")


if __name__ == "__main__":
    main()
