"""جمع إصدارات الهيئة العامة للإحصاء المنشورة (Excel/CSV/PDF) من stats.gov.sa وتصدير Tableau — آليًا، القديم والجديد.

python3 scripts/collect_gastat_pub.py --mode discover                  يزحف على كل الفئات → الكتالوج وقائمة الملفات (بلا تنزيل)
python3 scripts/collect_gastat_pub.py --mode update                    يومي: الصفحة الأولى لكل فئة → ينزّل الجديد/المعدَّل فقط + Tableau
python3 scripts/collect_gastat_pub.py --mode backfill [--priority 1] [--max-minutes N] [--max-files N] [--max-mb N] [--resume]
    التاريخي حسب الأولوية. --resume: لا يعيد الزحف على القوائم ويكمل من قائمة الملفات المحفوظة. التكرار آمن دائمًا.

الآلية (اكتُشفت 29 سبتمبر 2026 — 18 ربيع الآخر 1448):
- شجرة الفئات: GET /en/statistics و/ar/statistics (HTML من الخادم): 4 تبويبات (اقتصادية، اجتماعية، بيئية ومكانية، مؤرشفة)
  ← مجموعات «category-collapse-<id>» ← فئات data-category-id (107 فئة يوم الاكتشاف).
- إصدارات الفئة: GET /en/statistics-tabs?tab=436312&category=<ID>&delta=20&start=<رقم الصفحة> (الأحدث أولًا؛ delta>20
  يتعطل بعد الصفحة الأولى، فنلتزم 20). كل إصدار «publication-collapse-<id>» فيه العنوان والسنة والدورية وروابط
  /documents/20117/<folder>/<file>/<uuid>?t=<ختم الرفع ms>. نفس الرابط بختم جديد = نسخة معدّلة تُنزّل.
- تبويبا «الجداول» و«اللوحات» يُرسمان بالمتصفح (لا روابط في HTML)؛ الجداول تخص قاعدة Flexmonster (مهندس آخر).
  عروض Tableau تُدار يدويًا في data/collect/gastat_tableau_views.csv.

الحفظ (لا حذف لخام أبدًا):
- update: raw/history/gastat_pub/<slug>/<تاريخ السحب>/  |  backfill: raw/history/gastat_pub/<slug>/<تاريخ الرفع>/
  (مجلد الإصدار القديم بتاريخ رفعه فيبقى ترتيب المجلدات زمنيًا، ولا يظن موحِّد «أحدث ملف» أن نشرة قديمة هي الأحدث.)
- كل ملف: سطر في raw/history/manifest.csv (sha256 عبر history_manifest؛ النسخة المطابقة لإصدار محفوظ تُحذف) وسطر في
  raw/history/gastat_pub/published_on.csv (published_on من ختم ?t= بتوقيت الرياض، retrieved_on = يوم السحب).
- الحالة: data/collect/gastat_pub_state.json؛ الكتالوج: data/collect/gastat_pub_catalog.csv؛ قائمة الملفات: data/collect/gastat_pub_files.csv؛
  المتخطّى (>50MB): data/collect/gastat_pub_skipped.csv.
- الانضباط: Excel/CSV لكل الفئات، PDF وZIP لفئات الأولوية 1 فقط، لا ملف > 50MB، وسقف ميغابايت لكل تشغيل.
"""
import argparse, csv, datetime, html, json, os, re, subprocess, sys, time, urllib.parse, urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
try:
    import hijri
    def dlabel(d): return hijri.dstr(d)
except Exception:  # hijri-converter غير مثبت: التاريخ الميلادي وحده في السجل الداخلي
    def dlabel(d): return str(d)

UA = "Mozilla/5.0 (compatible; DataPulseOpenData/1.0; +https://github.com/alialia1436-dev/marqab-runner)"
HDR = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.8", "Accept-Language": "ar,en;q=0.9"}
BASE = "https://www.stats.gov.sa"
TAB_PUB = 436312
DELTA = 20
MAX_BYTES = 50 * 1024 * 1024
D = "data/collect"
CATALOG, FILES, STATE, SKIPLOG, TABCFG = (f"{D}/gastat_pub_catalog.csv", f"{D}/gastat_pub_files.csv", f"{D}/gastat_pub_state.json",
                                          f"{D}/gastat_pub_skipped.csv", f"{D}/gastat_tableau_views.csv")
RAW = "raw/history/gastat_pub"
PUBLOG = f"{RAW}/published_on.csv"
DATA_EXT = {"xlsx", "xls", "xlsm", "csv"}
P1_ONLY_EXT = {"pdf", "zip"}
REDISCOVER_DAYS = 7

# الفئات التي سُحبت سابقًا بـ pull_gastat_pub.py تحتفظ بالاسم نفسه (الموحِّدات تبحث فيه)
SLUG = {121920: "real_estate_price_index", 121421: "cpi_tables", 127329: "hies", 121662: "average_prices",
        123427: "wholesale_retail_trade", 3631799: "business_demography", 123038: "structural_business",
        123088: "sme_statistics", 2197212: "comprehensive_economic_survey", 3175009: "housing_statistics",
        120038: "gross_domestic_product"}

# الأولوية لمنتجاتنا: 1 = إنفاق المستهلك والأسعار والسكان والدخل والإنفاق والمنشآت والتجزئة والعقار والعمل والسياحة والحسابات القومية
P1 = {121421, 121920, 121662, 127329, 123427, 3631799, 123038, 123088, 2197212, 120038, 2366987, 3175009, 417515,
      124304, 10132653, 417653, 123342, 1340045, 1340106, 1340079, 1333433}
P2 = {122852, 3453505, 10329368, 1340144, 124074, 417700, 417705, 494915, 1340083, 1340089, 538468, 1333429, 2204353,
      124334, 124364, 123583, 122941, 419265, 123454, 3153652, 124988, 127305, 2163948, 127333, 1340149, 123481,
      1340102, 1340093, 1340112}

# موحِّدات تُستدعى بعد وصول ملف جديد للـ slug (الفشل تحذير فقط)
# المستودع العام: الخام فقط. التوحيد يجري في المستودع الخاص بعد المزامنة.
NORMALIZERS = {}

CAT_COLS = ["category_id", "priority", "slug", "title_ar", "title_en", "group_id", "group_en", "tab_id", "url", "n_pubs",
            "n_files", "n_xlsx", "n_pdf", "n_other", "last_upload_stamp", "last_upload_date", "last_seen"]
FILE_COLS = ["key", "url", "category_id", "slug", "priority", "pub_id", "pub_title", "year", "periodicity", "month", "ext",
             "stamp", "upload_date", "also_in", "first_seen"]

TODAY = datetime.date.today().isoformat()
NREQ = [0]


def warn(msg): print(f"::warning::{msg}", flush=True)


# ---------------------------------------------------------------- شبكة
def get(url, tries=4, delay=1.0, method="GET"):
    err = None
    for t in range(tries):
        try:
            time.sleep(delay)
            NREQ[0] += 1
            req = urllib.request.Request(url, headers=HDR, method=method)
            with urllib.request.urlopen(req, timeout=300) as r:
                return r if method == "HEAD" else r.read()
        except urllib.error.HTTPError as e:
            err = e
            if e.code in (403, 404, 410): break
            time.sleep(10 * 2 ** t)
        except Exception as e:
            err = e; time.sleep(10 * 2 ** t)
    raise RuntimeError(f"{url}: {err}")


def head_size(url, delay):
    try:
        r = get(url, tries=2, delay=delay, method="HEAD")
        v = r.headers.get("Content-Length")
        return int(v) if v else None
    except Exception:
        return None


def download(url, dest, delay, cap=MAX_BYTES, tries=4):
    """ينزّل إلى dest.part ثم يعيد التسمية؛ يتوقف إن تجاوز السقف. يعيد عدد البايتات أو -1 عند تجاوز السقف."""
    err = None
    for t in range(tries):
        try:
            time.sleep(delay); NREQ[0] += 1
            with urllib.request.urlopen(urllib.request.Request(url, headers=HDR), timeout=600) as r, open(dest + ".part", "wb") as fh:
                n = 0
                while True:
                    b = r.read(1 << 20)
                    if not b: break
                    n += len(b)
                    if n > cap:
                        fh.close(); os.remove(dest + ".part"); return -1
                    fh.write(b)
            os.replace(dest + ".part", dest); return n
        except Exception as e:
            err = e
            if os.path.exists(dest + ".part"): os.remove(dest + ".part")
            time.sleep(10 * 2 ** t)
    raise RuntimeError(f"{url}: {err}")


# ---------------------------------------------------------------- تحليل HTML
def clean(s): return re.sub(r"\s+", " ", html.unescape(re.sub("<[^>]+>", " ", s))).strip()


def parse_tree(s):
    tabs = [(m.start(), m.group(1)) for m in re.finditer(r'id="parent-tab(\d+)"', s)]
    out = {}
    for gm in re.finditer(r'data-bs-target="#category-collapse-(\d+)".*?<span class="text">(.*?)</span>(.*?)(?=data-bs-target="#category-collapse-|$)', s, re.S):
        tab = [t for p, t in tabs if p < gm.start()]
        for lm in re.finditer(r'data-category-id="(\d+)"\s*>\s*(.*?)\s*</a>', gm.group(3), re.S):
            out[int(lm.group(1))] = {"tab_id": tab[-1] if tab else "", "group_id": gm.group(1), "group": clean(gm.group(2)),
                                     "title": clean(lm.group(2))}
    return out


def parse_pubs(s):
    """الإصدارات في صفحة قائمة (نسخة الجوال من القائمة: كتلة لكل إصدار)."""
    a = s.find('id="publications-list-accordion"')
    if a < 0: return [], total_entries(s)
    b = s.find("d-none d-md-block", a)
    part = s[a:b if b > 0 else len(s)]
    bits = re.split(r'data-bs-target="#publication-collapse-(\d+)"', part)
    pubs = []
    for i in range(1, len(bits), 2):
        blk = bits[i + 1]
        t = re.search(r'<span class="td">\s*(.*?)\s*</span>', blk, re.S)
        meta = {k: clean(v) for k, v in re.findall(r'<span class="th">\s*(\w+)\s*</span>\s*<span class="td">(.*?)</span>', blk, re.S)}
        links = list(dict.fromkeys(html.unescape(x) for x in re.findall(r'href="(/documents/[^"]+)"', blk)))
        pubs.append({"pub_id": bits[i], "title": clean(t.group(1)) if t else "", "year": meta.get("Year", ""),
                     "periodicity": meta.get("Periodicity", ""), "month": meta.get("Month", ""), "links": links})
    return pubs, total_entries(s)


def total_entries(s):
    m = re.search(r"Showing [\d,]+ to [\d,]+ of ([\d,]+) entries", s)
    return int(m.group(1).replace(",", "")) if m else 0


def key_of(link): return urllib.parse.urlsplit(link).path


def stamp_of(link):
    m = re.search(r"[?&]t=(\d{12,})", link); return int(m.group(1)) if m else 0


def upload_date(stamp):
    if not stamp: return ""
    return (datetime.datetime.utcfromtimestamp(stamp / 1000) + datetime.timedelta(hours=3)).date().isoformat()


def fname(link):
    parts = key_of(link).split("/")
    if len(parts) > 4 and parts[1] == "documents":
        return re.sub(r"_fixed_\d+$", "", urllib.parse.unquote_plus(parts[4]))
    return urllib.parse.unquote_plus(parts[-1]) or "file"  # Tableau وغيره: آخر جزء من المسار


def ext_of(link):
    n = fname(link).lower()
    return n.rsplit(".", 1)[-1] if "." in n else ""


def slugify(t):
    s = re.sub(r"[^a-z0-9]+", "_", t.lower()).strip("_")
    s = re.sub(r"_?statistics?_?", "_", s).strip("_")
    return s[:48] or "cat"


def priority(cid): return 1 if cid in P1 else 2 if cid in P2 else 3


def overlap(a, b):
    w = lambda x: {t for t in re.findall(r"[a-z]{3,}", x.lower()) if t not in {"statistics", "and", "the", "for"}}
    return len(w(a) & w(b))


# ---------------------------------------------------------------- ملفات الحالة
def read_csv(p): return list(csv.DictReader(open(p, encoding="utf-8"))) if os.path.exists(p) else []


def write_csv(p, cols, rows):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p + ".tmp", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, cols, lineterminator="\n", extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    os.replace(p + ".tmp", p)


def load_state():
    st = json.load(open(STATE, encoding="utf-8")) if os.path.exists(STATE) else {}
    st.setdefault("files", {}); st.setdefault("tableau", {}); st.setdefault("runs", [])
    return st


def save_state(st):
    os.makedirs(D, exist_ok=True)
    with open(STATE + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(st, fh, ensure_ascii=False, indent=0, sort_keys=True)
    os.replace(STATE + ".tmp", STATE)


def seed_from_existing(st):
    """ما سحبه pull_gastat_pub.py سابقًا يُعلَّم «منزَّل» حتى لا يُنزَّل مرة أخرى."""
    for r in read_csv(PUBLOG):
        u = r["url"]
        if "/documents/" not in u: continue
        k = key_of(u); f = st["files"].setdefault(k, {})
        if f.get("status") != "downloaded":
            f.update(status="downloaded", t=stamp_of(u), path=r["file"], at=r.get("retrieved_on", ""))


# ---------------------------------------------------------------- الاكتشاف
def fetch_tree(delay):
    en = parse_tree(get(f"{BASE}/en/statistics", delay=delay).decode("utf-8", "replace"))
    ar = parse_tree(get(f"{BASE}/ar/statistics", delay=delay).decode("utf-8", "replace"))
    if len(en) < 50: raise RuntimeError(f"category tree parse returned {len(en)} categories (expected ~107) — page layout changed?")
    return en, ar


def cat_url(cid, page=1):
    u = f"{BASE}/en/statistics-tabs?tab={TAB_PUB}&category={cid}&delta={DELTA}"
    return u + (f"&start={page}" if page > 1 else "")


def crawl_category(cid, delay, max_pages=None):
    """كل صفحات الفئة. الخادم يعيد أحيانًا «No Results Found» لصفحة فيها نتائج (متقطع، رُصد 29 سبتمبر 2026)،
    فالصفحة الفارغة وسط النطاق تُعاد حتى 6 مرات قبل تجاوزها بتحذير."""
    pubs, total, page, npages = [], 0, 1, 1
    while page <= npages:
        for attempt in range(6):
            ps, tot = parse_pubs(get(cat_url(cid, page), delay=delay + 2 * attempt).decode("utf-8", "replace"))
            if page == 1 and attempt == 0:
                total = tot
                npages = max(1, -(-tot // DELTA)) if tot else 1
                if max_pages: npages = min(npages, max_pages)
            if ps or (not tot and (page > 1 or attempt >= 2)): break  # فئة فارغة فعلًا: 3 محاولات تكفي
        if not ps and tot: warn(f"category {cid} page {page}: empty after retries")
        pubs += ps
        page += 1
    return pubs, (total or len(pubs))


def build_catalog(en, ar, old):
    oldc = {int(r["category_id"]): r for r in old}
    used, rows = set(), {}
    for cid, v in en.items():
        slug = SLUG.get(cid) or oldc.get(cid, {}).get("slug") or slugify(("arch_" if v["tab_id"] == "1317550" else "") + v["title"])
        if slug in used: slug = f"{slug}_{cid}"
        used.add(slug)
        o = oldc.get(cid, {})
        rows[cid] = {"category_id": cid, "priority": priority(cid), "slug": slug, "title_ar": ar.get(cid, {}).get("title", ""),
                     "title_en": v["title"], "group_id": v["group_id"], "group_en": v["group"], "tab_id": v["tab_id"],
                     "url": cat_url(cid), **{k: o.get(k, "") for k in ["n_pubs", "n_files", "n_xlsx", "n_pdf", "n_other",
                                                                      "last_upload_stamp", "last_upload_date", "last_seen"]}}
    return rows


def assign(cands, cats):
    """ملف يظهر في أكثر من فئة: الفئة الأقرب لعنوان الإصدار، ثم الأعلى أولوية."""
    return sorted(cands, key=lambda c: (-overlap(c[1]["title"], cats[c[0]]["title_en"]), cats[c[0]]["priority"], c[0]))[0]


def merge_files(seen, cats, files):
    """seen: {key: [(cid, pub, link)]} → يحدّث قائمة الملفات (dict key→row). يعيد مفاتيح الملفات الجديدة/المعدّلة."""
    changed = []
    for k, occ in seen.items():
        cid, pub, link = assign(occ, cats)
        st = stamp_of(link)
        row = files.get(k)
        if row and int(row["stamp"] or 0) == st: continue
        also = sorted({str(c) for c, _, _ in occ} - {str(cid)})
        files[k] = {"key": k, "url": BASE + link, "category_id": cid, "slug": cats[cid]["slug"], "priority": cats[cid]["priority"],
                    "pub_id": pub["pub_id"], "pub_title": pub["title"], "year": pub["year"], "periodicity": pub["periodicity"],
                    "month": pub["month"], "ext": ext_of(link), "stamp": st, "upload_date": upload_date(st),
                    "also_in": " ".join(also) if not row else row.get("also_in", ""),
                    "first_seen": row["first_seen"] if row else TODAY}
        changed.append(k)
    return changed


def refresh_counts(cats, files, touched):
    by = {}
    for r in files.values(): by.setdefault(int(r["category_id"]), []).append(r)
    for cid in touched:
        c = cats[cid]; fs = by.get(cid, [])
        c["n_files"] = len(fs)
        c["n_xlsx"] = sum(r["ext"] in DATA_EXT for r in fs)
        c["n_pdf"] = sum(r["ext"] == "pdf" for r in fs)
        c["n_other"] = len(fs) - c["n_xlsx"] - c["n_pdf"]
        mx = max((int(r["stamp"] or 0) for r in fs), default=0)
        c["last_upload_stamp"] = mx or ""; c["last_upload_date"] = upload_date(mx)
        c["last_seen"] = TODAY


def discover(args, st, full=True, only=None):
    en, ar = fetch_tree(args.delay)
    cats = build_catalog(en, ar, read_csv(CATALOG))
    files = {r["key"]: r for r in read_csv(FILES)}
    seen, fails, touched = {}, [], []
    ids = [c for c in cats if (not only or c in only)]
    for n, cid in enumerate(sorted(ids, key=lambda c: (cats[c]["priority"], c)), 1):
        try:
            pubs, total = crawl_category(cid, args.delay, None if full else 1)
        except Exception as e:
            fails.append(cid); warn(f"category {cid} {cats[cid]['title_en']}: {e}"); continue
        prev = int(cats[cid]["n_pubs"] or 0)
        if full:
            cats[cid]["n_pubs"] = total
            if prev and total == 0: warn(f"category {cid} had {prev} publications, now 0 — layout change?")
            if total and len(pubs) < total: warn(f"category {cid}: parsed {len(pubs)} of {total} publications")
        elif not cats[cid]["n_pubs"]:
            cats[cid]["n_pubs"] = total
        for p in pubs:
            for l in p["links"]: seen.setdefault(key_of(l), []).append((cid, p, l))
        touched.append(cid)
        print(f"[{n}/{len(ids)}] {cid} p{cats[cid]['priority']} {cats[cid]['slug']}: {len(pubs)}/{total} pubs", flush=True)
    changed = merge_files(seen, cats, files)
    refresh_counts(cats, files, touched)
    if len(fails) > max(3, len(ids) // 4): raise RuntimeError(f"{len(fails)} of {len(ids)} category pages failed — site blocked or down")
    write_csv(CATALOG, CAT_COLS, sorted(cats.values(), key=lambda r: (int(r["priority"]), int(r["category_id"]))))
    write_csv(FILES, FILE_COLS, sorted(files.values(), key=lambda r: (int(r["priority"]), int(r["category_id"]), -int(r["stamp"] or 0))))
    if full: st["last_discover"] = TODAY
    if "baseline_stamp" not in st:
        # أول اكتشاف: ما رُفع قبل 14 يومًا «تاريخي» (للـ backfill)، والأحدث منه يأخذه update
        st["baseline_stamp"] = int((time.time() - 14 * 86400) * 1000)
    return cats, files, changed, fails


# ---------------------------------------------------------------- التنزيل
def allowed(r):
    e = r["ext"]
    if e in DATA_EXT: return True
    # قرار إدارة البرمجة (29 سبتمبر 2026): PDF/ZIP خارج الجمع افتراضيًا (~1.5 ج.ب من 2 ج.ب في الأولوية 1)، والبيانات في Excel/CSV.
    # للتضمين لفئات الأولوية 1: GASTAT_PUB_PDF=1.
    if e in P1_ONLY_EXT: return os.environ.get("GASTAT_PUB_PDF") == "1" and int(r["priority"]) == 1
    return False


def append_skip(r, size, reason):
    new = not os.path.exists(SKIPLOG)
    with open(SKIPLOG, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        if new: w.writerow(["date", "key", "slug", "size_bytes", "reason", "url"])
        w.writerow([TODAY, r["key"], r["slug"], size or "", reason, r["url"]])


def fetch_file(r, st, folder_date, args, summ):
    """ينزّل ملفًا واحدًا ويسجّله. يعيد True إن أُضيف ملف جديد للخام."""
    k = r["key"]; f = st["files"].setdefault(k, {})
    size = head_size(r["url"], args.delay)
    if size and size > MAX_BYTES:
        f.update(status="skipped_size", t=int(r["stamp"]), size=size, at=TODAY); append_skip(r, size, ">50MB")
        summ["skipped_size"].append(f"{r['slug']}/{fname(r['url'])} ({size / 1e6:.0f} MB)"); return False
    od = f"{RAW}/{r['slug']}/{folder_date}"; os.makedirs(od, exist_ok=True)
    name = fname(r["url"]); fn = f"{od}/{name}"
    if os.path.exists(fn):
        stem, dot, ext = name.rpartition(".")
        fn = f"{od}/{stem or name}__{k.rstrip('/').split('/')[-1][:8]}{dot}{ext if stem else ''}"
    try:
        n = download(r["url"], fn, args.delay)
    except Exception as e:
        f.update(status="failed", t=int(r["stamp"]), tries=f.get("tries", 0) + 1, at=TODAY, error=str(e)[:200])
        summ["failed"].append(f"{r['slug']}/{name}: {str(e)[:120]}")
        try: os.rmdir(od)
        except OSError: pass
        return False
    if n < 0:
        f.update(status="skipped_size", t=int(r["stamp"]), size=f">{MAX_BYTES}", at=TODAY); append_skip(r, "", ">50MB (stream)")
        summ["skipped_size"].append(f"{r['slug']}/{name} (>50 MB)")
        try: os.rmdir(od)
        except OSError: pass
        return False
    p = subprocess.run([sys.executable, "scripts/history_manifest.py", "gastat_pub", TODAY, fn, r["url"]], capture_output=True, text=True)
    out = p.stdout.strip() or p.stderr.strip()[-200:]
    added = out.startswith("added")
    if added:
        new = not os.path.exists(PUBLOG)
        with open(PUBLOG, "a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            if new: w.writerow(["file", "slug", "url", "published_on", "retrieved_on"])
            w.writerow([fn, r["slug"], r["url"], r["upload_date"], TODAY])
        f.update(status="downloaded", t=int(r["stamp"]), path=fn, size=n, at=TODAY)
        summ["added"].append((r["slug"], fn, n)); summ["bytes"] += n
    elif out.startswith("unchanged"):
        f.update(status="dup", t=int(r["stamp"]), size=n, at=TODAY); summ["dup"] += 1
    elif out.startswith("already"):
        f.update(status="downloaded", t=int(r["stamp"]), path=fn, at=TODAY)
    else:
        f.update(status="failed", t=int(r["stamp"]), tries=f.get("tries", 0) + 1, at=TODAY, error=out[:200])
        summ["failed"].append(f"{r['slug']}/{name}: manifest {out[:120]}")
    try: os.rmdir(od)
    except OSError: pass
    return added


def pending(r, st):
    f = st["files"].get(r["key"], {})
    if f.get("status") in ("downloaded", "dup", "skipped_size") and int(f.get("t", 0)) == int(r["stamp"] or 0): return False
    if f.get("status") == "failed" and f.get("tries", 0) >= 3 and int(f.get("t", 0)) == int(r["stamp"] or 0): return False
    return True


def run_normalizers(slugs, summ):
    done = set()
    for s in sorted(slugs):
        for script in NORMALIZERS.get(s, []):
            if script in done: continue
            done.add(script)
            try:
                p = subprocess.run([sys.executable, f"scripts/{script}"], capture_output=True, text=True, timeout=900)
                ok = p.returncode == 0
                summ["normalized"].append(f"{script}: {'ok' if ok else 'FAILED'}")
                if not ok: warn(f"normalizer {script} failed: {(p.stderr or p.stdout).strip()[-300:]}")
            except Exception as e:
                summ["normalized"].append(f"{script}: FAILED"); warn(f"normalizer {script}: {e}")


def tableau(st, args, summ, force=False):
    for v in read_csv(TABCFG):
        slug, url = v["slug"].strip(), v["url"].strip()
        if not slug or not url or slug.startswith("#"): continue
        last = st["tableau"].get(slug, {}).get("last_fetch", "")
        every = int(v.get("every_days") or 7)
        if not force and last and (datetime.date.fromisoformat(TODAY) - datetime.date.fromisoformat(last)).days < every: continue
        r = {"key": url, "url": url, "slug": slug, "stamp": 0, "upload_date": "", "priority": v.get("priority", 1), "ext": "csv"}
        # نفس المنطق: سقف الحجم، وبصمة تمنع تكرار نسخة مطابقة
        st["files"].pop(url, None)
        try:
            fetch_file(r, st, TODAY, args, summ)
            st["tableau"][slug] = {"last_fetch": TODAY}
        except Exception as e:
            warn(f"tableau {slug}: {e}"); summ["failed"].append(f"tableau {slug}: {str(e)[:120]}")


# ---------------------------------------------------------------- الأوضاع
def new_summary(): return {"added": [], "dup": 0, "skipped_size": [], "skipped_type": 0, "failed": [], "bytes": 0, "normalized": []}


def print_summary(mode, summ, t0, extra=""):
    mins = (time.time() - t0) / 60
    print("\n==== gastat_pub", mode, "|", dlabel(TODAY), f"| {mins:.1f} min | {NREQ[0]} requests", extra)
    print(f"new files: {len(summ['added'])} ({summ['bytes'] / 1e6:.1f} MB) | identical to archived: {summ['dup']} | "
          f"skipped >50MB: {len(summ['skipped_size'])} | skipped by type rule: {summ['skipped_type']} | failed: {len(summ['failed'])}")
    for s, fn, n in summ["added"][:60]: print(f"  + {fn} ({n / 1e6:.2f} MB)")
    if len(summ["added"]) > 60: print(f"  … +{len(summ['added']) - 60}")
    for x in summ["skipped_size"]: print("  skip", x)
    for x in summ["failed"]: warn("download failed " + x)
    for x in summ["normalized"]: print("  normalize", x)


def log_run(st, mode, summ, extra):
    st["runs"] = (st["runs"] + [{"date": TODAY, "mode": mode, "added": len(summ["added"]), "mb": round(summ["bytes"] / 1e6, 1),
                                 "failed": len(summ["failed"]), "skipped_size": len(summ["skipped_size"]), **extra}])[-30:]


def mode_update(args, st):
    t0 = time.time(); summ = new_summary()
    full = not st.get("last_discover") or (datetime.date.fromisoformat(TODAY) - datetime.date.fromisoformat(st["last_discover"])).days >= REDISCOVER_DAYS
    cats, files, changed, fails = discover(args, st, full=full)
    base = int(st.get("baseline_stamp", 0))
    todo = [files[k] for k in changed if k in files]
    # إضافة لما تغيّر: ملفات حديثة (بعد خط الأساس) لم تُنزّل بعد (مثلًا فشلت أمس)
    todo += [r for r in files.values() if int(r["stamp"] or 0) >= base and r["key"] not in changed]
    new_slugs = set()
    for r in sorted(todo, key=lambda r: -int(r["stamp"] or 0)):
        if not pending(r, st): continue
        if int(r["stamp"] or 0) < base and st["files"].get(r["key"], {}).get("status") not in ("downloaded", "dup"):
            continue  # ملف تاريخي ظهر أول مرة: مكانه backfill
        if not allowed(r): summ["skipped_type"] += 1; continue
        if fetch_file(r, st, TODAY, args, summ):
            new_slugs.add(r["slug"])
            if r["slug"] in NORMALIZERS: st["normalize_pending"] = sorted(set(st.get("normalize_pending", [])) | {r["slug"]})
        save_state(st)
    tableau(st, args, summ)
    # يشمل ما نُزّل في تشغيل سابق انقطع قبل التوحيد
    todo_norm = ({s for s, _, _ in summ["added"]} | set(st.get("normalize_pending", []))) & set(NORMALIZERS)
    if todo_norm and not args.no_normalize:
        run_normalizers(todo_norm, summ); st["normalize_pending"] = []
    log_run(st, "update", summ, {"full_discover": full, "listing_failures": len(fails)})
    save_state(st)
    print_summary("update", summ, t0, f"| full discover: {full} | listing failures: {len(fails)}")


def mode_backfill(args, st):
    t0 = time.time(); summ = new_summary()
    if args.resume and os.path.exists(FILES):
        files = {r["key"]: r for r in read_csv(FILES)}
    else:
        only = None
        if args.priority:
            only = {int(r["category_id"]) for r in read_csv(CATALOG) if int(r["priority"]) <= args.priority} or None
        _, files, _, _ = discover(args, st, full=True, only=only)
    rows = [r for r in files.values() if (not args.priority or int(r["priority"]) <= args.priority)
            and (not args.category or int(r["category_id"]) in args.category)]
    rows.sort(key=lambda r: (int(r["priority"]), int(r["category_id"]), -int(r["stamp"] or 0)))
    left = [r for r in rows if pending(r, st)]
    typed = [r for r in left if allowed(r)]; summ["skipped_type"] = len(left) - len(typed)
    budget_s = args.max_minutes * 60 if args.max_minutes else None
    cap_b = args.max_mb * 1e6 if args.max_mb else None
    new_slugs, n = set(), 0
    for r in typed:
        if budget_s and time.time() - t0 > budget_s: print("time budget reached"); break
        if cap_b and summ["bytes"] > cap_b: print("size budget reached"); break
        if args.max_files and n >= args.max_files: break
        n += 1
        if fetch_file(r, st, r["upload_date"] or TODAY, args, summ): new_slugs.add(r["slug"])
        if n % 5 == 0: save_state(st)
    if new_slugs and args.normalize: run_normalizers(new_slugs, summ)
    remaining = sum(pending(r, st) and allowed(r) for r in rows)
    log_run(st, "backfill", summ, {"priority": args.priority or 0, "remaining": remaining})
    save_state(st)
    print_summary("backfill", summ, t0, f"| remaining eligible files: {remaining}")


def mode_discover(args, st):
    t0 = time.time()
    cats, files, changed, fails = discover(args, st, full=True)
    save_state(st)
    rows = list(files.values())
    print("\n==== gastat_pub discover |", dlabel(TODAY), f"| {(time.time() - t0) / 60:.1f} min | {NREQ[0]} requests")
    print(f"categories: {len(cats)} | publications: {sum(int(c['n_pubs'] or 0) for c in cats.values())} | files: {len(rows)} "
          f"| new/changed since last discover: {len(changed)} | listing failures: {len(fails)}")
    for p in (1, 2, 3):
        rs = [r for r in rows if int(r["priority"]) == p]
        el = [r for r in rs if allowed(r)]
        print(f"  priority {p}: categories {sum(int(c['priority']) == p for c in cats.values())}, files {len(rs)}, eligible "
              f"{len(el)} (xlsx/csv {sum(r['ext'] in DATA_EXT for r in el)}, pdf {sum(r['ext'] == 'pdf' for r in el)}, "
              f"zip {sum(r['ext'] == 'zip' for r in el)})")
    if args.head_sample:
        import random
        random.seed(1); by = {}
        el = [r for r in rows if allowed(r)]
        for e in sorted({r["ext"] for r in el}):
            smp = random.sample([r for r in el if r["ext"] == e], min(args.head_sample, sum(r["ext"] == e for r in el)))
            sz = [x for x in (head_size(r["url"], args.delay) for r in smp) if x]
            by[e] = (sum(sz) / len(sz) if sz else 0, max(sz) if sz else 0, len(sz))
        print("size sample (avg MB, max MB, n):", {e: (round(a / 1e6, 2), round(m / 1e6, 1), k) for e, (a, m, k) in by.items()})
        for p in (1, 2, 3):
            est = sum(by.get(r["ext"], (0,))[0] for r in el if int(r["priority"]) == p)
            print(f"  estimated backfill size priority {p}: {est / 1e6:,.0f} MB")
        st["size_sample"] = {e: {"avg_mb": round(a / 1e6, 3), "max_mb": round(m / 1e6, 1), "n": k} for e, (a, m, k) in by.items()}
        save_state(st)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["discover", "update", "backfill"], required=True)
    ap.add_argument("--priority", type=int, choices=[1, 2, 3])
    ap.add_argument("--category", type=int, nargs="*")
    ap.add_argument("--max-minutes", type=float)
    ap.add_argument("--max-files", type=int)
    ap.add_argument("--max-mb", type=float, default=700, help="سقف حجم التنزيل لكل تشغيل (backfill)")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--normalize", action="store_true", help="backfill: شغّل الموحِّدات بعده (افتراضيًا لا)")
    ap.add_argument("--no-normalize", action="store_true", help="update: لا تشغّل الموحِّدات")
    ap.add_argument("--delay", type=float, default=1.0, help="ثوانٍ بين الطلبات")
    ap.add_argument("--head-sample", type=int, default=0, help="discover: عينة HEAD لكل امتداد لتقدير حجم backfill")
    args = ap.parse_args()
    import collect_budget; collect_budget.apply('gastat_pub', args.mode, args)  # ميزانية دقائق Actions (data/collect/budget.json)
    st = load_state(); seed_from_existing(st)
    {"discover": mode_discover, "update": mode_update, "backfill": mode_backfill}[args.mode](args, st)


if __name__ == "__main__":
    main()
