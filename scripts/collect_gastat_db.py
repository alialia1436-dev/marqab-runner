"""جمع قاعدة البيانات الإحصائية للهيئة العامة للإحصاء (database.stats.gov.sa، خادم Flexmonster العام) آليًا.

نقطة دخول واحدة تشغّلها GitHub Actions (scripts/workflows/collect-gastat-db.yml) ولا تحتاج Claude إلا عند العطل:

  python3 scripts/collect_gastat_db.py --mode update          # يومي: ما تغيّر فقط
  python3 scripts/collect_gastat_db.py --mode backfill [--limit N] [--resume] [--max-minutes 300] [--max-mb 400]
  python3 scripts/collect_gastat_db.py --mode recheck-broken  # أسبوعي: الفهارس «Index not found»
  (للاختبار: --only IX1,IX2 يحصر التشغيل في فهارس محددة؛ --all في update يتجاهل جدولة الفهارس السنوية)

كيف يقرر update أن فهرسًا تغيّر، بطلبين صغيرين لا بسحب كامل:
  1) /fields (أسماء الحقول وأنواعها)، 2) /select بتجميع واحد حسب حقل الزمن: لكل فترة (وللإجمالي) عدد الصفوف +
  مجموع كل حقل رقمي + أقصى كل حقل تاريخ؛ آخر فترة = last_period. بصمة الاثنين = sig في ملف الحالة.
  sig معروف (الحالي أو نسخة سابقة مؤرشفة) → لا سحب. تغيّر (فترة جديدة، مراجعة قيمة، حقل جديد) → سحب كامل
  (كل الحقول، وكل طلب صفحة واحدة بمرشح على الفترات: ترتيب الخادم غير ثابت فالسحب متعدد الصفحات يكرر ويُسقط صفوفًا).
  أول مرة لفهرس مؤرشف قبل هذه الأداة: إن طابق عدد الصفوف وآخر فترة سطر manifest وكان صفحة واحدة تُعتمد البصمة بلا سحب.
الأرشفة: raw/history/gastat/<INDEX>/<تاريخ اليوم>/<INDEX>.csv.gz (سحب ثانٍ في اليوم نفسه → <تاريخ>-2)، الصفوف مرتبة
  ترتيبًا ثابتًا وgzip بلا ختم وقت، فنفس المحتوى = نفس البصمة. نفس محتوى أي لقطة سابقة → لا يُكتب شيء، وكل ملف يمر على
  scripts/history_manifest.py. لا ملف فوق 45 م.ب (أجزاء .p<n>). الفهارس الثقيلة (> مليون صف، تجارة خارجية غالبًا) تُسحب
  سنة بسنة بمرشح <INDEX>.y<سنة>.csv.gz، والسنوات التي تغيّرت بصمتها فقط، فتتوقف عند الميزانية وتكمل لاحقًا.
  الكتابة في *.part (في .gitignore) ثم إعادة تسمية: لا ملف ناقص في الأرشيف، ولا سحب ناقص أو مختلط النسخ.
بعد لقطة جديدة لفهرس له موحِّد (أسعار المستهلك، السكان، دخل وإنفاق الأسرة، البطالة بالمنطقة، مرافق الضيافة، الحج والعمرة،
الكثافة والنمو السكاني، الطاقة المنزلية والكهرباء ومياه الشرب، التعليم والصحة والاتصالات) يُشغَّل الموحِّد؛ فشله تحذير لا يوقف التشغيل.
الحالة: data/collect/gastat_db_state.json (سطر لكل فهرس: last_checked, last_period, last_sha, status, error ...). صغير ويُرفع.
لا يُحذف خام أبدًا. لطيف مع الخادم: طلب واحد في كل مرة، نوم قصير بين الطلبات، إعادة محاولة بتراجع أُسّي.
"""
import argparse, csv, datetime, glob, gzip, hashlib, io, json, os, re, subprocess, sys, time
import requests

B = "https://database.stats.gov.sa/flexmonster/"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE = "data/collect/gastat_db_state.json"
INV = "data/gastat_inventory.csv"
MANIFEST = "raw/history/manifest.csv"
SPLIT_BYTES = int(os.environ.get("GASTAT_SPLIT_BYTES", 45 * 1024 * 1024))  # حد الملف الواحد (GitHub يرفض > 100 م.ب ويحذّر فوق 50)
BYTES_PER_ROW = 14                      # تقدير الحجم المضغوط (مقاس: 461,826 صفًا ≈ 6.1 م.ب)
HEAVY_ROWS = int(os.environ.get("GASTAT_HEAVY_ROWS", 1_000_000))  # «ثقيل»: آخر الطابور، ويُسحب سنة بسنة 
PAGE_ROWS = 45_000                      # الخادم يعيد 50,000 صف في الصفحة؛ كل طلب يُخطَّط ليكون صفحة واحدة بهامش
CELLS_PER_MIN = 500_000                 # سرعة الخادم المقاسة 29 سبتمبر 2026 (صف × حقل في الدقيقة) لتقدير زمن السحب
HARD_CAP_ROWS = 12_000_000              # فوقه لا يُسحب آليًا (الصفوف تُرتَّب في الذاكرة ≈ 250 بايت/صف) → status too_big
# حقل الزمن الكامل، الأدق أولًا (MONTH_TIME وQUARTER_TIME وحدهما بلا سنة فلا يصلحان)
TIME_CANDIDATES = ["YEAR_MONTH", "YEAR_MONTH_TIME", "YEAR_QUARTER_TIME", "YEAR_TIME", "YEAR", "PERIOD", "TIME_PERIOD",
                   "YEAR_FREQUENCY_TIME"]
NOT_TIME = {"BASE_YEAR_TIME", "MONTH_TIME", "QUARTER_TIME"}
FREQ_ORDER = {"Monthly": 0, "Quarterly": 1, "Semi Annual": 2, "Annual": 3}
DAILY_FREQ = {"Monthly", "Quarterly"}   # تُفحص يوميًا؛ السنوية ونصف السنوية أسبوعيًا موزعة على أيام الأسبوع
# المستودع العام: الخام فقط. التوحيد (clean/) يجري في المستودع الخاص بعد المزامنة (scripts/sync_gastat_open.py هناك).
NORMALIZERS = []


class NotFound(Exception):
    pass


class MixedVersions(RuntimeError):
    pass


def log(*a):
    print(*a, flush=True)


def warn(msg):
    print(f"::warning::{msg}", flush=True)


# ---------- الشبكة ----------
SLEEP = 1.0


def post(ep, body, timeout=600, tries=5):
    """طلب واحد بإعادة محاولة وتراجع أُسّي (10، 30، 90، 270 ث). «Index not found» → NotFound فورًا بلا إعادة."""
    delay = 10
    for t in range(tries):
        try:
            r = requests.post(B + ep, json=body, timeout=timeout)
            if r.status_code == 400 and "not found" in r.text.lower():
                raise NotFound(r.text.strip()[:200])
            r.raise_for_status()
            time.sleep(SLEEP)
            return r.json()
        except NotFound:
            raise
        except Exception as e:
            if t == tries - 1:
                raise RuntimeError(f"{ep} {body.get('index')}: {str(e)[:200]}")
            print("  retry", ep, body.get("index"), body.get("page", ""), str(e)[:120], f"sleep {delay}s",
                  file=sys.stderr, flush=True)
            time.sleep(delay); delay *= 3


def period_key(v):
    """ترتيب الفترات: «2026/07»، «2021 / Q1»، 2024، «2023-2024» → أرقامها بالترتيب."""
    nums = [int(x) for x in re.findall(r"\d+", str(v))]
    return (nums or [-1], str(v))


def norm_period(v):
    s = re.sub(r"\s+", "", str(v))
    return s[:-2] if s.endswith(".0") else s


def year_of(v):
    m = re.search(r"(19|20)\d\d", str(v))
    return m.group(0) if m else "na"


def pick_time_field(names, hint=None):
    """حقل الزمن الكامل أولًا (YEAR_MONTH قبل حقل «الشهر» وحده الذي قد يرد في الحصر)، ثم أي حقل *_TIME، ثم تلميح الحصر."""
    return next((c for c in TIME_CANDIDATES if c in names), None) \
        or next((c for c in names if c.endswith("_TIME") and c not in NOT_TIME), None) \
        or (hint if hint in names else None)


def fnum(v):
    """المجاميع العشرية تختلف في آخر الخانات حسب ترتيب الجمع عند الخادم (154366.4999999999 مقابل ...49999999994):
    3 منازل عشرية بحد أقصى 12 رقمًا — تكشف أي مراجعة منشورة وتتجاهل ضجيج الجمع."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return v  # الخادم يعيد المجموع الصحيح أحيانًا عددًا صحيحًا (136674) وأحيانًا عشريًا: يوحَّدان
    v = float(v)
    d = max(0, min(3, 12 - len(str(int(abs(v))))))
    return f"{v:.{d}f}".replace("-0.000", "0.000")


def canon_vals(d):
    return {k: {f: fnum(v) for f, v in sorted(x.items())} for k, x in sorted(d.items())}


def fingerprint(ix, tf_hint=None):
    """بصمة رخيصة لحالة الفهرس في المصدر بطلبين صغيرين: الحقول، وتجميع واحد حسب الفترة
    (عدد الصفوف + مجموع كل حقل رقمي + أقصى كل حقل تاريخ، لكل فترة وللإجمالي).
    لا نستعمل /members: رُصد 29 سبتمبر 2026 يعيد قوائم متقلبة (127 فترة ثم فترة واحدة) وأخطاء 500."""
    fields = post("fields", {"type": "fields", "index": ix}, timeout=120, tries=3)["fields"]
    names = [f["uniqueName"] for f in fields]
    nums = [f["uniqueName"] for f in fields if f.get("type") == "number"][:30]
    dates = [f["uniqueName"] for f in fields if f.get("type") == "date"][:5]
    vals = [{"field": {"uniqueName": names[0]}, "func": "count"}]
    vals += [{"field": {"uniqueName": n}, "func": "sum"} for n in nums if n != names[0]]
    vals += [{"field": {"uniqueName": d}, "func": "max"} for d in dates if d != names[0]]
    tf = pick_time_field(names, tf_hint)
    q = {"aggs": {"values": vals}}
    if tf:
        q["aggs"]["by"] = {"rows": [{"uniqueName": tf}]}
    groups, p, total = [], 0, 1
    while p < total and p < 50:
        r = post("select", {"type": "select", "index": ix, "page": p, "query": q}, timeout=300, tries=3)
        groups += r.get("aggs", []); total = r.get("pageTotal", 1); p += 1
    tot = next((g["values"] for g in groups if not g.get("keys")), {})
    keyed = [g for g in groups if g.get("keys")]
    per = {str(g["keys"].get(tf)): canon_vals(g["values"]) for g in keyed}
    rows = int(tot.get(names[0], {}).get("count", 0) or 0)
    periods = sorted(per, key=period_key)
    # لكل سنة: بصمة فتراتها، وعدد صفوفها، وقيم حقل الزمن كما يعيدها الخادم (لسحب السنة وحدها بمرشح)
    years = {}
    for g in keyed:
        k = g["keys"].get(tf); y = years.setdefault(year_of(k), {"members": [], "rows": 0, "per": {}})
        c = int(g["values"].get(names[0], {}).get("count", 0) or 0)
        y["members"].append([k, c]); y["rows"] += c
        y["per"][str(k)] = per[str(k)]
    for y in years.values():
        y["hash"] = hashlib.sha256(json.dumps(y.pop("per"), ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:12]
    sig = hashlib.sha256(json.dumps({"f": [[f["uniqueName"], f.get("type")] for f in fields], "t": canon_vals(tot),
                                     "p": per}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]
    return {"names": names, "types": {f["uniqueName"]: f.get("type") for f in fields}, "rows": rows, "tf": tf, "last_period": periods[-1] if periods else "",
            "first_period": periods[0] if periods else "", "n_periods": len(periods), "sig": sig, "years": years}


# ---------- السحب والأرشفة ----------


def csv_line(row):
    buf = io.StringIO(); csv.writer(buf, lineterminator="\n").writerow(row); return buf.getvalue()


def write_gz(path, header, lines):
    """gzip بلا اسم ولا ختم وقت: نفس المحتوى = نفس البصمة."""
    with open(path, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
        gz.write(header.encode("utf-8"))
        for ln in lines:
            gz.write(ln.encode("utf-8"))


def read_lines(path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") if path.endswith(".gz") else \
            open(path, encoding="utf-8-sig", newline="") as fh:
        rd = csv.reader(fh)
        return csv_line(next(rd, [])), sorted(csv_line(r) for r in rd)


def snapshot_dir(ix, day):
    """مجلد اللقطة: raw/history/gastat/<INDEX>/<تاريخ>/، وإن وُجد (سحب ثانٍ في اليوم نفسه) فـ<تاريخ>-2 ... لا كتابة فوق خام."""
    base = f"raw/history/gastat/{ix}/{day}"; od, k = base, 1
    while os.path.isdir(od) and os.listdir(od):
        k += 1; od = f"{base}-{k}"
    return od


def content_hash(header, lines):
    h = hashlib.sha256(header.encode("utf-8"))
    for ln in lines:
        h.update(ln.encode("utf-8"))
    return h.hexdigest()[:16]


def known_contents(ix, s):
    """بصمات المحتوى (الصفوف مرتبة) لكل لقطة مؤرشفة للفهرس — تُحسب مرة من الملفات ثم تُحفظ في الحالة.
    تحمي من مصدر «يتأرجح» بين نسختين (رُصد 29 سبتمبر 2026 في DPV_RS_EFPI0404: خادمان بمحتويين مختلفين)."""
    if "cshas" not in s:
        fs = [f for f in glob.glob(f"raw/history/gastat/{ix}/*/{ix}.csv.gz")]
        if s.get("last_file", "").endswith((".csv", ".csv.gz")) and s["last_file"] not in fs:
            fs.append(s["last_file"])
        hs = []
        for f in sorted(fs):
            if os.path.isfile(f) and os.path.getsize(f) <= SPLIT_BYTES:
                try:
                    hs.append(content_hash(*read_lines(f)))
                except Exception as e:
                    warn(f"cannot hash {f}: {e}")
        s["cshas"] = sorted(set(hs))
    return s["cshas"]


def fetch_rows(ix, names, expect, filt=None):
    """صفحات طلب واحد (بمرشح أو بلا)، مع فحص الاكتمال والاتساق. يعيد (الصفوف، عدد الصفحات)."""
    rows, ragged, p, total = [], 0, 0, 1
    q = {"fields": [{"uniqueName": x} for x in names]}
    if filt:
        q["filter"] = filt
    while p < total:
        r = post("select", {"type": "select", "index": ix, "page": p, "query": q})
        total = r["pageTotal"]
        for h in r["hits"]:
            ragged += len(h) < len(names)
        rows += r["hits"]; p += 1
    if len(rows) < expect:
        raise RuntimeError(f"incomplete pull: {len(rows)} rows < {expect} counted by server")
    # الخادم يخدم أحيانًا نسختين من الفهرس (رُصد 29 سبتمبر 2026: DPV_RS_EFPI0404 وDPV_CLI_CHANGE_YEAR تتبدل حقولهما بين طلبين).
    # صف أقصر من قائمة الحقول = صفحات من نسخة غير التي أعطت الحقول → لا أرشفة، ويُعاد في التشغيل التالي.
    if ragged:
        raise MixedVersions(f"{ragged} rows shorter than the {len(names)} fields (mixed source versions); not archived, retry next run")
    return rows, total


def group_counts(ix, first, field, filt):
    q = {"aggs": {"values": [{"field": {"uniqueName": first}, "func": "count"}], "by": {"rows": [{"uniqueName": field}]}}}
    if filt:
        q["filter"] = filt
    groups, p, total = [], 0, 1
    while p < total and p < 50:
        r = post("select", {"type": "select", "index": ix, "page": p, "query": q}, timeout=300, tries=3)
        groups += r.get("aggs", []); total = r.get("pageTotal", 1); p += 1
    return [[g["keys"].get(field), int(g["values"].get(first, {}).get("count", 0) or 0)] for g in groups if g.get("keys")]


def plan_chunks(ix, fp, filt, field, members, used, depth=0):
    """يقسم السحب إلى طلبات كل منها صفحة واحدة (≤ PAGE_ROWS صفًا): يجمع قيم الحقل في دفعات، والقيمة الأكبر من صفحة
    تُقسَّم بحقل آخر (حتى 3 مستويات). السبب: ترتيب الخادم غير ثابت، فالسحب متعدد الصفحات يكرر صفوفًا ويُسقط غيرها
    (رُصد 29 سبتمبر 2026: 924 صفًا مكررًا في أسعار المستهلك). يعيد [(مرشح، العدد المتوقع)]."""
    out, batch, bc = [], [], 0

    def flush():
        if batch:
            out.append((filt + [{"field": {"uniqueName": field}, "include": [{"member": k} for k in batch]}], bc))

    for k, c in sorted(members, key=lambda m: period_key(m[0])):
        if c > PAGE_ROWS:
            out.extend(split_further(ix, fp, filt + [{"field": {"uniqueName": field}, "include": [{"member": k}]}],
                                     used | {field}, c, depth))
            continue
        if bc + c > PAGE_ROWS:
            flush(); batch, bc = [], 0
        batch.append(k); bc += c
    flush()
    return out


def split_further(ix, fp, filt, used, count, depth):
    if depth < 3:
        tried = 0
        for f in fp["names"]:
            if f in used or f == fp["tf"] or fp["types"].get(f) != "string" or f.endswith(("_AR", "_ARAB")):
                continue
            tried += 1
            g = group_counts(ix, fp["names"][0], f, filt)
            if len(g) > 1 and max(c for _, c in g) < count and sum(c for _, c in g) == count:
                return plan_chunks(ix, fp, filt, f, g, used | {f}, depth + 1)
            if tried >= 4:
                break
    return [(filt, count)]  # لا حقل يقسمه: طلب متعدد الصفحات يُتحقق منه بسحب مزدوج


def fetch_all(ix, fp, members=None):
    """كل صفوف الفهرس (أو فترات members=[[قيمة، عدد]...]) بطلبات صفحة واحدة. يعيد الصفوف."""
    names, tf = fp["names"], fp["tf"]
    if tf:
        if members is None:
            members = [m for y in fp["years"].values() for m in y["members"]]
        plan = plan_chunks(ix, fp, [], tf, members, {tf})
    else:
        plan = [([], fp["rows"])] if fp["rows"] <= PAGE_ROWS else split_further(ix, fp, [], set(), fp["rows"], 0)
    want = sum(c for _, c in members) if tf else fp["rows"]
    if sum(c for _, c in plan) != want:
        raise RuntimeError(f"chunk plan covers {sum(c for _, c in plan)} of {want} rows (null keys?); not archived")
    rows = []
    for i, (filt, c) in enumerate(plan):
        got, pages = fetch_rows(ix, names, c, filt or None)
        if pages > 1:  # لم يتيسر تقسيمه: يُقبل فقط إن تطابق سحبان
            for attempt in range(3):
                again, _ = fetch_rows(ix, names, c, filt or None)
                if sorted(map(csv_line, got)) == sorted(map(csv_line, again)):
                    break
                got = again
            else:
                raise RuntimeError(f"unstable paging on a {c}-row chunk that cannot be split; not archived")
        rows += got
        if len(plan) > 20 and (i + 1) % 20 == 0:
            log(f"  {ix} chunk {i + 1}/{len(plan)}")
    return rows


def write_parts(od, stem, header, lines):
    """ملف واحد <stem>.csv.gz، وإن تجاوز الحد فأجزاء متساوية <stem>.p<n>.csv.gz (عددها يتضاعف حتى يتسع كل جزء).
    الصفوف مرتبة، فنفس المحتوى = نفس الملفات = نفس البصمات."""
    nparts = 1
    while True:
        size = -(-len(lines) // nparts) if lines else 0
        files = [f"{od}/{stem}" + (f".p{k}" if nparts > 1 else "") + ".csv.gz" for k in range(nparts)]
        for k, f in enumerate(files):
            write_gz(f + ".part", header, lines[k * size:(k + 1) * size])
        if nparts >= max(1, len(lines)) or all(os.path.getsize(f + ".part") <= SPLIT_BYTES for f in files):
            for f in files:
                os.replace(f + ".part", f)
            return files
        for f in files:
            os.remove(f + ".part")
        nparts *= 2


def register(files, fp, day, ix):
    """سطر manifest لكل ملف؛ history_manifest يحذف الملف المطابق بالبصمة لإصدار محفوظ. يعيد (مضافة، بايتات)."""
    added, nbytes = [], 0
    url = f"{B}select index={ix} (all fields, all pages)"
    for f in files:
        size = os.path.getsize(f)
        res = subprocess.run([sys.executable, "scripts/history_manifest.py", "gastat_db", day, f, url] +
                             ([fp["tf"]] if fp["tf"] else []), capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"history_manifest failed for {f}: {res.stderr.strip()[-300:]}")
        if res.stdout.startswith("added"):
            added.append(f); nbytes += size
    return added, nbytes


def pull_years(ix, fp, day, s, budget_left):
    """الفهارس الثقيلة (> HEAVY_ROWS): سنة بسنة بمرشح على حقل الزمن، والسنوات التي تغيّرت بصمتها فقط.
    كل سنة تُحفظ فور سحبها (<INDEX>.y<سنة>.csv.gz) وتُسجَّل في year_hash، فالتوقف عند الميزانية آمن ويكمل التشغيل التالي.
    budget_left(minutes, mb) → False إن لم يتسع. يعيد (مضافة، بايتات، صفوف، اكتمل؟)."""
    names = fp["names"]; header = csv_line(names)
    yh = s.setdefault("year_hash", {})
    for y in [y for y in yh if y not in fp["years"]]:
        log(f"  {ix} year {y} no longer at source (kept in archive)"); yh.pop(y)
    todo = sorted(y for y, v in fp["years"].items() if yh.get(y) != v["hash"])
    added, nbytes, n, od = [], 0, 0, None
    for y in todo:
        v = fp["years"][y]
        need = v["rows"] * len(names) / CELLS_PER_MIN; est = v["rows"] * BYTES_PER_ROW / 1e6
        if not budget_left(need, est + nbytes / 1e6):
            log(f"  {ix} stop before year {y} (~{need:.0f} min, run budget); continues next run")
            return added, nbytes, n, False
        rows = fetch_all(ix, fp, v["members"])
        lines = sorted(csv_line(h) for h in rows); del rows
        if od is None:
            od = snapshot_dir(ix, day); os.makedirs(od, exist_ok=True)
        a, b = register(write_parts(od, f"{ix}.y{y}", header, lines), fp, day, ix)
        added += a; nbytes += b; n += len(lines); yh[y] = v["hash"]
        log(f"  {ix} year {y}: {len(lines)} rows, {'new' if a else 'unchanged'}")
    if od:
        try: os.rmdir(od)
        except OSError: pass
    return added, nbytes, n, True


def pull(ix, fp, day, s):
    """سحب كل الحقول وكل الصفحات، ترتيب الصفوف ترتيبًا ثابتًا (الخادم يعيدها بترتيب متغير فتتغير البصمة بلا تغيّر
    المحتوى)، ثم ملف واحد أو ملفات حسب السنة إن تجاوز الحد، ثم manifest. يعيد (ملفات مضافة، بايتات، صفوف)."""
    names = fp["names"]; header = csv_line(names)
    ti = names.index(fp["tf"]) if fp["tf"] in names else None
    raw_lines = fetch_all(ix, fp)
    lines = [csv_line(h) for h in raw_lines]
    years = [year_of(h[ti]) if ti is not None else "all" for h in raw_lines]
    del raw_lines
    n = len(lines)
    order = sorted(range(n), key=lines.__getitem__)
    lines = [lines[i] for i in order]; years = [years[i] for i in order]
    # نفس محتوى أي لقطة مؤرشفة (ولو بترتيب صفوف مختلف) = لا جديد، لا يُكتب شيء
    csha = content_hash(header, lines)
    known = known_contents(ix, s)
    if csha in known:
        return [], 0, n
    od = snapshot_dir(ix, day); os.makedirs(od, exist_ok=True)
    single = f"{od}/{ix}.csv.gz"
    write_gz(single + ".part", header, lines)
    if os.path.getsize(single + ".part") <= SPLIT_BYTES:
        os.replace(single + ".part", single); finals = [single]
    else:  # احتياط لفهرس خفيف كبير الحجم: ملفات حسب السنة
        os.remove(single + ".part"); by, finals = {}, []
        for ln, y in zip(lines, years):
            by.setdefault(y, []).append(ln)
        for y, ls in sorted(by.items()):
            finals += write_parts(od, f"{ix}.y{y}", header, ls)
        log(f"  {ix} split into {len(finals)} files by year")
    added, nbytes = register(finals, fp, day, ix)
    try: os.rmdir(od)
    except OSError: pass
    if added or len(finals) > 1:
        s["cshas"] = sorted(set(known) | {csha})
    return added, nbytes, n


# ---------- الحالة والمراجع ----------
def load_inventory():
    inv = {}
    with open(INV, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            ix = r["flexmonster_index"]
            e = inv.setdefault(ix, {"ok": False, "freq": r["frequency"], "tf": r["time_field"]})
            e["ok"] = e["ok"] or r["cloud_ok_2026_09_28"] == "True"
            if FREQ_ORDER.get(r["frequency"], 9) < FREQ_ORDER.get(e["freq"], 9):  # فهرس لعدة تقارير: أسرعها تكرارًا
                e["freq"] = r["frequency"]
    return inv


def load_manifest():
    """أحدث سطر manifest لكل فهرس من الهيئة (أي مصدر gastat*، ومنه مجلدات gastat_cpi وgastat_pop القديمة)."""
    last = {}
    with open(MANIFEST, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            m = re.search(r"index=(\S+)", r["url"])
            if not r["source"].startswith("gastat") or not m:
                continue
            ix = m.group(1)
            if ix not in last or (r["download_date"], r["file"]) >= (last[ix]["download_date"], last[ix]["file"]):
                last[ix] = r
    return last


def load_state():
    if os.path.exists(STATE):
        with open(STATE, encoding="utf-8") as fh:
            return json.load(fh)
    return {"_meta": {}, "indexes": {}}


def save_state(st):
    """سطر لكل فهرس (فروقات git مقروءة)، ومفاتيح مرتبة (التشغيل المكرر لا يغيّر الملف)."""
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    ix = st["indexes"]
    body = ",\n".join(f"  {json.dumps(k)}: {json.dumps(ix[k], ensure_ascii=False, sort_keys=True)}" for k in sorted(ix))
    txt = ("{\n" + f'"_meta": {json.dumps(st["_meta"], ensure_ascii=False, sort_keys=True)},\n"indexes": {{\n'
           + body + "\n}\n}\n")
    tmp = STATE + ".part"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(txt)
    os.replace(tmp, STATE)


def seed(st, inv, man):
    """كل فهرس في الحصر له سطر، وما أُرشف قبل هذه الأداة يُربط بآخر سطر manifest."""
    for ix in sorted(set(inv) | set(man)):
        e = inv.get(ix, {"ok": True})
        s = st["indexes"].setdefault(ix, {"status": "pending" if e["ok"] else "broken", "last_checked": "",
                                          "last_period": "", "last_sha": "", "error": ""})
        if ix in man and not s.get("last_sha") and not s.get("last_checked"):  # ما أُرشف قبل هذه الأداة فقط
            m = man[ix]
            s.update(last_sha=m["sha256"], last_file=m["file"], rows=int(m["rows"] or 0), last_changed=m["download_date"])
            if s["status"] in ("pending", "broken") and not s.get("last_checked"):
                s["status"] = "ok"  # مؤرشف فعلًا = مقروء (الحصر أقدم من السحب)


def due_today(ix, e, s, today, force):
    if force or e.get("freq") in DAILY_FREQ or not s.get("last_checked"):
        return True
    age = (today - datetime.date.fromisoformat(s["last_checked"])).days
    # السنوية: يوم ثابت في الأسبوع لكل فهرس (يوزّع الحمل)، أو إن مضى أسبوع
    return age >= 7 or int(hashlib.md5(ix.encode()).hexdigest(), 16) % 7 == today.weekday()


def remember_sig(s, sig):
    s["sigs"] = (s.get("sigs", []) + [sig])[-10:] if sig not in s.get("sigs", []) else s["sigs"]


# ---------- الأنماط ----------
def run(args):
    global SLEEP
    SLEEP = args.sleep
    os.chdir(ROOT)
    t0 = time.time(); today = datetime.date.today(); day = today.isoformat()
    inv, man, st = load_inventory(), load_manifest(), load_state()
    seed(st, inv, man)
    S = st["indexes"]
    only = set(filter(None, (args.only or "").split(",")))
    C = {"checked": 0, "unchanged": 0, "new": 0, "same_sha": 0, "broken": 0, "recovered": 0, "errors": 0,
         "skipped_budget": 0, "too_big": 0, "partial": 0}
    mb = 0.0

    def out_of_budget():
        return (time.time() - t0) / 60 > args.max_minutes or mb > args.max_mb

    def record_error(ix, s, e):
        C["errors"] += 1
        s.update(status="error", error=str(e)[:300], fails=s.get("fails", 0) + 1, last_checked=day)
        warn(f"gastat_db {ix}: {str(e)[:200]}")

    def budget_left(need_min, est_mb):
        left = args.max_minutes - (time.time() - t0) / 60
        return need_min <= left and (mb + est_mb <= args.max_mb or mb == 0)

    def do_pull(ix, s, fp):
        nonlocal mb
        if fp["rows"] > HEAVY_ROWS and fp["tf"] and set(fp["years"]) != {"na"}:
            added, nbytes, n, done = pull_years(ix, fp, day, s, budget_left)
            mb += nbytes / 1e6
            s.update(heavy=True, last_checked=day, error="", fails=0)
            if added:
                C["new"] += 1; s["last_changed"] = day; s["last_file"] = os.path.dirname(added[0]) + "/"
                log(f"NEW  {ix} (by year) files={len(added)} {nbytes/1e6:.1f}MB")
            if not done:
                C["partial"] += 1; s.update(status="partial", rows=fp["rows"]); return
            remember_sig(s, fp["sig"])
            s.update(status="ok", sig=fp["sig"], last_period=fp["last_period"], first_period=fp["first_period"],
                     n_periods=fp["n_periods"], rows=fp["rows"],
                     last_sha="years:" + hashlib.sha256(json.dumps(s["year_hash"], sort_keys=True).encode()).hexdigest()[:16])
            if not added:
                C["same_sha"] += 1; log(f"SAME {ix} (by year) nothing new stored")
            return
        for attempt in range(3):  # نسختان عند المصدر: حقول جديدة ثم سحب جديد (حتى 3 مرات للفهارس دون مليون صف)
            try:
                added, nbytes, n = pull(ix, fp, day, s); break
            except MixedVersions as ex:
                if attempt == 2 or fp["rows"] > 1_000_000:
                    raise
                log(f"  {ix}: {str(ex)[:80]} → retry {attempt + 2}/3 in 30s")
                time.sleep(30); fp = fingerprint(ix, fp["tf"])
        mb += nbytes / 1e6
        remember_sig(s, fp["sig"])
        s.update(status="ok", error="", fails=0, sig=fp["sig"], last_period=fp["last_period"],
                 first_period=fp["first_period"], n_periods=fp["n_periods"], rows=n, last_checked=day)
        if added:
            C["new"] += 1
            s.update(last_changed=day, last_file=added[0] if len(added) == 1 else os.path.dirname(added[0]) + "/",
                     last_sha=sha_of(added[0]) if len(added) == 1 else "multi:" + str(len(added)))
            for rx, cmd in NORMALIZERS:
                if rx.match(ix):
                    # يُحفظ في الحالة فورًا: لو قُطع التشغيل قبل الموحِّد يشغّله التشغيل التالي
                    pend = st["_meta"].setdefault("normalize_pending", [])
                    c = [x.replace("{file}", added[0]) for x in cmd] + [ix]
                    if c not in pend: pend.append(c)
            log(f"NEW  {ix} rows={n} last={fp['last_period']} files={len(added)} {nbytes/1e6:.1f}MB")
        else:
            C["same_sha"] += 1
            log(f"SAME {ix} rows={n} (re-pulled, same content as last snapshot → nothing stored)")

    if args.mode == "update":
        targets = [ix for ix in sorted(S) if S[ix].get("last_sha") and S[ix]["status"] not in ("broken", "too_big")]
        if only: targets = [ix for ix in targets if ix in only]
        targets.sort(key=lambda ix: (S[ix].get("rows", 0) > HEAVY_ROWS, ix))  # الخفيفة أولًا
        for ix in targets:
            s, e = S[ix], inv.get(ix, {})
            if not due_today(ix, e, s, today, args.all or bool(only)):
                continue
            if out_of_budget():
                C["skipped_budget"] += 1; continue
            C["checked"] += 1
            try:
                fp = fingerprint(ix, e.get("tf"))
                if s.get("sig") == fp["sig"] or fp["sig"] in s.get("sigs", []):
                    # بصمة معروفة (الحالية أو نسخة سابقة أُرشف محتواها) = لا جديد
                    C["unchanged"] += 1; s.update(last_checked=day, status="ok", error="", fails=0)
                    if s.get("sig") != fp["sig"]:
                        s["flips"] = s.get("flips", 0) + 1; s["sig"] = fp["sig"]
                        log(f"FLIP {ix} source returned an earlier known version (flips={s['flips']})")
                    continue
                m = man.get(ix)
                if not s.get("sig") and m and m["file"] == s.get("last_file") and int(m["rows"] or -1) == fp["rows"] \
                        and norm_period(m["last_date"]) == norm_period(fp["last_period"]) and fp["rows"] <= PAGE_ROWS:
                    # أول فحص لفهرس مؤرشف سابقًا وهو مطابق: اعتمد البصمة بلا سحب. فوق صفحة واحدة لا: السحب القديم متعدد
                    # الصفحات قد يكون فيه تكرار وسقوط (ترتيب الخادم غير ثابت)، فيُعاد سحبه مرة بالطريقة المقسمة
                    C["unchanged"] += 1; remember_sig(s, fp["sig"])
                    s.update(sig=fp["sig"], last_period=fp["last_period"], first_period=fp["first_period"],
                             n_periods=fp["n_periods"], last_checked=day, status="ok", error="", fails=0)
                    log(f"BASE {ix} rows={fp['rows']} last={fp['last_period']} (matches manifest, no pull)")
                    continue
                log(f"CHG  {ix} sig {s.get('sig', '-')}→{fp['sig']} rows {s.get('rows', '-')}→{fp['rows']} "
                    f"last {s.get('last_period') or '-'}→{fp['last_period']}")
                need = fp["rows"] * len(fp["names"]) / CELLS_PER_MIN
                if fp["rows"] <= HEAVY_ROWS and not budget_left(need, fp["rows"] * BYTES_PER_ROW / 1e6):
                    C["skipped_budget"] += 1; log(f"DEFER {ix} ~{need:.0f} min (run budget; next run)"); continue
                do_pull(ix, s, fp)
            except NotFound as ex:
                C["broken"] += 1; s.update(status="broken", error=str(ex)[:300], last_checked=day)
                warn(f"gastat_db {ix} became unreadable: {ex}")
            except Exception as ex:
                record_error(ix, s, ex)
            save_state(st)

    elif args.mode == "backfill":
        targets = [ix for ix in S if not S[ix].get("last_sha") and S[ix]["status"] in ("pending", "ok", "error", "partial")]
        if args.resume:
            targets = [ix for ix in targets if S[ix]["status"] != "error"]
        # قرار إدارة البرمجة (29 سبتمبر 2026): فهارس التجارة الخارجية الثقيلة (~1.25 ج.ب) خارج السحب التاريخي
        # لأنها «غير مطلوبة الآن» في الحصر، حفاظًا على حجم المستودع. للتضمين: GASTAT_BACKFILL_SKIP="" أو --only.
        _skip = [x for x in os.environ.get("GASTAT_BACKFILL_SKIP", "DPV_EXP_,DPV_IMP_,DPV_TRD_").split(",") if x]
        if _skip and not only: targets = [ix for ix in targets if not any(ix.startswith(x) for x in _skip)]
        if only: targets = [ix for ix in targets if ix in only]
        # الأخطاء السابقة والثقيلة المعروفة في آخر الطابور حتى لا يعلق التشغيل على فهرس واحد
        targets.sort(key=lambda ix: (S[ix]["status"] == "error", bool(S[ix].get("heavy")),
                                     FREQ_ORDER.get(inv.get(ix, {}).get("freq"), 4), ix))
        if args.limit: targets = targets[:args.limit]
        heavy = []
        for ix in targets:
            if out_of_budget():
                C["skipped_budget"] += len(targets) - C["checked"]; break
            s = S[ix]; C["checked"] += 1
            try:
                fp = fingerprint(ix, inv.get(ix, {}).get("tf"))
                if HEAVY_ROWS < fp["rows"] <= HARD_CAP_ROWS:
                    s.update(heavy=True, rows=fp["rows"]); heavy.append((ix, fp)); continue  # الجولة الثانية
                est = fp["rows"] * BYTES_PER_ROW / 1e6
                if fp["rows"] > HARD_CAP_ROWS:
                    C["too_big"] += 1
                    s.update(status="too_big", rows=fp["rows"], error=f"{fp['rows']} rows > {HARD_CAP_ROWS} (~{est:.0f}MB)",
                             last_checked=day)
                    warn(f"gastat_db {ix}: ~{est:.0f}MB, skipped (needs a decision)"); continue
                need = fp["rows"] * len(fp["names"]) / CELLS_PER_MIN
                if not budget_left(need, est):
                    C["skipped_budget"] += 1; log(f"DEFER {ix} ~{need:.0f} min ~{est:.0f}MB (run budget)"); continue
                log(f"PULL {ix} rows={fp['rows']} last={fp['last_period']} ~{est:.1f}MB")
                do_pull(ix, s, fp)
            except NotFound as ex:
                C["broken"] += 1; s.update(status="broken", error=str(ex)[:300], last_checked=day)
            except Exception as ex:
                record_error(ix, s, ex)
            save_state(st)
        # الجولة الثانية: الثقيلة، سنة بسنة حتى تنفد الميزانية (ما بقي يكمل في التشغيل التالي)
        for ix, fp in heavy:
            if out_of_budget():
                C["skipped_budget"] += 1; continue
            s = S[ix]
            try:
                log(f"PULL {ix} rows={fp['rows']} years={len(fp['years'])} (heavy, by year)")
                do_pull(ix, s, fp)
            except NotFound as ex:
                C["broken"] += 1; s.update(status="broken", error=str(ex)[:300], last_checked=day)
            except Exception as ex:
                record_error(ix, s, ex)
            save_state(st)

    elif args.mode == "recheck-broken":
        targets = [ix for ix in sorted(S) if S[ix]["status"] == "broken"]
        if only: targets = [ix for ix in targets if ix in only]
        for ix in targets:
            if out_of_budget():
                C["skipped_budget"] += 1; continue
            s = S[ix]; C["checked"] += 1
            try:
                post("fields", {"type": "fields", "index": ix}, timeout=120, tries=3)
                C["recovered"] += 1
                s.update(status="ok" if s.get("last_sha") else "pending", error="", recovered_on=day, last_checked=day)
                warn(f"gastat_db {ix} is readable again → queued for backfill/update")
            except NotFound as ex:
                C["broken"] += 1; s.update(error=str(ex)[:300], last_checked=day)
            except Exception as ex:
                C["errors"] += 1; s.update(error=str(ex)[:300], last_checked=day)
        save_state(st)

    # الموحِّدات (ومنها ما بقي من تشغيل قُطع): مرة واحدة لكل أمر، وفشلها تحذير فقط ولا يُعاد تلقائيًا
    done = set()
    for c in st["_meta"].pop("normalize_pending", []):
        cmd, ix = tuple(c[:-1]), c[-1]
        if cmd in done: continue
        done.add(cmd)
        res = subprocess.run([sys.executable, *cmd], capture_output=True, text=True)
        if res.returncode == 0:
            log(f"NORM ok {cmd[0]} ({ix})")
        else:
            warn(f"normalizer {cmd[0]} failed after new {ix}: {(res.stderr or res.stdout).strip()[-300:]}")

    stat = {}
    for s in S.values():
        stat[s["status"]] = stat.get(s["status"], 0) + 1
    mins = (time.time() - t0) / 60
    summary = (f"SUMMARY gastat_db mode={args.mode} date={day} checked={C['checked']} unchanged={C['unchanged']} "
               f"new={C['new']} same_sha={C['same_sha']} recovered={C['recovered']} broken={C['broken']} "
               f"errors={C['errors']} too_big={C['too_big']} partial={C['partial']} deferred={C['skipped_budget']} added_mb={mb:.1f} "
               f"minutes={mins:.1f} | state: " + " ".join(f"{k}={v}" for k, v in sorted(stat.items())))
    if C["checked"]:  # تشغيل فارغ لا يغيّر الملف
        st["_meta"][args.mode] = {"date": day, "checked": C["checked"], "new": C["new"], "errors": C["errors"],
                                  "broken": C["broken"], "recovered": C["recovered"]}
    save_state(st)
    log(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as fh:
            fh.write(summary + "\n")
    # عطل شامل (كل ما فُحص فشل) = فشل ظاهر في GitHub؛ عطل فهرس واحد = تحذير فقط
    if C["checked"] >= 3 and C["errors"] == C["checked"]:
        sys.exit(2)


def sha_of(f):
    h = hashlib.sha256()
    with open(f, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mode", required=True, choices=["update", "backfill", "recheck-broken"])
    ap.add_argument("--limit", type=int, default=0, help="backfill: أقصى عدد فهارس في هذا التشغيل")
    ap.add_argument("--resume", action="store_true", help="backfill: تخطَّ الفهارس التي فشلت سابقًا (status=error)")
    ap.add_argument("--max-minutes", type=float, default=300, help="ميزانية الوقت؛ ما تبقى يكمل في التشغيل التالي")
    ap.add_argument("--max-mb", type=float, default=400, help="ميزانية حجم الخام الجديد في التشغيل (حجم commit)")
    ap.add_argument("--sleep", type=float, default=1.0, help="ثوانٍ بين الطلبات")
    ap.add_argument("--only", default="", help="فهارس محددة مفصولة بفواصل (للاختبار أو الإصلاح)")
    ap.add_argument("--all", action="store_true", help="update: افحص كل الفهارس اليوم (بلا جدولة السنوية)")
    _a = ap.parse_args()
    import collect_budget; collect_budget.apply('gastat_db', _a.mode, _a)  # ميزانية دقائق Actions (data/collect/budget.json)
    run(_a)
