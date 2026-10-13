#!/usr/bin/env python3
"""مراقب نبض البيانات — يفحص الموقع الحي كل ساعة بلا Claude وبلا أسرار (أمر المالك 13 أكتوبر 2026).

يفحص ما يراه العالم فعلًا، لا ما في الخادم:
  - النطاق: سجل A في DNS العام = خادمنا، وخوادم الأسماء ليست خوادم إيقاف المسجّل
    (حادثة 12 أكتوبر 2026: إيقاف ICANN حوّل النطاق إلى name-services.com يومين قبل أن نكتشفه).
  - الصفحات الأساسية = 200 وفيها محتوانا، وليست صفحة إيقاف.
  - robots.txt يسمح بالزحف ويشير إلى sitemap، وsitemap كامل.
  - ملف تحقق Search Console كما هو (حذفه يفقدنا ملكية الموقع في قوقل).
  - شهادة TLS لا تنتهي خلال 14 يومًا.

الناتج: سطر لكل فحص، وملف JSON بالنتيجة (site_watch.json) يقرؤه سير العمل ليفتح أو يغلق بلاغ «استنفار».
لا يُفشل نفسه: القرار لسير العمل.
"""
import json, socket, ssl, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone

DOMAIN = 'datapulseactive.com'
SERVER_IP = '188.117.84.73'                 # خادم سهارا sl32 (يطابق DNS Manager وcPanel)
PARKED_NS = ('name-services.com',)          # خوادم eNom الافتراضية = نطاق موقوف أو منقول
GSC_FILE = 'google610948a640751a8c.html'
GSC_BODY = 'google-site-verification: google610948a640751a8c.html'
MIN_SITEMAP = 336
MIN_TLS_DAYS = 14
UA = 'DataPulse-SiteWatch/1.0 (+https://datapulseactive.com/)'

results = []   # (ok, key, message_ar)


def add(ok, key, msg):
    results.append((ok, key, msg))
    print(('✅ ' if ok else '🔴 ') + msg, flush=True)


def doh(name, rtype):
    """DNS عبر قوقل وكلاودفلير (DoH) — ما يراه العالم، لا ما يراه الخادم."""
    out = {}
    for prov, url in (('google', f'https://dns.google/resolve?name={name}&type={rtype}'),
                      ('cloudflare', f'https://cloudflare-dns.com/dns-query?name={name}&type={rtype}')):
        try:
            req = urllib.request.Request(url, headers={'accept': 'application/dns-json', 'user-agent': UA})
            with urllib.request.urlopen(req, timeout=20) as r:
                j = json.load(r)
            out[prov] = sorted(a['data'].rstrip('.').lower() for a in j.get('Answer', []) if a.get('type') == (1 if rtype == 'A' else 2))
        except Exception as e:  # noqa: BLE001
            out[prov] = None
            print(f'   (DoH {prov} {rtype}: {e})')
    return out


def fetch(path, tries=3):
    """GET مع إعادة المحاولة؛ يعيد (status, body) أو (None, سبب الخطأ)."""
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(f'https://{DOMAIN}{path}', headers={'user-agent': UA, 'cache-control': 'no-cache'})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, r.read().decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            last = (e.code, '')
        except Exception as e:  # noqa: BLE001
            last = (None, str(e))
        if i < tries - 1:
            time.sleep(20)
    return last


# 1) النطاق
a = doh(DOMAIN, 'A')
seen = {ip for v in a.values() if v for ip in v}
if not any(v is not None for v in a.values()):
    add(False, 'dns-a', 'تعذّر فحص DNS العام (قوقل وكلاودفلير لم يردّا) — قد يكون خللًا مؤقتًا في الفحص نفسه')
elif seen == {SERVER_IP}:
    add(True, 'dns-a', f'النطاق يشير لخادمنا {SERVER_IP}')
else:
    add(False, 'dns-a', f'النطاق يشير إلى {", ".join(sorted(seen)) or "لا شيء"} بدل خادمنا {SERVER_IP} — الموقع لا يصل للناس ولا لقوقل')

ns = doh(DOMAIN, 'NS')
nss = {n for v in ns.values() if v for n in v}
if nss and any(n.endswith(p) for n in nss for p in PARKED_NS):
    add(False, 'dns-ns', f'خوادم أسماء النطاق صارت خوادم المسجّل ({", ".join(sorted(nss))}) — علامة إيقاف النطاق (تحقق ICANN أو تجديد)')
elif nss:
    add(True, 'dns-ns', f'خوادم الأسماء: {", ".join(sorted(nss))}')

# 2) الصفحات
st, body = fetch('/')
if st == 200 and 'نبض البيانات' in body:
    add(True, 'home', 'الرئيسية 200 وفيها محتوانا')
elif st == 200 and 'ICANN' in body:
    add(False, 'home', 'الرئيسية تعرض صفحة إيقاف ICANN — بريد مسجّل النطاق يحتاج تحقق')
else:
    add(False, 'home', f'الرئيسية لا تعمل كما يجب (الحالة: {st}{"؛ " + body[:120] if st is None else ""})')

for p, name in (('/privacy/', 'سياسة الخصوصية'), ('/terms/', 'شروط الاستخدام')):
    s, _ = fetch(p)
    add(s == 200, 'page' + p, f'{name} {s}' if s == 200 else f'{name} لا تعمل (الحالة: {s})')

# 3) robots وsitemap
s, rb = fetch('/robots.txt')
lines = [l.strip().lower() for l in (rb or '').splitlines()]
if s != 200:
    add(False, 'robots', f'robots.txt لا يُجلب (الحالة: {s}) — قوقل يوقف الزحف على الموقع كله حين يفشل جلبه')
elif 'disallow: /' in lines:
    add(False, 'robots', 'robots.txt يمنع الزحف على الموقع كله (Disallow: /)')
elif not any(l.startswith('sitemap:') for l in lines):
    add(False, 'robots', 'robots.txt بلا سطر Sitemap')
else:
    add(True, 'robots', 'robots.txt سليم')

s, sm = fetch('/sitemap.xml')
n = (sm or '').count('<loc>')
add(s == 200 and n >= MIN_SITEMAP, 'sitemap',
    f'sitemap.xml فيه {n} رابطًا' if s == 200 and n >= MIN_SITEMAP else f'sitemap.xml ناقص أو لا يعمل (الحالة: {s}، الروابط: {n}، المطلوب ≥ {MIN_SITEMAP})')

s, g = fetch('/' + GSC_FILE)
add(s == 200 and (g or '').strip() == GSC_BODY, 'gsc',
    'ملف تحقق Search Console سليم' if s == 200 and (g or '').strip() == GSC_BODY else f'ملف تحقق Search Console مفقود أو متغير (الحالة: {s}) — نخسر ملكية الموقع في قوقل')

# 4) الشهادة
try:
    ctx = ssl.create_default_context()
    with socket.create_connection((DOMAIN, 443), timeout=20) as sock, ctx.wrap_socket(sock, server_hostname=DOMAIN) as ss:
        exp = datetime.strptime(ss.getpeercert()['notAfter'], '%b %d %H:%M:%S %Y %Z').replace(tzinfo=timezone.utc)
    days = (exp - datetime.now(timezone.utc)).days
    add(days >= MIN_TLS_DAYS, 'tls', f'شهادة TLS صالحة {days} يومًا' if days >= MIN_TLS_DAYS else f'شهادة TLS تنتهي بعد {days} يومًا فقط ({exp:%Y-%m-%d}) — AutoSSL في cPanel لم يجددها')
except Exception as e:  # noqa: BLE001
    add(False, 'tls', f'تعذّر فحص شهادة TLS: {e}')

bad = [r for r in results if not r[0]]
report = {
    'at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'ok': not bad,
    'failed': [{'key': k, 'msg': m} for _, k, m in bad],
    'passed': [m for ok, _, m in results if ok],
    'signature': ','.join(sorted(k for _, k, _ in bad)),
}
json.dump(report, open('site_watch.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=1)

# نص البلاغ: وش صار، ووش المطلوب منك (إن وُجد) — بالعربي البسيط
OWNER_DO = {
    'domain': ('**المطلوب منك (5 دقائق):** افتح بريدك ali.ali.a1436@gmail.com وابحث عن رسالة عنوانها '
               '`IMMEDIATE VERIFICATION` من `name-services.com` (رسالة أصلية من الشركة الأم لنطاقات سهارا)، واضغط رابط التحقق فيها. '
               'إن لم تجدها: اتصل بسهارا على 8003040304 وقل «نطاق datapulseactive.com موقوف، أبغى رسالة التحقق تنعاد». '
               '**لا تغيّر خوادم الأسماء ولا DNS بنفسك.**'),
    'tls': ('**المطلوب منك (دقيقة):** cPanel ← ابحث عن **SSL/TLS Status** ← اضغط **Run AutoSSL**. '
            'إن بقي الخطأ: تذكرة لسهارا بأن AutoSSL لا يجدد شهادة datapulseactive.com.'),
}
domain_keys = {'dns-a', 'dns-ns'} | ({'home'} if any(k == 'home' and 'ICANN' in m for _, k, m in bad) else set())
keys = {k for _, k, _ in bad}
lines = ['## وش صار', ''] + [f'- 🔴 {m}' for _, _, m in bad] + ['', '## وش المطلوب', '']
if keys & domain_keys:
    lines.append(OWNER_DO['domain'])
elif 'tls' in keys and len(keys) == 1:
    lines.append(OWNER_DO['tls'])
else:
    lines.append('**ما عليك شيء الحين.** هذه مشكلة تقنية يتولاها فريق الهندسة. إن استمر هذا البلاغ مفتوحًا أكثر من ساعتين، اتصل بسهارا على 8003040304.')
lines += ['', '---', 'المراقب يعيد الفحص كل ساعة، ويُغلق هذا البلاغ تلقائيًا ويبلّغك حين يرجع كل شيء سليمًا.',
          'المرجع للفريق: claude/حادثة_النطاق_1012.md وقاعدة «حالة الاستنفار» في CLAUDE.md.',
          f'<!-- sig:{report["signature"]} -->']
open('site_watch_body.md', 'w', encoding='utf-8').write('\n'.join(lines) + '\n')
print(f'\nالنتيجة: {"سليم" if not bad else f"{len(bad)} مشكلة"}')
sys.exit(0)
