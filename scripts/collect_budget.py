# -*- coding: utf-8 -*-
"""ميزانية دقائق GitHub Actions للجامعين (قرار إدارة البرمجة 30 سبتمبر 2026).
الخطة المجانية للمستودع الخاص = 2,000 دقيقة شهريًا لكل المهام معًا (النشر والدمج والجمع). تجاوزها يوقف النشر نفسه.
الإعداد في data/collect/budget.json ويُعدَّل من GitHub مباشرة بلا تعديل ملفات .github:
  {"<جامع>": {"<وضع>": {"days": [0..6 حيث 0=الاثنين], "max_minutes": رقم}}}
يُطبَّق على التشغيل المجدول فقط (GITHUB_EVENT_NAME=schedule)؛ التشغيل اليدوي من Actions يأخذ ما يختاره المالك."""
import os, json, datetime as dt, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def apply(collector, mode, args, attr='max_minutes'):
    if os.environ.get('GITHUB_EVENT_NAME') != 'schedule':
        return
    try:
        cfg = json.load(open(os.path.join(ROOT, 'data', 'collect', 'budget.json'), encoding='utf-8'))
    except Exception:
        return
    c = (cfg.get(collector) or {}).get(mode)
    if not isinstance(c, dict):
        return
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=3))).weekday()
    if 'days' in c and today not in c['days']:
        print(f'SKIP budget: {collector} {mode} لا يعمل اليوم (data/collect/budget.json)')
        sys.exit(0)
    cap = c.get('max_minutes')
    if cap is not None:
        cur = getattr(args, attr, None)
        if cap <= 0:
            print(f'SKIP budget: {collector} {mode} max_minutes=0'); sys.exit(0)
        setattr(args, attr, min(cur, cap) if cur else cap)
        print(f'budget: {collector} {mode} ≤ {getattr(args, attr)} دقيقة')
