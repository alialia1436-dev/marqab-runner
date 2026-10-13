# بيانات الهيئة العامة للإحصاء — نسخة خام مفتوحة

جمع آلي يومي للملفات **كما نشرتها الهيئة العامة للإحصاء** (stats.gov.sa وقاعدة البيانات الإحصائية database.stats.gov.sa)، يشغّله موقع [نبض البيانات](https://datapulseactive.com).

- **المصدر الرسمي:** الهيئة العامة للإحصاء، المملكة العربية السعودية. الملفات تحت `raw/history/` نسخ غير معدّلة كما نُشرت (ملفات القاعدة مرتبة الصفوف ومضغوطة gzip فقط، بلا تغيير في القيم).
- **الاستخدام:** وفق [سياسة الاستخدام لدى الهيئة](https://www.stats.gov.sa/en/use-policy): اذكر الهيئة مصدرًا رسميًا، وبيّن أي تعديل تجريه، ولا تقدّم المعدَّل على أنه إحصاء رسمي. وإذا كان لمؤشر سياسة خاصة على صفحته فهي المقدَّمة. لا يشمل ذلك أي مادة لطرف ثالث داخل منشورات الهيئة.
- **لا علاقة رسمية:** نبض البيانات غير تابع للهيئة ولا معتمد منها، ولا يُستخدم شعار الهيئة.
- **السجل:** `raw/history/manifest.csv` (الملف، الرابط، تاريخ التنزيل، البصمة sha256).
- **الكود** في `scripts/`: © 2026 نبض البيانات، جميع الحقوق محفوظة.

---

Raw, unmodified copies of files published by the General Authority for Statistics (GASTAT), Saudi Arabia, collected daily. Source: GASTAT (official). Reuse under the [GASTAT Use Policy](https://www.stats.gov.sa/en/use-policy). Not affiliated with or endorsed by GASTAT. Scripts © 2026 DataPulse, all rights reserved.

---

## مراقب الموقع (site-watch)
`scripts/site_watch.py` + `.github/workflows/site-watch.yml`: فحص خارجي كل ساعة لـdatapulseactive.com (النطاق في DNS العام، الصفحات الأساسية، robots.txt، sitemap، ملف تحقق Search Console، شهادة TLS). عند أي مشكلة يُفتح بلاغ «🔴 استنفار» في هذا المستودع فيصل المالك بريد من GitHub، ويُغلق تلقائيًا عند الرجوع. بلا أسرار وبلا كود موقع (أمر المالك 13 أكتوبر 2026 بعد حادثة النطاق 12 أكتوبر).
