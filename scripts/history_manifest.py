"""Append entries to raw/history/manifest.csv (never rewrites existing rows).
Usage: python3 scripts/history_manifest.py <source> <download_date> <file> <url> [date_col]"""
import sys, os, csv, hashlib, pandas as pd
src, dd, f, url = sys.argv[1:5]; dcol = sys.argv[5] if len(sys.argv)>5 else None
M='raw/history/manifest.csv'; H=['file','source','url','download_date','rows','first_date','last_date','sha256']
sha=hashlib.sha256(open(f,'rb').read()).hexdigest()
rows=first=last=''
if f.endswith(('.csv', '.csv.gz')):
    cols=list(pd.read_csv(f, encoding='utf-8-sig', nrows=0).columns)
    c = dcol if dcol in cols else next((x for x in ['starting_date','date_object','date','YEAR_MONTH'] if x in cols), None)
    # عمود واحد فقط في الذاكرة (ملفات الهيئة قد تبلغ ملايين الصفوف)؛ نفس الناتج
    d=pd.read_csv(f, encoding='utf-8-sig', usecols=[c] if c else [0]); rows=len(d)
    if c: first, last = str(d[c].min()), str(d[c].max())
new = not os.path.exists(M)
# LF فقط: سطر CRLF كان يُفشل فحص البصمة في collect-kapsarc (grep ",sha$") فتُحفظ نسخة مطابقة كل يوم.
if not new:
    raw=open(M,'rb').read()
    if b'\r\n' in raw: open(M,'wb').write(raw.replace(b'\r\n', b'\n'))  # تطبيع نهايات الأسطر فقط، بلا تغيير محتوى
    prev=[r for r in csv.DictReader(open(M, encoding='utf-8')) if r['sha256']==sha]
    if any(r['file']==f for r in prev): print('already', f); sys.exit()
    if prev and os.path.abspath(f)!=os.path.abspath(prev[0]['file']) and os.path.exists(prev[0]['file']):
        # نسخة مطابقة بالبصمة لإصدار محفوظ: لا تُكرَّر (الإصدار الأول باقٍ كما هو)
        os.remove(f); print('unchanged (same sha as', prev[0]['file'] + ')', f); sys.exit()
with open(M,'a',newline='',encoding='utf-8') as fh:
    w=csv.writer(fh, lineterminator='\n'); new and w.writerow(H); w.writerow([f,src,url,dd,rows,first,last,sha])
print('added', f, rows, first, last)
