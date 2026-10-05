from __future__ import annotations
import csv, json, os, re, time, urllib.request, urllib.error, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DATE="2026-10-05"
OUT=Path(f"n8n-workflows-free-{DATE}")
WF=OUT/"workflows"; META=OUT/"metadata"
WF.mkdir(parents=True, exist_ok=True); META.mkdir(parents=True, exist_ok=True)
ROWS=100
WORKERS=24
UA={"Accept":"application/json,text/plain,*/*","User-Agent":"n8n-catalog-personal-export/1.0"}

def get_json(url, tries=5, timeout=30):
    last=None
    for a in range(tries):
        try:
            req=urllib.request.Request(url,headers=UA)
            with urllib.request.urlopen(req,timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last=e
            if isinstance(e,urllib.error.HTTPError) and e.code not in (429,) and e.code<500:
                raise
            time.sleep(min(20,2**a))
    raise last

def search_page(page):
    urls=[
      f"https://n8n.io/api/product-api/workflows/search?rows={ROWS}&page={page}",
      f"https://api.n8n.io/templates/search?rows={ROWS}&page={page}",
    ]
    last=None
    for u in urls:
        try: return get_json(u),u
        except Exception as e: last=e
    raise last

def normalize_search(d):
    c=[]
    if isinstance(d,dict):
        data=d.get("data") if isinstance(d.get("data"),dict) else {}
        for x in (d.get("workflows"),data.get("workflows"),data.get("results"),d.get("results"),d.get("items"),data.get("items")):
            if isinstance(x,list): c=x; break
        total=d.get("totalWorkflows") or data.get("totalWorkflows") or d.get("total") or data.get("total")
        try: total=int(total) if total is not None else None
        except: total=None
        return c,total
    return [],None

def wid(item):
    for v in (item.get("id"),item.get("workflowId"),(item.get("workflow") or {}).get("id") if isinstance(item.get("workflow"),dict) else None):
        try:
            n=int(v)
            if n>0:return n
        except: pass
    return None

def name(item):
    return str(item.get("name") or item.get("title") or ((item.get("workflow") or {}).get("name") if isinstance(item.get("workflow"),dict) else "") or "Untitled")

def price_num(v):
    if v is None or v is False or v=="": return 0
    if isinstance(v,(int,float)): return float(v)
    if isinstance(v,dict):
        return max([price_num(v.get(k)) for k in ("amount","value","price")]+[0])
    s=str(v).strip().lower()
    if s in ("free","0","$0","€0","£0","null","none",""):return 0
    m=re.search(r"(\d+(?:[.,]\d+)?)",s)
    return float(m.group(1).replace(",",".")) if m else 0

def paid_info(item):
    if not isinstance(item,dict): return False,None,None
    p=item.get("pricing") if isinstance(item.get("pricing"),dict) else {}
    a=item.get("attributes") if isinstance(item.get("attributes"),dict) else {}
    w=item.get("workflow") if isinstance(item.get("workflow"),dict) else {}
    price=max([price_num(v) for v in (item.get("price"),p.get("price"),p.get("amount"),item.get("amount"),w.get("price"),a.get("price"))]+[0])
    purchase=item.get("purchaseUrl") or item.get("purchase_url") or item.get("checkoutUrl") or item.get("checkout_url") or p.get("purchaseUrl") or a.get("purchaseUrl")
    flags=(item.get("paid"),item.get("isPaid"),item.get("is_paid"),p.get("paid"),a.get("paid"))
    explicit=any(v is True or str(v).lower()=="true" for v in flags)
    typ=str(item.get("pricingType") or p.get("type") or item.get("type") or "").lower()
    return (explicit or typ=="paid" or price>0 or bool(purchase)),(price or None),purchase

def slug(s):
    s=re.sub(r"[^A-Za-z0-9_-]+","-",s).strip("-").lower()
    return (s[:100] or "workflow")

def detail(workflow_id):
    urls=[f"https://api.n8n.io/api/workflows/{workflow_id}",f"https://api.n8n.io/templates/workflows/{workflow_id}"]
    last=None
    for u in urls:
        try:return get_json(u),u
        except Exception as e:
            last=e
            if isinstance(e,urllib.error.HTTPError) and e.code in (402,403): raise
    raise last

def unpack(d):
    data=d.get("data") if isinstance(d,dict) and isinstance(d.get("data"),dict) else {}
    attrs=data.get("attributes") if isinstance(data.get("attributes"),dict) else (d.get("attributes") if isinstance(d,dict) and isinstance(d.get("attributes"),dict) else data)
    if not isinstance(attrs,dict): attrs={}
    workflow=attrs.get("workflow") or (d.get("workflow") if isinstance(d,dict) else None) or data.get("workflow")
    nm=attrs.get("name") or (d.get("name") if isinstance(d,dict) else None) or data.get("name")
    status=attrs.get("status") or (d.get("status") if isinstance(d,dict) else None) or data.get("status")
    return attrs,workflow,nm,status

items={}
reported=None
page=1
while True:
    data,endpoint=search_page(page)
    arr,total=normalize_search(data)
    if total is not None: reported=total
    if not arr: break
    before=len(items)
    for it in arr:
        if isinstance(it,dict):
            i=wid(it)
            if i: items[i]=it
    print(f"page={page} count={len(arr)} unique={len(items)} total={reported}",flush=True)
    if reported and len(items)>=reported:break
    if len(arr)<ROWS and not reported:break
    if len(items)==before and page>2: break
    page+=1
    if page>10000:break

free=[]; paid=[]
for i,it in items.items():
    isp,pr,pu=paid_info(it)
    rec={"id":i,"name":name(it),"source_url":f"https://n8n.io/workflows/{i}","price":pr,"purchase_url":pu,"search_metadata":it}
    (paid if isp else free).append(rec)

def proc(c):
    i=c["id"]
    try:
        d,ep=detail(i)
        attrs,w,nm,status=unpack(d)
        isp,pr,pu=paid_info(attrs)
        if isp:
            return {"kind":"paid","id":i,"name":nm or c["name"],"price":pr,"purchase_url":pu,"source_url":c["source_url"]}
        if status and str(status).lower()!="published":
            return {"kind":"skipped","id":i,"name":nm or c["name"],"status":str(status),"source_url":c["source_url"]}
        if not isinstance(w,dict) or not isinstance(w.get("nodes"),list) or not isinstance(w.get("connections"),dict):
            raise RuntimeError("No importable workflow object")
        fn=f"{i}-{slug(nm or c['name'])}.json"
        (WF/fn).write_text(json.dumps(w,ensure_ascii=False,indent=2),encoding="utf-8")
        meta={"id":i,"name":nm or c["name"],"status":"downloaded","nodes":len(w["nodes"]),"source_url":c["source_url"],"workflow_file":f"workflows/{fn}","detail_endpoint":ep}
        (META/f"{i}.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        return {"kind":"downloaded",**meta}
    except urllib.error.HTTPError as e:
        return {"kind":"restricted" if e.code in (402,403) else "failed","id":i,"name":c["name"],"http_status":e.code,"source_url":c["source_url"],"error":str(e)}
    except Exception as e:
        return {"kind":"failed","id":i,"name":c["name"],"source_url":c["source_url"],"error":str(e)}

results=[]
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    fs=[ex.submit(proc,c) for c in free]
    for n,f in enumerate(as_completed(fs),1):
        results.append(f.result())
        if n%100==0 or n==len(fs):print(f"processed={n}/{len(fs)}",flush=True)

for r in results:
    if r["kind"]=="paid": paid.append(r)
paid=list({x["id"]:x for x in paid}.values())
downloaded=sorted([r for r in results if r["kind"]=="downloaded"],key=lambda x:x["id"])
errors=[r for r in results if r["kind"]!="downloaded" and r["kind"]!="paid"]

index=[{k:r.get(k) for k in ("id","name","nodes","source_url","workflow_file")} for r in downloaded]
(OUT/"index.json").write_text(json.dumps(index,ensure_ascii=False,indent=2),encoding="utf-8")
with (OUT/"index.csv").open("w",newline="",encoding="utf-8-sig") as f:
    w=csv.DictWriter(f,fieldnames=["id","name","nodes","source_url","workflow_file"]);w.writeheader();w.writerows(index)
(OUT/"paid_templates.json").write_text(json.dumps(sorted(paid,key=lambda x:x["id"]),ensure_ascii=False,indent=2),encoding="utf-8")
with (OUT/"paid_templates.csv").open("w",newline="",encoding="utf-8-sig") as f:
    w=csv.DictWriter(f,fieldnames=["id","name","price","source_url","purchase_url"],extrasaction="ignore");w.writeheader();w.writerows(sorted(paid,key=lambda x:x["id"]))
(OUT/"errors.json").write_text(json.dumps(errors,ensure_ascii=False,indent=2),encoding="utf-8")
manifest={"generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"catalog_reported":reported,"discovered":len(items),"downloaded_free_published":len(downloaded),"paid_skipped":len(paid),"errors_or_other_skips":len(errors)}
(OUT/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
(OUT/"README.txt").write_text("Personal export of public FREE n8n workflow templates. Paid/restricted templates are not included. Review every workflow before activation.\n",encoding="utf-8")

zp=Path(f"{OUT.name}.zip")
with zipfile.ZipFile(zp,"w",zipfile.ZIP_DEFLATED,compresslevel=6) as z:
    for p in OUT.rglob("*"):
        if p.is_file():z.write(p,arcname=f"{OUT.name}/{p.relative_to(OUT)}")
print(json.dumps(manifest),flush=True)
print(f"ZIP={zp} size={zp.stat().st_size}",flush=True)
