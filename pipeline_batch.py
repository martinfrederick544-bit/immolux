"""
SYNC CRM — Pipeline batch complet
Scrape Google Maps -> email -> GHL jusqu'à OBJECTIF leads qualifiés
"""

import json, re, time, os, sys
from pathlib import Path
from urllib.parse import unquote
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

# ── CONFIG ────────────────────────────────────────────────────────────────────
OBJECTIF = int(sys.argv[1]) if len(sys.argv) > 1 else 30
MIN_AVIS = 3
SEARCHES = []  # rempli dynamiquement ou passé en argument

KEY      = "pit-f9b378fe-1099-4bb3-8e92-9cbdfe48721b"
LOC      = "0DkpRaoTql6L7OixzlfA"
BASE     = "https://services.leadconnectorhq.com"
PIPE_ID  = "mmQ9dqGd4YCQGfs4xWou"
STAGE_ID = "087d4d96-2c63-4672-9276-896ec229e20f"
CF_TYPE  = "FhopcNZMbXfGV2OtWQbs"
TAGS     = ["usinage-qc", "outreach-j1", "source-sync-ai"]
GHL_H    = {
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Version": "2021-07-28",
}

# ── VILLES DÉJÀ SCRAPÉES ─────────────────────────────────────────────────────
VILLES_FAITES = {
    "Saint-Georges-Beauce","Sainte-Marie-Beauce","Scott-Beauce","Valleyfield",
    "L'Assomption","Louiseville","La Tuque","Windsor QC","Acton-Vale",
    "Quebec City","Jonquiere","Laval","Rive-Nord Montreal","Sainte-Anne-des-Plaines",
    "Rawdon","Estrie","Roberval","Saint-Felicien","Drummondville","Granby",
    "Saint-Hyacinthe","Shawinigan","Victoriaville","Thetford-Mines","Sorel-Tracy",
    "Joliette","Terrebonne","Rive-Sud Montreal","Varennes","Saint-Jerome","Lachute",
    "Rouyn-Noranda","Val-d-Or","Sherbrooke","Alma","Saguenay","Chicoutimi",
    "Trois-Rivieres","Saint-Eustache","Mirabel","Repentigny","Blainville","Mascouche",
    "Mont-Laurier","Longueuil","Boucherville","Sainte-Julie","Chambly","Rimouski",
    "Riviere-du-Loup","Sept-Iles","Baie-Comeau","Dolbeau-Mistassini","Magog",
    "Coaticook","LaSalle Montreal","Saint-Laurent Montreal","Anjou Montreal",
    "Riviere-des-Prairies","Pointe-Claire","Vaudreuil-Dorion","Chateauguay","Candiac",
    "Sainte-Therese","Boisbriand","Deux-Montagnes","Levis","Beauport","Sainte-Foy",
    "Black Lake","Plessisville","Princeville","Sainte-Croix","Portneuf","Deschambault",
    "Becancour","Nicolet","Yamaska","Saint-Pie","Waterloo","Bedford","Dunham",
}

# ── VILLES DISPONIBLES PAR RÉGION ─────────────────────────────────────────────
VILLES_DISPO = {
    "Abitibi-Temiscamingue": ["Amos","La Sarre","Senneterre","Malartic"],
    "Cote-Nord":             ["Forestville","Havre-Saint-Pierre","Port-Cartier"],
    "Gaspesie":              ["Matane","Gaspe","Chandler","New-Richmond","Amqui","Sainte-Anne-des-Monts"],
    "Bas-Saint-Laurent":     ["Pohenegamook","Montmagny","Kamouraska","La Pocatiere","Cabano"],
    "Outaouais":             ["Gatineau","Hull","Buckingham","Maniwaki","Thurso","Papineauville"],
    "Monteregie":            ["Iberville","Bromont","Cowansville","Farnham","Beloeil","McMasterville",
                              "Saint-Bruno","Saint-Constant","Sainte-Catherine","Delson","Saint-Luc","Napierville"],
    "Laurentides":           ["Saint-Donat","Prevost","Sainte-Adele","Morin-Heights","Saint-Sauveur","Lac-des-Iles"],
    "Chaudiere-Appalaches":  ["Montmagny","Saint-Jean-Port-Joli","Saint-Pamphile","Saint-Anselme","Sainte-Henedine"],
}

MOTS_CLES = [
    "atelier+usinage",
    "fabrication+metallique",
    "usinage+soudure",
    "machine+shop+Quebec",
]

def villes_non_scrapees():
    return [v for villes in VILLES_DISPO.values() for v in villes if v not in VILLES_FAITES]

PRIORITE_VILLES = [
    # Grandes villes en premier (plus de résultats)
    "Gatineau", "Hull", "Bromont", "Beloeil", "Saint-Bruno",
    "Cowansville", "Farnham", "Iberville", "Saint-Constant",
    "Sainte-Adele", "Prevost", "Matane", "Amos", "Gaspe",
]

def generer_searches(n_villes=6):
    dispo = villes_non_scrapees()
    # Priorité aux grandes villes
    triees = [v for v in PRIORITE_VILLES if v in dispo] + [v for v in dispo if v not in PRIORITE_VILLES]
    villes = triees[:n_villes]
    urls = []
    for ville in villes:
        slug = ville.replace(" ", "+")
        for mot in MOTS_CLES[:2]:
            urls.append(f"https://www.google.com/maps/search/{mot}+{slug}")
    return urls

# ── ÉTAT EXISTANT ─────────────────────────────────────────────────────────────
LEADS_FILE = Path("leads/leads_enriched.json")
LEADS_FILE.parent.mkdir(exist_ok=True)
if not LEADS_FILE.exists():
    LEADS_FILE.write_text("[]")

with open(LEADS_FILE) as f:
    existants = json.load(f)

noms_existants   = set(re.sub(r"[^a-z0-9]","", (l.get("nom","") or "").lower()) for l in existants)
emails_existants = set(l.get("emailTrouve","") for l in existants if l.get("emailTrouve"))

# ── HELPERS ───────────────────────────────────────────────────────────────────
HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "fr-CA",
}
RE_EMAIL = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE)
EXCLUS   = {
    "sentry.io","example.com","wix.com","wordpress.com","protection","placeholder",
    ".png",".jpg",".svg","domain","noreply","no-reply","donotreply",
    "yoursite","yourdomain","test.com","info@info","email@email",
    "admin@admin","mail@mail",
}

def scraper_emails(url):
    try:
        r = requests.get(url, headers=HEADERS_HTTP, timeout=8, verify=False, allow_redirects=True)
        if r.status_code != 200: return []
        soup = BeautifulSoup(r.text, "html.parser")
        emails = set()
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                e = a["href"].replace("mailto:","").split("?")[0].strip().lower()
                if RE_EMAIL.match(e): emails.add(e)
        for m in RE_EMAIL.finditer(soup.get_text(" ")):
            emails.add(m.group(0).lower())
        return [e for e in emails if not any(x in e for x in EXCLUS)]
    except:
        return []

def trouver_email(site):
    if not site: return ""
    if not site.startswith("http"): site = "https://" + site
    def score(e):
        for p in ["info","contact","courriel","admin","vente","service","reception","accueil"]:
            if e.startswith(p+"@"): return 0
        return 1
    emails = scraper_emails(site)
    if not emails:
        for c in ["/contact","/nous-joindre","/contactez-nous","/contact-us"]:
            emails = scraper_emails(site.rstrip("/")+c)
            if emails: break
    if not emails: return ""
    emails.sort(key=score)
    return emails[0]

def detecter_type(nom):
    n = (nom or "").lower()
    if "polissage" in n or "powdercoat" in n: return "atelier de polissage et revetement poudre"
    if "decoupe" in n or "decoupage" in n:    return "atelier de decoupage industriel"
    if "laser" in n:                           return "atelier de decoupe laser"
    if "plastique" in n:                       return "atelier d'usinage plastique"
    if "soudure" in n and "usinage" in n:      return "atelier d'usinage et soudure"
    if "soudure" in n or "soudage" in n:       return "atelier de soudure"
    if "transmission" in n:                    return "atelier de mecanique et transmission"
    if "mecanique" in n and "pneu" not in n:   return "atelier de mecanique industrielle"
    if "garage" in n or "pneu" in n:           return "atelier mecanique auto"
    if "precision" in n:                       return "atelier d'usinage de precision"
    if any(k in n for k in ["usinage","machine shop","machinerie"]): return "atelier d'usinage"
    if "fabrication" in n or "industrie" in n: return "atelier de fabrication metallique"
    return "atelier de fabrication mecanique"

def extraire_fiche(page):
    d = {}
    for sel in ["h1.DUwDvf","h1.fontHeadlineLarge","h1"]:
        try:
            t = page.locator(sel).first.text_content(timeout=3000).strip()
            if t: d["nom"] = t; break
        except: pass
    d.setdefault("nom","")
    d["note"], d["nbAvis"] = None, 0
    try:
        bloc = page.locator("div.F7nice").first.text_content(timeout=3000)
        m1 = re.search(r"([\d]+[,\.][\d]+)", bloc)
        m2 = re.search(r"\(([\d\s ]+)\)", bloc)
        if m1: d["note"] = float(m1.group(1).replace(",","."))
        if m2: d["nbAvis"] = int(re.sub(r"\D","",m2.group(1)))
    except: pass
    if d["nbAvis"] == 0:
        try:
            aria = page.locator("[aria-label*='avis'],[aria-label*='reviews']").first.get_attribute("aria-label", timeout=2000)
            if aria:
                m = re.search(r"([\d,\.]+)\s+[ee]toile", aria)
                if m: d["note"] = float(m.group(1).replace(",","."))
                m2 = re.search(r"([\d\s]+)\s+avis", aria)
                if m2: d["nbAvis"] = int(re.sub(r"\D","",m2.group(1)))
        except: pass
    d["tel"] = d["adresse"] = d["siteWeb"] = ""
    try:
        t = page.locator('button[data-item-id^="phone:tel:"]').first.text_content(timeout=2000).strip()
        m = re.search(r"[\d\s\-\(\)\+]+", t)
        d["tel"] = m.group(0).strip() if m else t
    except: pass
    try:
        d["adresse"] = page.locator('button[data-item-id="address"]').first.text_content(timeout=2000).strip()
    except: pass
    try:
        href = page.locator('a[data-item-id="authority"]').first.get_attribute("href", timeout=2000) or ""
        if "/url?q=" in href:
            m = re.search(r"/url\?q=([^&]+)", href)
            if m: href = unquote(m.group(1))
        d["siteWeb"] = href
    except: pass
    d["urlMaps"] = page.url
    return d

def pousser_ghl(lead):
    nom = lead.get("nom","")
    tel = re.sub(r"[^\d+]", "", lead.get("tel","") or "")
    payload = {
        "locationId": LOC,
        "companyName": nom,
        "firstName": nom,
        "email": lead["emailTrouve"],
        "website": lead.get("siteWeb","") or "",
        "address1": lead.get("adresse","") or "",
        "tags": TAGS,
        "source": "sync-pipeline",
        "customFields": [{"id": CF_TYPE, "value": lead["type_atelier"]}],
    }
    if tel: payload["phone"] = tel
    r = requests.post(f"{BASE}/contacts/", headers=GHL_H, json=payload, timeout=15)
    if r.status_code not in (200, 201):
        return None
    cid = (r.json().get("contact") or r.json()).get("id")
    time.sleep(0.4)
    requests.post(f"{BASE}/opportunities/", headers=GHL_H, json={
        "pipelineId": PIPE_ID,
        "locationId": LOC,
        "name": nom,
        "pipelineStageId": STAGE_ID,
        "status": "open",
        "contactId": cid,
        "monetaryValue": 0,
    }, timeout=15)
    return cid

# ── MAIN ──────────────────────────────────────────────────────────────────────
searches = SEARCHES if SEARCHES else generer_searches(n_villes=8)
leads_ajoutes = []

print(f"\n{'='*58}")
print(f"  SYNC CRM — Objectif : {OBJECTIF} leads")
print(f"  URLs à scraper : {len(searches)}")
print(f"{'='*58}")

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=["--lang=fr-CA","--no-sandbox","--disable-dev-shm-usage"],
    )
    ctx = browser.new_context(
        locale="fr-CA",
        viewport={"width": 1440, "height": 900},
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        ignore_https_errors=True,
    )
    page = ctx.new_page()

    for search_url in searches:
        if len(leads_ajoutes) >= OBJECTIF:
            break
        ville = search_url.split("+")[-1].replace("%20"," ")
        print(f"\n{'─'*58}")
        print(f"  {ville}  —  {len(leads_ajoutes)}/{OBJECTIF} leads")
        print(f"{'─'*58}")
        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            print(f"  ERR navigation : {e}")
            continue
        page.wait_for_timeout(6000)
        for btn in ["Tout accepter","Accept all","Accepter tout","J'accepte"]:
            try:
                b = page.locator(f'button:has-text("{btn}")')
                if b.count() > 0:
                    b.first.click(timeout=2000)
                    page.wait_for_timeout(3000)
                    break
            except: pass
        # Attendre que les résultats apparaissent
        try:
            page.locator('div[role="article"]').first.wait_for(timeout=8000)
        except: pass
        for _ in range(20):
            try:
                panneau = page.locator('div[role="feed"]').first
                panneau.wait_for(timeout=4000)
                if page.locator("a.hfpxzc").count() >= 40: break
                if page.locator('span:has-text("fin de la liste")').count(): break
                panneau.evaluate("el => el.scrollTo(0, el.scrollHeight)")
                page.wait_for_timeout(2000)
            except: break
        urls = []
        for c in page.locator("a.hfpxzc").all():
            try:
                href = c.get_attribute("href") or ""
                if "/maps/place/" in href and href not in urls:
                    urls.append(href)
            except: pass
        print(f"  {len(urls)} fiches trouvées")
        for url in urls:
            if len(leads_ajoutes) >= OBJECTIF: break
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
                page.wait_for_timeout(2800)
                d = extraire_fiche(page)
                if not d.get("nom"): continue
                nom_norm = re.sub(r"[^a-z0-9]","", (d["nom"] or "").lower())
                if nom_norm in noms_existants: continue
                noms_existants.add(nom_norm)
                if d["nbAvis"] < MIN_AVIS: continue
                email = trouver_email(d.get("siteWeb",""))
                if not email:
                    print(f"  ✗  {d['nom'][:50]}  (pas d'email)")
                    continue
                if email in emails_existants:
                    print(f"  ✗  {d['nom'][:50]}  (email dupliqué)")
                    continue
                d["emailTrouve"]  = email
                d["type_atelier"] = detecter_type(d["nom"])
                d["dateCollecte"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                cid = pousser_ghl(d)
                if not cid:
                    print(f"  ✗  {d['nom'][:50]}  (erreur GHL)")
                    continue
                emails_existants.add(email)
                leads_ajoutes.append(d)
                print(f"  ✓  [{len(leads_ajoutes):02}/{OBJECTIF}] {d['nom'][:40]:<40}  {email}")
                time.sleep(0.3)
            except Exception as e:
                print(f"  ERR  {str(e)[:60]}")

    browser.close()

with open(LEADS_FILE) as f:
    tous = json.load(f)
tous.extend(leads_ajoutes)
with open(LEADS_FILE,"w") as f:
    json.dump(tous, f, ensure_ascii=False, indent=2)

print(f"\n{'='*58}")
print(f"  OK  {len(leads_ajoutes)}/{OBJECTIF} leads ajoutés dans GHL")
print(f"  Total cumulatif : {len(tous)} leads")
print(f"{'='*58}\n")
