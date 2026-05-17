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
MIN_AVIS = 0
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
    "Capitale-Nationale":    ["Donnacona","Pont-Rouge","Saint-Raymond","Sainte-Anne-de-Beaupre","Montmagny-est"],
    "Mauricie-extra":        ["Saint-Tite","Louiseville","Yamachiche","Sainte-Anne-de-la-Perade"],
    "Lanaudiere-extra":      ["Saint-Lin","Lanoraie","Berthierville","Saint-Gabriel","Lavaltrie"],
    "Centre-du-Quebec-extra":["Kingsey-Falls","Warwick","Arthabaska","Daveluyville"],
}

MOTS_CLES = [
    "atelier+usinage",
    "fabrication+metallique",
    "usinage+soudure",
    "machine+shop+Quebec",
    "reparation+mecanique+industrielle",
    "services+industriels+usinage",
]

def villes_non_scrapees():
    return [v for villes in VILLES_DISPO.values() for v in villes if v not in VILLES_FAITES]

PRIORITE_VILLES = [
    # Nouvelles régions non épuisées
    "Donnacona", "Pont-Rouge", "Saint-Raymond",
    "Saint-Tite", "Louiseville", "Yamachiche",
    "Saint-Lin", "Lanoraie", "Berthierville", "Lavaltrie",
    "Kingsey-Falls", "Warwick", "Arthabaska",
    "Buckingham", "Maniwaki", "Thurso",
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

def domaine_site(site):
    """Extrait le domaine racine du site (ex: lotus-design.ca)."""
    try:
        h = site.split("//")[-1].split("/")[0].lower()
        return re.sub(r"^www\.", "", h)
    except:
        return ""

def trouver_email(site):
    if not site: return ""
    if not site.startswith("http"): site = "https://" + site
    dom = domaine_site(site)
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
    # Filtrer : l'email doit appartenir au même domaine que le site
    if dom:
        emails_valides = [e for e in emails if e.split("@")[-1] == dom]
        if emails_valides:
            emails_valides.sort(key=score)
            return emails_valides[0]
    # Fallback sans filtre domaine si aucun email correspondant
    emails.sort(key=score)
    return emails[0]

# Catégories Google Maps hors-domaine → lead rejeté
CATEGORIES_EXCLUES = {
    "couvreur","toiture","roofing","peintre","plombier","plomberie","electricien",
    "electricite","chauffage","climatisation","hvac","restauration","restaurant",
    "epicerie","boulangerie","coiffeur","salon","nettoyage","demenagement",
    "assurance","comptable","avocat","notaire","pharmacie","clinique","medecin",
    "dentiste","optique","garderie","ecole","agence immobiliere","hotel","motel",
    "detaillant","boutique","vetement","sport","loisir","tourisme",
    "cloisons seches","cloison","drywall","paysagiste",
    "excavation","beton","ciment","poids lourds","camion","transport",
}

# Mots dans le NOM de l'entreprise → rejet automatique peu importe la catégorie GMap
# Utiliser des mots complets uniquement (regex \b) pour éviter les faux positifs
# ex: "abri" ne doit pas matcher "fabrication"
NOMS_EXCLUS = [
    r"\btoiture",r"\bcouvreur",r"\broofing",r"\brevetement",r"\bexterieur",
    r"\bsiding\b",r"\bbardage\b",r"\bcloison\b",r"\bdrywall\b",
    r"\bpaysage\b",r"\bpaysagiste\b",r"\bexcavation\b",r"\bbeton\b",
    r"\bciment\b",r"\bcabanon\b",r"\babri\b",r"\becocentre\b",r"\bécocentre\b",
    r"\brecyclage\b",r"\bmateriaux\brusses\b",
]
_NOMS_EXCLUS_RE = re.compile("|".join(NOMS_EXCLUS))

def _norm(s):
    """Lowercase + retire accents pour comparaison."""
    s = (s or "").lower()
    for a, b in [("é","e"),("è","e"),("ê","e"),("ë","e"),("à","a"),("â","a"),
                 ("ô","o"),("ù","u"),("û","u"),("ü","u"),("î","i"),("ï","i"),("ç","c")]:
        s = s.replace(a, b)
    return s

def categoriser(categorie_gmap, nom):
    """Retourne (type_atelier, hors_domaine). hors_domaine=True si le lead doit être rejeté."""
    c = _norm(categorie_gmap)
    n = _norm(nom)
    texte = c + " " + n

    # Vérifier si hors-domaine via catégorie GMap
    for exclu in CATEGORIES_EXCLUES:
        if exclu in c:
            return None, True
    # Vérifier si hors-domaine via le nom de l'entreprise
    if _NOMS_EXCLUS_RE.search(n):
        return None, True

    # Correspondances précises basées sur la catégorie GMap d'abord
    CAT_MAP = {
        "atelier d'usinage":               "atelier d'usinage",
        "usinage":                         "atelier d'usinage",
        "machine shop":                    "atelier d'usinage",
        "atelier de soudure":              "atelier de soudure",
        "soudure":                         "atelier de soudure",
        "welding":                         "atelier de soudure",
        "decoupe laser":                   "atelier de decoupe laser",
        "laser":                           "atelier de decoupe laser",
        "fabrication metallique":          "atelier de fabrication metallique",
        "fabrication de metal":            "atelier de fabrication metallique",
        "metal fabricator":                "atelier de fabrication metallique",
        "tolerie":                         "atelier de tolerie industrielle",
        "sheet metal":                     "atelier de tolerie industrielle",
        "polissage":                       "atelier de polissage et traitement de surface",
        "chromage":                        "atelier de polissage et traitement de surface",
        "placage":                         "atelier de polissage et traitement de surface",
        "traitement de surface":           "atelier de polissage et traitement de surface",
        "hydraulique":                     "service hydraulique industriel",
        "pneumatique":                     "service hydraulique industriel",
        "decoupe":                         "atelier de decoupe industrielle",
        "estampage":                       "atelier d'estampage et emboutissage",
        "emboutissage":                    "atelier d'estampage et emboutissage",
        "fonderie":                        "fonderie et moulage",
        "moulage":                         "fonderie et moulage",
        "injection plastique":             "atelier d'usinage plastique",
        "plastique":                       "atelier d'usinage plastique",
        "mecanique industrielle":          "atelier de mecanique industrielle",
        "mecanique generale":              "atelier de mecanique industrielle",
        "transmission":                    "atelier de mecanique et transmission",
        "aluminium":                       "fabricant et distributeur aluminium",
        "acier":                           "distributeur et transformateur acier",
        "metal":                           "atelier de fabrication metallique",
        "precision":                       "atelier d'usinage de precision",
        "cnc":                             "atelier d'usinage CNC",
        "tournage":                        "atelier d'usinage - tournage/fraisage",
        "fraisage":                        "atelier d'usinage - tournage/fraisage",
        "services industriels":            "services industriels specialises",
        "industrie":                       "atelier de fabrication industrielle",
        "equipement industriel":           "fournisseur d'equipement industriel",
        "maintenance industrielle":        "maintenance et reparation industrielle",
        "fabricant de machines":           "fabricant de machines industrielles",
        "construction de machines":        "fabricant de machines industrielles",
        "fabricant":                       "atelier de fabrication metallique",
        "manufacture de metal":            "atelier de fabrication metallique",
        "acieriste":                       "distributeur et transformateur acier",
        "ingenieur en mecanique":          "services d'ingenierie mecanique",
        "ingenierie":                      "services d'ingenierie mecanique",
        "entreprise de construction":      "atelier de fabrication et construction",
        "entrepreneur":                    "entrepreneur en fabrication metallique",
    }
    for key, val in CAT_MAP.items():
        if key in c:
            return val, False

    # Fallback sur le nom si catégorie inconnue
    if "polissage" in texte or "chromage" in texte:  return "atelier de polissage et traitement de surface", False
    if "laser" in texte:                              return "atelier de decoupe laser", False
    if "plastique" in texte:                          return "atelier d'usinage plastique", False
    if "soudure" in texte and "usinage" in texte:     return "atelier d'usinage et soudure", False
    if "soudure" in texte or "soudage" in texte or "welding" in texte: return "atelier de soudure", False
    if "hydrauli" in texte:                           return "service hydraulique industriel", False
    if "transmission" in texte:                       return "atelier de mecanique et transmission", False
    if "tolerie" in texte or "toleri" in texte:       return "atelier de tolerie industrielle", False
    if "aluminium" in texte or "aluminum" in texte:   return "fabricant et distributeur aluminium", False
    if "acier" in texte or "steel" in texte:          return "distributeur et transformateur acier", False
    if "precision" in texte:                          return "atelier d'usinage de precision", False
    if "usinage" in texte or "machining" in texte or "machine shop" in texte: return "atelier d'usinage", False
    if "fabrication" in texte or "industrie" in texte or "metal" in texte: return "atelier de fabrication metallique", False
    if "mecanique" in texte:                          return "atelier de mecanique industrielle", False

    # Si on n'a aucun indice, on retourne un type générique mais pas "mécanique"
    return "entreprise de fabrication et transformation", False

def extraire_fiche(page):
    d = {}
    for sel in ["h1.DUwDvf","h1.fontHeadlineLarge","h1"]:
        try:
            t = page.locator(sel).first.text_content(timeout=3000).strip()
            if t: d["nom"] = t; break
        except: pass
    d.setdefault("nom","")
    # Catégorie Google Maps (bouton sous le nom)
    d["categorie_gmap"] = ""
    try:
        d["categorie_gmap"] = page.locator("button.DkEaL").first.text_content(timeout=2000).strip()
    except: pass
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
    if r.status_code in (200, 201):
        cid = (r.json().get("contact") or r.json()).get("id")
    elif r.status_code == 400:
        # Contact dupliqué — GHL retourne l'ID existant dans meta
        cid = r.json().get("meta", {}).get("contactId")
        if not cid:
            return None
    else:
        return None
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
                # Catégoriser et filtrer les hors-domaine
                type_atelier, hors_domaine = categoriser(d.get("categorie_gmap",""), d["nom"])
                if hors_domaine:
                    print(f"  ✗  {d['nom'][:50]}  (hors-domaine: {d.get('categorie_gmap','')})")
                    continue
                email = trouver_email(d.get("siteWeb",""))
                if not email:
                    print(f"  ✗  {d['nom'][:50]}  (pas d'email)")
                    continue
                if email in emails_existants:
                    print(f"  ✗  {d['nom'][:50]}  (email dupliqué)")
                    continue
                d["emailTrouve"]  = email
                d["type_atelier"] = type_atelier
                d["dateCollecte"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                cid = pousser_ghl(d)
                if not cid:
                    print(f"  ✗  {d['nom'][:50]}  (erreur GHL)")
                    continue
                emails_existants.add(email)
                leads_ajoutes.append(d)
                print(f"  ✓  [{len(leads_ajoutes):02}/{OBJECTIF}] {d['nom'][:35]:<35}  {d['type_atelier'][:30]:<30}  {email}")
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
