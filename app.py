import os
import io
import re
import csv
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests
from flask import Flask, request, send_file, render_template_string, abort

app = Flask(__name__)

# =========================
# CONFIG
# =========================
APP_VERSION = "6.1.3-site"
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")
USER_AGENT = os.environ.get("USER_AGENT", f"rota-privada-web/{APP_VERSION}")
COUNTRY = "Brazil"

SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.65"))
MAX_SECONDS_PER_ADDRESS = float(os.environ.get("MAX_SECONDS_PER_ADDRESS", "15"))
MAX_TOTAL_ADDRESSES = int(os.environ.get("MAX_TOTAL_ADDRESSES", "500"))

# Trava opcional global de bairro
BAIRRO_FIXO = os.environ.get("BAIRRO_FIXO", "").strip()

# Manaus
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)

# Cache local
GEOCODE_CACHE_FILE = "geocode_cache.csv"
VIACEP_CACHE_FILE = "viacep_cache.csv"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# =========================
# HTML
# =========================
HTML = f"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <title>Roteirizador Privado v{APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    body{{font-family:Arial,sans-serif;background:#fff;margin:0;padding:0}}
    .wrap{{max-width:950px;margin:38px auto;padding:0 16px}}
    h1{{text-align:center;margin:0 0 18px}}
    .badge{{font-size:12px;background:#eee;border-radius:999px;padding:3px 8px;vertical-align:middle}}
    .card{{border:1px solid #ddd;border-radius:12px;padding:18px;box-shadow:0 2px 10px rgba(0,0,0,.03)}}
    .hint{{font-size:13px;color:#444;line-height:1.5}}
    .row{{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-top:12px}}
    input[type=password]{{padding:10px;border:1px solid #ccc;border-radius:10px;width:260px}}
    button{{padding:10px 16px;border:0;border-radius:10px;background:#111;color:#fff;font-weight:bold;cursor:pointer}}
    button:disabled{{opacity:.5;cursor:not-allowed}}
    textarea{{width:100%;min-height:360px;margin-top:12px;border:1px solid #ccc;border-radius:10px;padding:12px;font:13px Consolas,monospace;resize:vertical}}
    .small{{font-size:12px;color:#666;margin-top:8px}}
  </style>
</head>
<body>
<div class="wrap">
  <h1>Roteirizador Privado <span class="badge">v{APP_VERSION}</span></h1>

  <div class="card">
    <div class="hint">
      Cole a lista bagunçada.<br>
      Formatos aceitos:
      <br>• nome + endereço + CEP em linhas separadas
      <br>• endereço + CEP
      <br>• rua / bairro / CEP
      <br>• CEP com ou sem <b>CEP:</b>
      <br><br>
      Se a geocodificação ficar duvidosa, cai em fallback e marca em <b>Notes</b>.
    </div>

    <form method="post" action="/process">
      <div class="row">
        <label><b>Senha:</b></label>
        <input name="password" type="password" placeholder="APP_PASSWORD" required>
        <button type="submit">Gerar CSV</button>
      </div>

      <label style="display:block;margin-top:14px"><b>Cole aqui:</b></label>
      <textarea name="text" placeholder="Cole aqui sua lista bagunçada..." required></textarea>
    </form>

    <div class="small">
      Saída: <b>circuit_import_site.csv</b> (download direto)
    </div>
  </div>
</div>
</body>
</html>
"""

# =========================
# HELPERS
# =========================
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def formata_cep(s: str) -> str:
    d = only_digits(s)
    return f"{d[:5]}-{d[5:]}" if len(d) == 8 else (s or "").strip()

def normaliza(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def dentro_de_manaus(lat: float, lon: float) -> bool:
    west, south, east, north = MANAUS_VIEWBOX
    return west <= lon <= east and south <= lat <= north

def limpar_texto(s: str) -> str:
    s = (s or "").replace("\u00a0", " ").strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extrair_numero(endereco: str) -> str:
    m = re.search(r"\b(\d{1,5}[A-Za-z]?)\b", re.sub(r"\b\d{8}\b", " ", endereco or ""))
    return m.group(1) if m else "S/N"

def canonicaliza_via(s: str) -> str:
    s = limpar_texto(s)
    s = re.sub(r"^\s*R\b\.?\s*", "Rua ", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*AV\b\.?\s*", "Avenida ", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*TV\b\.?\s*", "Travessa ", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*TRAV\b\.?\s*", "Travessa ", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*BCO\b\.?\s*", "Beco ", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*EST\b\.?\s*", "Estrada ", s, flags=re.IGNORECASE)
    return limpar_texto(s)

def force_rua_if_rio(raw: str) -> str:
    n = normaliza(raw)
    if n.startswith("rio ") and ("rua" not in n):
        return "Rua " + raw
    return raw

def similaridade(a: str, b: str) -> float:
    import difflib
    a = normaliza(a)
    b = normaliza(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

# =========================
# CACHE
# =========================
def load_cache_csv(path: str) -> Dict[str, Dict[str, str]]:
    out = {}
    if not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                k = (row.get("key") or "").strip()
                if k:
                    out[k] = row
    except Exception:
        return {}
    return out

def save_cache_csv(path: str, fieldnames: List[str], rows: Dict[str, Dict[str, str]]) -> None:
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for _, row in rows.items():
                w.writerow(row)
    except Exception:
        pass

# =========================
# VIACEP
# =========================
def viacep_get(cep_digits: str, viacep_cache: Dict[str, Dict[str, str]]) -> Optional[Dict[str, str]]:
    cep_digits = only_digits(cep_digits)
    if len(cep_digits) != 8:
        return None

    if cep_digits in viacep_cache:
        return viacep_cache[cep_digits]

    url = f"https://viacep.com.br/ws/{cep_digits}/json/"
    try:
        r = SESSION.get(url, timeout=12)
        if r.status_code != 200:
            return None
        j = r.json()
        if "erro" in j:
            return None

        data = {
            "key": cep_digits,
            "cep": (j.get("cep") or "").strip(),
            "logradouro": (j.get("logradouro") or "").strip(),
            "bairro": (j.get("bairro") or "").strip(),
            "localidade": (j.get("localidade") or "").strip(),
            "uf": (j.get("uf") or "").strip(),
            "updated_at": now_iso(),
        }
        viacep_cache[cep_digits] = data
        return data
    except Exception:
        return None

# =========================
# NOMINATIM
# =========================
def nominatim_search(query: str, bounded: bool, limit: int, timeout_s: float) -> List[Dict[str, Any]]:
    url = "https://nominatim.openstreetmap.org/search"
    west, south, east, north = MANAUS_VIEWBOX
    params = {
        "q": query,
        "format": "json",
        "limit": int(limit),
        "countrycodes": "br",
        "addressdetails": 1,
    }
    if bounded:
        params["viewbox"] = f"{west},{north},{east},{south}"
        params["bounded"] = 1

    try:
        r = SESSION.get(url, params=params, timeout=max(3.0, timeout_s))
        time.sleep(SLEEP_NOMINATIM)
        if r.status_code != 200:
            return []
        return r.json() or []
    except Exception:
        return []

def resultado_e_lugar_ruim(item):
    cls = (item.get("class") or "").lower()
    typ = (item.get("type") or "").lower()
    display = (item.get("display_name") or "").lower()

    ruins_cls = {"waterway", "natural"}
    ruins_typ = {"river", "stream", "canal", "lake", "reservoir", "bay", "wetland"}

    if cls in ruins_cls:
        return True
    if typ in ruins_typ:
        return True
    if any(w in display for w in [" rio ", " igarapé", " igarape", " lago ", " canal "]):
        if cls in ruins_cls or typ in ruins_typ:
            return True
    return False

def pega_bairro_do_resultado(item):
    addr = item.get("address") or {}
    cand = (
        addr.get("suburb")
        or addr.get("neighbourhood")
        or addr.get("city_district")
        or addr.get("district")
        or ""
    )
    return (cand or "").strip()

def bairro_bate(bairro_esperado, item):
    if not bairro_esperado:
        return True
    b = normaliza(bairro_esperado)
    if not b:
        return True

    bairro_resultado = pega_bairro_do_resultado(item)
    b_res = normaliza(bairro_resultado) or normaliza(item.get("display_name", ""))

    if b in b_res:
        return True
    if "mestri" in b and "mestri" in b_res:
        return True
    if similaridade(bairro_esperado, bairro_resultado) >= 0.62:
        return True
    return False

def escolher_melhor_candidato(candidatos, rua_raw, bairro_esperado):
    melhor = (None, None, -1.0, None)
    for c in candidatos:
        try:
            lat = float(c.get("lat"))
            lon = float(c.get("lon"))
        except Exception:
            continue

        if not dentro_de_manaus(lat, lon):
            continue
        if resultado_e_lugar_ruim(c):
            continue
        if not bairro_bate(bairro_esperado, c):
            continue

        display = c.get("display_name", "") or ""
        score = similaridade(rua_raw, display)

        cls = (c.get("class") or "").lower()
        typ = (c.get("type") or "").lower()
        if cls in ("highway", "building", "amenity"):
            score += 0.08
        if typ in ("residential", "road", "house", "yes"):
            score += 0.08

        if score > melhor[2]:
            melhor = (lat, lon, score, c)

    if melhor[0] is not None:
        return melhor[0], melhor[1], melhor[2], melhor[3]
    return None, None, 0.0, None

# =========================
# PARSER ROBUSTO (CEP COMO ÂNCORA)
# =========================
def extrair_cep_any(texto: str):
    if not texto:
        return None
    m = re.search(r"(?:\bcep\b\s*[:\-]?\s*)?(\d{5})[-\s]?(\d{3})", texto, flags=re.I)
    if not m:
        return None
    return (m.group(1) + m.group(2)).strip()

def linha_tem_via(texto: str):
    t = (texto or "").lower().strip()

    if re.search(r"(^|\s)(r\.?|rua)\b", t):
        return True
    if re.search(r"\b(av\.?|avenida)\b", t):
        return True
    if re.search(r"\b(tv\.?|travessa)\b", t):
        return True
    if re.search(r"\b(beco|estrada|alameda|praça|praca|rodovia|rio)\b", t):
        return True

    tem_numero = bool(re.search(r",\s*\d{1,6}\b", texto or ""))
    tem_manaus = "manaus" in t
    return tem_numero and tem_manaus

def is_linha_nome(s: str):
    if not s:
        return False
    if extrair_cep_any(s):
        return False
    if linha_tem_via(s):
        return False
    if re.search(r"\d", s):
        return False

    bad = ["pacote", "logistica", "ltda", "pedido", "tracking", "codigo", "shpx", "ml", "bli_"]
    ns = s.lower()
    if any(b in ns for b in bad):
        return False

    toks = [t for t in s.split() if t.strip()]
    return len(toks) >= 2

def extrair_bairro_de_endereco(endereco: str):
    if not endereco:
        return ""
    partes = [p.strip() for p in re.split(r"\s-\s", endereco) if p.strip()]
    if len(partes) >= 2:
        bairro = partes[1].strip()
        bairro = re.sub(r"\b(manaus|am|amazonas)\b", "", bairro, flags=re.I).strip()
        return bairro
    return ""

def strip_cep_text(s: str):
    s = re.sub(r"\bCEP\b\s*:?\s*\d{5}-?\d{3}\b", "", s, flags=re.I)
    s = re.sub(r"\b\d{5}-?\d{3}\b", "", s)
    s = re.sub(r"\b\d{8}\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_entregas(texto_colado: str):
    linhas = [limpar_texto(l) for l in (texto_colado or "").splitlines()]
    linhas = [l for l in linhas if l.strip()]

    entregas = []

    for i, linha in enumerate(linhas):
        cep = extrair_cep_any(linha)
        if not cep:
            continue

        nome = ""
        endereco = ""
        bairro = ""

        anteriores = []
        for j in range(max(0, i - 3), i):
            if linhas[j].strip():
                anteriores.append(linhas[j].strip())

        # tenta endereço na linha mais próxima do CEP
        for cand in reversed(anteriores):
            if linha_tem_via(cand) or re.search(r",\s*\d", cand) or re.search(r"\d{1,6}", cand):
                endereco = cand
                break

        # se não achou endereço, tenta usar a própria linha do CEP
        if not endereco and linha_tem_via(linha):
            endereco = strip_cep_text(linha)

        # tenta bairro separado
        if anteriores:
            for cand in reversed(anteriores):
                if cand != endereco and not is_linha_nome(cand) and not extrair_cep_any(cand):
                    if len(cand.split()) <= 5 and not linha_tem_via(cand):
                        bairro = cand
                        break

        # tenta nome
        for cand in anteriores:
            if cand != endereco and cand != bairro and is_linha_nome(cand):
                nome = cand
                break

        # tenta bairro a partir do endereço
        if not bairro and endereco:
            bairro = extrair_bairro_de_endereco(endereco)

        # se tem endereço e bairro separado, monta bloco mais forte
        if endereco and bairro and "manaus" not in endereco.lower():
            endereco = f"{endereco} - {bairro} - MANAUS/AM"

        if endereco:
            entregas.append({
                "nome": nome,
                "endereco_raw": strip_cep_text(endereco),
                "bairro_raw": bairro,
                "cep8": cep
            })

    entregas = [e for e in entregas if len(only_digits(e.get("cep8", ""))) == 8]
    return entregas

# =========================
# GEOCODIFICAR
# =========================
def make_cache_key(cep_fmt, numero, bairro, base_rua):
    return f"{cep_fmt}|{str(numero).upper()}|{normaliza(bairro)}|{normaliza(base_rua)}"

def geocodificar_entrega(entrega, geocode_cache, viacep_cache):
    start = time.monotonic()

    def estourou():
        return (time.monotonic() - start) > MAX_SECONDS_PER_ADDRESS

    cep8d = only_digits(entrega.get("cep8", ""))
    cep_fmt = formata_cep(cep8d)
    endereco_raw = (entrega.get("endereco_raw") or "").strip()
    rua_lista = canonicaliza_via(endereco_raw)
    rua_lista = force_rua_if_rio(rua_lista)
    numero = extrair_numero(rua_lista)

    via = viacep_get(cep8d, viacep_cache)
    if not via:
        bairro_alvo = BAIRRO_FIXO or entrega.get("bairro_raw", "")
        dest = f"{rua_lista or cep_fmt}, {numero}, {bairro_alvo}, Manaus-AM".strip(", ")
        return None, None, bairro_alvo, dest, "FALLBACK: VIACEP_FALHOU", False

    if normaliza(via.get("localidade")) != "manaus":
        bairro_alvo = BAIRRO_FIXO or entrega.get("bairro_raw", "") or via.get("bairro", "")
        dest = f"{via.get('logradouro','') or rua_lista}, {numero}, {bairro_alvo}, {via.get('localidade','')}-{via.get('uf','')}".strip(", ")
        return None, None, bairro_alvo, dest, f"FALLBACK: CEP_FORA_MANAUS ({via.get('localidade','')})", False

    via_logradouro = via.get("logradouro", "")
    via_bairro = via.get("bairro", "")
    city = via.get("localidade", "Manaus") or "Manaus"
    uf = via.get("uf", "AM") or "AM"

    bairro_alvo = BAIRRO_FIXO or entrega.get("bairro_raw", "") or via_bairro

    base_rua = via_logradouro or rua_lista or endereco_raw or cep_fmt
    ck = make_cache_key(cep_fmt, numero, bairro_alvo, base_rua)

    if ck in geocode_cache:
        try:
            lat = float(geocode_cache[ck]["lat"])
            lon = float(geocode_cache[ck]["lon"])
            if dentro_de_manaus(lat, lon):
                dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
                return lat, lon, bairro_alvo, dest, "CACHE", True
        except Exception:
            pass

    queries = []
    if cep_fmt:
        queries.append(f"{cep_fmt}, {numero}, {bairro_alvo}, {city}-{uf}, {COUNTRY}")
    if via_logradouro:
        queries.append(f"{canonicaliza_via(via_logradouro)}, {numero}, {bairro_alvo}, {city}-{uf}, {cep_fmt}, {COUNTRY}")
    if rua_lista:
        queries.append(f"{rua_lista}, {numero}, {bairro_alvo}, {city}-{uf}, {cep_fmt}, {COUNTRY}")
    if via_logradouro:
        queries.append(f"{canonicaliza_via(via_logradouro)}, {bairro_alvo}, {city}-{uf}, {cep_fmt}, {COUNTRY}")

    melhor_lat = melhor_lon = None
    melhor_score = -1.0
    melhor_note = ""

    for q_idx, q in enumerate(queries, start=1):
        if estourou():
            break

        cand = nominatim_search(q, bounded=True, limit=5, timeout_s=8)
        lat, lon, score, item = escolher_melhor_candidato(cand, rua_lista or endereco_raw or q, bairro_alvo)

        if lat is not None and score > melhor_score:
            melhor_lat, melhor_lon, melhor_score = lat, lon, score
            melhor_note = f"NOMINATIM_BOUNDED(q{q_idx},score={score:.2f})"
            if score >= 0.78:
                break

        if estourou():
            break

        cand2 = nominatim_search(q, bounded=False, limit=5, timeout_s=8)
        lat2, lon2, score2, item2 = escolher_melhor_candidato(cand2, rua_lista or endereco_raw or q, bairro_alvo)

        if lat2 is not None and score2 > melhor_score:
            melhor_lat, melhor_lon, melhor_score = lat2, lon2, score2
            melhor_note = f"NOMINATIM_FREE(q{q_idx},score={score2:.2f})"
            if score2 >= 0.78:
                break

    if melhor_lat is not None and melhor_lon is not None and dentro_de_manaus(melhor_lat, melhor_lon):
        geocode_cache[ck] = {
            "key": ck,
            "lat": str(melhor_lat),
            "lon": str(melhor_lon),
            "updated_at": now_iso(),
        }
        dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
        return melhor_lat, melhor_lon, bairro_alvo, dest, melhor_note, False

    dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
    if not dest:
        dest = f"{bairro_alvo}, Manaus-AM, {cep_fmt}"
    return None, None, bairro_alvo, dest, "FALLBACK: NAO_ENCONTRADO", False

# =========================
# CSV
# =========================
def build_csv(rows: List[List[Any]]) -> bytes:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
    w.writerows(rows)
    return out.getvalue().encode("utf-8")

# =========================
# ROUTES
# =========================
@app.get("/")
def home():
    return render_template_string(HTML)

@app.post("/process")
def process():
    pwd = (request.form.get("password") or "").strip()
    text = request.form.get("text") or ""

    if pwd != APP_PASSWORD:
        abort(401, "Senha errada.")

    if not text.strip():
        abort(400, "Texto vazio.")

    geocode_cache = load_cache_csv(GEOCODE_CACHE_FILE)
    viacep_cache = load_cache_csv(VIACEP_CACHE_FILE)

    entregas = parse_entregas(text)

    if not entregas:
        abort(400, "Não achei endereços. Pode colar no formato nome + endereço + CEP em linhas separadas.")

    if len(entregas) > MAX_TOTAL_ADDRESSES:
        abort(400, f"Lista grande demais ({len(entregas)}). Limite: {MAX_TOTAL_ADDRESSES}.")

    ok_rows = []

    for idx, ent in enumerate(entregas, start=1):
        lat, lon, bairro, dest, note, used_cache = geocodificar_entrega(ent, geocode_cache, viacep_cache)
        cep_fmt = formata_cep(ent.get("cep8", ""))

        ok_rows.append([
            idx,
            dest,
            bairro,
            "Manaus",
            cep_fmt,
            f"{lat:.6f}" if isinstance(lat, float) else "",
            f"{lon:.6f}" if isinstance(lon, float) else "",
            note
        ])

    save_cache_csv(GEOCODE_CACHE_FILE, ["key", "lat", "lon", "updated_at"], geocode_cache)
    save_cache_csv(VIACEP_CACHE_FILE, ["key", "cep", "logradouro", "bairro", "localidade", "uf", "updated_at"], viacep_cache)

    csv_bytes = build_csv(ok_rows)

    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name="circuit_import_site.csv"
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
