import os
import io
import re
import csv
import time
import json
import base64
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import requests
from flask import Flask, request, send_file, render_template_string, abort, jsonify

app = Flask(__name__)

# =========================
# CONFIG
# =========================
APP_VERSION = "6.1-site"
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

# Jobs em memória
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

# =========================
# HTML
# =========================
HTML = r"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <title>Roteirizador Privado v6.1</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    body{font-family:Arial,sans-serif;background:#fff;margin:0;padding:0}
    .wrap{max-width:950px;margin:38px auto;padding:0 16px}
    h1{text-align:center;margin:0 0 18px}
    .badge{font-size:12px;background:#eee;border-radius:999px;padding:3px 8px;vertical-align:middle}
    .card{border:1px solid #ddd;border-radius:12px;padding:18px;box-shadow:0 2px 10px rgba(0,0,0,.03)}
    .hint{font-size:13px;color:#444;line-height:1.5}
    .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-top:12px}
    input[type=password]{padding:10px;border:1px solid #ccc;border-radius:10px;width:260px}
    button{padding:10px 16px;border:0;border-radius:10px;background:#111;color:#fff;font-weight:bold;cursor:pointer}
    button:disabled{opacity:.5;cursor:not-allowed}
    textarea{width:100%;min-height:360px;margin-top:12px;border:1px solid #ccc;border-radius:10px;padding:12px;font:13px Consolas,monospace;resize:vertical}
    .progress-wrap{display:none;margin-top:14px}
    .status{font-size:13px;color:#333;margin-bottom:8px}
    .progress-bar{width:100%;height:14px;border-radius:999px;border:1px solid #ddd;background:#eee;overflow:hidden}
    .progress-fill{height:100%;width:0;background:#111;transition:width .2s linear}
    .small{font-size:12px;color:#666;margin-top:8px}
    .err{color:#b00020;font-weight:bold}
  </style>
</head>
<body>
<div class="wrap">
  <h1>Roteirizador Privado <span class="badge">v6.1</span></h1>

  <div class="card">
    <div class="hint">
      Cole a lista bagunçada (Shopee/Loggi/ML/etc).<br>
      Formatos aceitos:
      <br>• nome + endereço + CEP em linhas separadas
      <br>• endereço com CEP na mesma linha
      <br>• CEP com ou sem <b>CEP:</b>
      <br><br>
      Se o endereço ficar duvidoso, eu caio em fallback e marco em <b>Notes</b>.
    </div>

    <div class="row">
      <label><b>Senha:</b></label>
      <input id="pwd" type="password" placeholder="APP_PASSWORD">
      <button id="btn" onclick="startJob()">Gerar CSV</button>
    </div>

    <label style="display:block;margin-top:14px"><b>Cole aqui:</b></label>
    <textarea id="txt" placeholder="Cole aqui sua lista bagunçada..."></textarea>

    <div class="progress-wrap" id="pwrap">
      <div class="status" id="status">Preparando…</div>
      <div class="progress-bar"><div class="progress-fill" id="pbar"></div></div>
      <div class="small" id="small">Processando… não fecha a página.</div>
    </div>

    <div class="small">
      Saída: <b>circuit_import_site.csv</b> (download automático)
    </div>
  </div>
</div>

<script>
let pollTimer = null;

function setProgress(p){
  document.getElementById("pbar").style.width = `${p.toFixed(1)}%`;
}

async function startJob(){
  const pwd = document.getElementById("pwd").value.trim();
  const text = document.getElementById("txt").value;

  if(!pwd){
    alert("Coloca a senha.");
    return;
  }
  if(!text.trim()){
    alert("Cole a lista primeiro.");
    return;
  }

  document.getElementById("btn").disabled = true;
  document.getElementById("pwrap").style.display = "block";
  document.getElementById("status").innerText = "Criando job…";
  setProgress(0);

  try{
    const r = await fetch("/start", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({password: pwd, text: text})
    });

    const j = await r.json();

    if(!r.ok){
      throw new Error(j.error || "Falha ao iniciar.");
    }

    const job_id = j.job_id;

    if(pollTimer) clearInterval(pollTimer);

    pollTimer = setInterval(async ()=>{
      try{
        const rr = await fetch(`/progress/${job_id}`);
        const jj = await rr.json();

        if(!rr.ok){
          throw new Error(jj.error || "Falha ao consultar progresso.");
        }

        setProgress(jj.percent || 0);
        document.getElementById("status").innerText = (jj.message || "Processando…");
        document.getElementById("small").innerText = (jj.detail || "Processando…");

        if(jj.state === "done"){
          clearInterval(pollTimer);
          pollTimer = null;
          setProgress(100);
          document.getElementById("status").innerText = "Finalizado. Baixando CSV…";
          window.location.href = `/download/${job_id}`;
          document.getElementById("btn").disabled = false;
        }

        if(jj.state === "error"){
          clearInterval(pollTimer);
          pollTimer = null;
          document.getElementById("btn").disabled = false;
          document.getElementById("status").innerHTML = "<span class='err'>Erro:</span> " + (jj.error || "erro");
        }

      }catch(e){
        clearInterval(pollTimer);
        pollTimer = null;
        document.getElementById("btn").disabled = false;
        document.getElementById("status").innerHTML = "<span class='err'>Erro:</span> " + e.message;
      }
    }, 900);

  }catch(e){
    document.getElementById("btn").disabled = false;
    document.getElementById("status").innerHTML = "<span class='err'>Erro:</span> " + e.message;
  }
}
</script>
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

def similaridade(a: str, b: str) -> float:
    import difflib
    a = normaliza(a)
    b = normaliza(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

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
    b_res = normaliza(pega_bairro_do_resultado(item)) or normaliza(item.get("display_name", ""))
    if b in b_res:
        return True
    if similaridade(bairro_esperado, pega_bairro_do_resultado(item)) >= 0.62:
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
# PARSER NOVO
# =========================
RE_HAS_STREET = re.compile(r"\b(rua|r\b|avenida|av\b|travessa|tv\b|beco|estrada|alameda|praça|praca|rodovia|rio)\b", re.IGNORECASE)
RE_BAIRRO_MANAUS = re.compile(r"\-\s*([A-Za-zÀ-ÿ0-9\s\.\']{3,})\s*\-\s*manaus", re.IGNORECASE)

def extrair_cep_any(texto: str):
    if not texto:
        return None
    m = re.search(r"(?:\bcep\b\s*:\s*)?(\d{5})[-\s]?(\d{3})", texto, flags=re.I)
    if not m:
        return None
    return (m.group(1) + m.group(2)).strip()

def linha_tem_via(texto: str):
    t = (texto or "").lower()

    if re.search(r"(^|\s)(r\.?|rua)\b", t):
        return True
    if re.search(r"\b(av\.?|avenida)\b", t):
        return True
    if re.search(r"\b(travessa|beco|estrada|alameda|praça|praca|rodovia|rio)\b", t):
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
    toks = [t for t in s.split() if t.strip()]
    return len(toks) >= 2

def extrair_bairro_de_endereco(endereco: str):
    parts = [p.strip() for p in re.split(r"\s-\s", endereco)]
    if len(parts) >= 2:
        bairro = parts[1].strip()
        bairro = re.sub(r"\b(manaus|am|amazonas)\b", "", bairro, flags=re.IGNORECASE).strip()
        if bairro and len(bairro) >= 3:
            return bairro
    return ""

def strip_cep_text(s: str):
    s = re.sub(r"\bCEP\b\s*:?\s*\d{5}-?\d{3}\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\b\d{5}-?\d{3}\b", "", s)
    s = re.sub(r"\b\d{8}\b", "", s)
    return limpar_texto(s)

def parse_entregas(texto_colado: str):
    linhas = [limpar_texto(l) for l in (texto_colado or "").splitlines()]
    raw_lines = [l for l in linhas]

    entregas = []
    i = 0

    while i < len(raw_lines):
        line = raw_lines[i].strip()
        if not line:
            i += 1
            continue

        # Modelo 1: nome / endereço / cep
        if is_linha_nome(line):
            nome = line
            endereco = ""
            cep8d = None

            if i + 1 < len(raw_lines) and raw_lines[i + 1].strip():
                poss_end = raw_lines[i + 1].strip()
                if linha_tem_via(poss_end):
                    endereco = poss_end

                    if i + 2 < len(raw_lines):
                        cep8d = extrair_cep_any(raw_lines[i + 2].strip())

                    if not cep8d:
                        cep8d = extrair_cep_any(endereco)

                    if endereco and cep8d:
                        entregas.append({
                            "nome": nome,
                            "endereco_raw": strip_cep_text(endereco),
                            "bairro_raw": extrair_bairro_de_endereco(endereco),
                            "cep8": cep8d
                        })
                        i += 3
                        continue

        # Modelo 2: endereço / bairro / cep
        if linha_tem_via(line):
            endereco = line
            cep8d = extrair_cep_any(line)

            if not cep8d and i + 1 < len(raw_lines):
                cep8d = extrair_cep_any(raw_lines[i + 1].strip())

            if cep8d:
                entregas.append({
                    "nome": "",
                    "endereco_raw": strip_cep_text(endereco),
                    "bairro_raw": extrair_bairro_de_endereco(endereco),
                    "cep8": cep8d
                })
                i += 2
                continue

        # Modelo 3: rua numa linha / bairro noutra / cep noutra
        if i + 2 < len(raw_lines):
            l1 = raw_lines[i].strip()
            l2 = raw_lines[i + 1].strip()
            l3 = raw_lines[i + 2].strip()

            if l1 and l2 and l3:
                c3 = extrair_cep_any(l3)
                if c3:
                    if (linha_tem_via(l1) or re.search(r",\s*\d", l1)) and (not extrair_cep_any(l1)):
                        endereco = f"{l1} - {l2} - MANAUS/AM"
                        entregas.append({
                            "nome": "",
                            "endereco_raw": strip_cep_text(endereco),
                            "bairro_raw": l2,
                            "cep8": c3
                        })
                        i += 3
                        continue

        i += 1

    entregas = [e for e in entregas if len(only_digits(e.get("cep8", ""))) == 8]
    return entregas

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
# JOBS
# =========================
def cleanup_jobs():
    dead = []
    now = time.time()
    with JOBS_LOCK:
        for jid, j in JOBS.items():
            if now - j.get("created_at", now) > 1800:
                dead.append(jid)
        for jid in dead:
            JOBS.pop(jid, None)

def set_job(job_id: str, **kwargs):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(kwargs)

# =========================
# GEOCODIFICAR
# =========================
def make_cache_key(cep_fmt, numero, bairro, base_rua):
    base = normaliza(base_rua)
    b = normaliza(bairro)
    return f"{cep_fmt}|{str(numero).upper()}|{b}|{base}"

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
        bairro_alvo = BAIRRO_FIXO.strip() or entrega.get("bairro_raw", "")
        dest = f"{rua_lista or cep_fmt}, {numero}, {bairro_alvo}, Manaus-AM".strip(", ")
        return None, None, bairro_alvo, dest, "FALLBACK: VIACEP_FALHOU", False

    if normaliza(via.get("localidade")) != "manaus":
        bairro_alvo = BAIRRO_FIXO.strip() or entrega.get("bairro_raw", "") or via.get("bairro", "")
        dest = f"{via.get('logradouro','') or rua_lista}, {numero}, {bairro_alvo}, {via.get('localidade','')}-{via.get('uf','')}".strip(", ")
        return None, None, bairro_alvo, dest, f"FALLBACK: CEP_FORA_MANAUS ({via.get('localidade','')})", False

    via_logradouro = via.get("logradouro", "")
    via_bairro = via.get("bairro", "")
    city = via.get("localidade", "Manaus") or "Manaus"
    uf = via.get("uf", "AM") or "AM"

    bairro_alvo = BAIRRO_FIXO.strip() or entrega.get("bairro_raw", "") or via_bairro

    base_rua = via_logradouro or rua_lista or endereco_raw or cep_fmt
    ck = make_cache_key(cep_fmt, numero, bairro_alvo, base_rua)

    if ck in geocode_cache:
        lat = float(geocode_cache[ck]["lat"])
        lon = float(geocode_cache[ck]["lon"])
        if dentro_de_manaus(lat, lon):
            dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
            return lat, lon, bairro_alvo, dest, "CACHE", True

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
# WORKER
# =========================
def worker_process(job_id: str, text: str):
    geocode_cache = load_cache_csv(GEOCODE_CACHE_FILE)
    viacep_cache = load_cache_csv(VIACEP_CACHE_FILE)

    try:
        entregas = parse_entregas(text)
        total = len(entregas)

        if total == 0:
            set_job(job_id, state="error", error="Não achei endereços. Pode colar no formato nome + endereço + CEP em linhas separadas.")
            return

        if total > MAX_TOTAL_ADDRESSES:
            set_job(job_id, state="error", error=f"Lista grande demais ({total}). Limite: {MAX_TOTAL_ADDRESSES}.")
            return

        ok_rows = []
        revisar = 0

        set_job(job_id, state="running", total=total, done=0, percent=0.0, message="Começando…", detail="Processando…")

        start_all = time.monotonic()

        for idx, ent in enumerate(entregas, start=1):
            pct = (idx / total) * 100.0
            set_job(job_id, done=idx-1, percent=pct, message=f"{pct:.1f}% ({idx}/{total})", detail=(ent.get("endereco_raw","")[:80] or "Preparando…"))

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

            if note.startswith("FALLBACK"):
                revisar += 1

            set_job(job_id, done=idx, percent=pct, message=f"{pct:.1f}% ({idx}/{total})", detail=note)

        save_cache_csv(GEOCODE_CACHE_FILE, ["key", "lat", "lon", "updated_at"], geocode_cache)
        save_cache_csv(VIACEP_CACHE_FILE, ["key", "cep", "logradouro", "bairro", "localidade", "uf", "updated_at"], viacep_cache)

        csv_bytes = build_csv(ok_rows)
        elapsed = time.monotonic() - start_all

        set_job(
            job_id,
            state="done",
            done=total,
            total=total,
            percent=100.0,
            message="100.0% Finalizado",
            detail=f"OK={len(ok_rows)} | Revisar={revisar} | Tempo={elapsed:.1f}s",
            file_bytes=csv_bytes,
            file_name="circuit_import_site.csv"
        )

    except Exception as e:
        set_job(job_id, state="error", error=str(e))

# =========================
# ROUTES
# =========================
@app.get("/")
def home():
    return render_template_string(HTML)

@app.post("/start")
def start():
    payload = request.get_json(silent=True) or {}
    pwd = (payload.get("password") or "").strip()
    text = payload.get("text") or ""

    if pwd != APP_PASSWORD:
        return jsonify({"error": "Senha errada."}), 401

    if not text.strip():
        return jsonify({"error": "Texto vazio."}), 400

    cleanup_jobs()

    job_id = base64.urlsafe_b64encode(os.urandom(9)).decode("ascii").rstrip("=")

    with JOBS_LOCK:
        JOBS[job_id] = {
            "job_id": job_id,
            "created_at": time.time(),
            "state": "queued",
            "done": 0,
            "total": 0,
            "percent": 0.0,
            "message": "Na fila…",
            "detail": "",
            "error": None,
            "file_bytes": None,
            "file_name": None,
        }

    t = threading.Thread(target=worker_process, args=(job_id, text), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})

@app.get("/progress/<job_id>")
def progress(job_id: str):
    cleanup_jobs()
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Job não encontrado. Rode de novo."}), 404

        return jsonify({
            "job_id": job_id,
            "state": job.get("state"),
            "done": job.get("done", 0),
            "total": job.get("total", 0),
            "percent": job.get("percent", 0.0),
            "message": job.get("message", ""),
            "detail": job.get("detail", ""),
            "error": job.get("error"),
        })

@app.get("/download/<job_id>")
def download(job_id: str):
    cleanup_jobs()
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            abort(404, "Job não encontrado.")
        if job.get("state") != "done":
            abort(400, "Job ainda não terminou.")
        data = job.get("file_bytes")
        fname = job.get("file_name") or "circuit_import_site.csv"
        if not data:
            abort(500, "Arquivo não disponível.")

    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=fname
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
