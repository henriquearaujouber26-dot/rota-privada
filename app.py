import os
import io
import re
import csv
import time
import difflib
import requests
import threading
import uuid
from datetime import datetime
from flask import Flask, request, send_file, render_template_string, abort

# ===== SSL FIX (Windows / alguns ambientes) =====
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
except Exception:
    pass

app = Flask(__name__)

# ================== CONFIG ==================
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")
USER_AGENT = os.environ.get("USER_AGENT", "rota-privada-web/6.0.1")
COUNTRY = "Brazil"

SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.70"))
TIMEOUT_HTTP = int(os.environ.get("TIMEOUT_HTTP", "25"))

# Manaus box (west, south, east, north)
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)

GEOCODE_CACHE_FILE = os.environ.get("GEOCODE_CACHE_FILE", "geocode_cache.csv")
VIACEP_CACHE_FILE = os.environ.get("VIACEP_CACHE_FILE", "viacep_cache.csv")

# ================== PROGRESS / JOBS ==================
JOBS = {}
JOBS_LOCK = threading.Lock()

def job_set(job_id, **kwargs):
    with JOBS_LOCK:
        JOBS.setdefault(job_id, {})
        JOBS[job_id].update(kwargs)

def job_get(job_id):
    with JOBS_LOCK:
        return JOBS.get(job_id)

def job_done(job_id, csv_bytes, filename, stats=None):
    job_set(
        job_id,
        status="done",
        percent=100,
        message="Finalizado. Preparando download...",
        csv_bytes=csv_bytes,
        filename=filename,
        finished_at=datetime.now().isoformat(timespec="seconds"),
        stats=stats or {},
    )

# ================== HTML ==================
HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Roteirizador Privado v6.0.1</title>
  <style>
    body{font-family:Arial, sans-serif; max-width:980px; margin:24px auto; padding:0 14px;}
    .card{border:1px solid #ddd; border-radius:12px; padding:16px; box-shadow:0 2px 10px rgba(0,0,0,.04);}
    textarea{width:100%; height:360px; padding:12px; border-radius:10px; border:1px solid #ccc; font-family:monospace; font-size:13px;}
    input[type=password]{padding:10px; border-radius:10px; border:1px solid #ccc; width:240px;}
    button{padding:12px 16px; border-radius:10px; border:none; cursor:pointer;}
    .row{display:flex; gap:12px; align-items:center; flex-wrap:wrap;}
    .muted{color:#666; font-size:13px;}
    .warn{color:#b45309;}
    .footer{margin-top:10px; font-size:12px; color:#888;}
    .pill{display:inline-block; padding:4px 10px; border-radius:999px; background:#f3f4f6; font-size:12px;}
    .barwrap{width:100%; background:#eee; border-radius:999px; overflow:hidden; height:14px;}
    .bar{height:14px; width:0%; background:#111;}
    .status{margin-top:10px; font-family:monospace; font-size:13px;}
    .hidden{display:none;}
  </style>
</head>
<body>
  <h2>Roteirizador Privado <span class="pill">v6.0.1</span></h2>

  <div class="card">
    <p class="muted">
      Cole o texto bagunçado (Shopee/Loggi/etc) e eu gero o CSV do Circuit.
      <br><span class="warn">Agora com barra de progresso.</span>
    </p>

    <form id="frm">
      <div class="row">
        <label>Senha:</label>
        <input name="password" type="password" placeholder="APP_PASSWORD" required />
        <button id="btn" type="submit">Gerar CSV</button>
      </div>

      <p class="muted">Cole aqui:</p>
      <textarea name="text" placeholder="Cole aqui sua lista bagunçada..."></textarea>
    </form>

    <div id="prog" class="hidden">
      <p class="muted">Processando… não fecha a página.</p>
      <div class="barwrap"><div id="bar" class="bar"></div></div>
      <div id="status" class="status">0%</div>
    </div>

    <div class="footer">
      Saída: <b>circuit_import_*.csv</b>. Se cair em fallback, vai em Notes com motivo.
    </div>
  </div>

<script>
  const frm = document.getElementById("frm");
  const prog = document.getElementById("prog");
  const bar = document.getElementById("bar");
  const statusEl = document.getElementById("status");
  const btn = document.getElementById("btn");

  function setProgress(p, msg){
    const pct = Math.max(0, Math.min(100, p||0));
    bar.style.width = pct + "%";
    statusEl.textContent = (pct.toFixed(1)) + "% - " + (msg || "");
  }

  async function poll(job_id){
    while(true){
      const r = await fetch("/status/" + job_id);
      const j = await r.json();
      setProgress(j.percent || 0, j.message || "");
      if(j.status === "done"){
        window.location.href = "/download/" + job_id;
        btn.disabled = false;
        return;
      }
      if(j.status === "error"){
        alert("Erro: " + (j.message || "falhou"));
        btn.disabled = false;
        return;
      }
      await new Promise(res => setTimeout(res, 900));
    }
  }

  frm.addEventListener("submit", async (e) => {
    e.preventDefault();
    btn.disabled = true;
    prog.classList.remove("hidden");
    setProgress(0, "Enviando…");

    const fd = new FormData(frm);
    const r = await fetch("/start", { method:"POST", body: fd });
    if(!r.ok){
      const t = await r.text();
      alert(t);
      btn.disabled = false;
      return;
    }
    const j = await r.json();
    setProgress(1, "Iniciando…");
    poll(j.job_id);
  });
</script>
</body>
</html>
"""

# ================== HTTP session ==================
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

# ================== Helpers ==================
def normaliza(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def similaridade(a: str, b: str) -> float:
    a = normaliza(a)
    b = normaliza(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def formata_cep(cep8: str) -> str:
    c = re.sub(r"\D", "", cep8 or "")
    return f"{c[:5]}-{c[5:]}" if len(c) == 8 else (cep8 or "")

def extrair_cep(texto: str):
    m = re.search(r"\b(\d{5}-?\d{3})\b", texto or "")
    if not m:
        return None
    return re.sub(r"\D", "", m.group(1))

def extrair_numero(texto: str) -> str:
    t = texto or ""
    m = re.search(r"(?:,|\s)\s*(\d+[A-Za-z]?)\b", t)
    if m:
        return m.group(1)
    m2 = re.search(r"\b(\d+[A-Za-z]?)\b", t)
    return m2.group(1) if m2 else "S/N"

def limpar_linha(texto: str) -> str:
    t = (texto or "").replace("N/A", " ")
    t = re.sub(r"\b\d{9,}\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def tem_palavra_de_via(texto: str) -> bool:
    return bool(re.search(r"\b(rua|avenida|av\.|av |travessa|beco|estrada|alameda|praça|praca|rodovia)\b", (texto or "").lower()))

def dentro_de_manaus(lat: float, lon: float) -> bool:
    west, south, east, north = MANAUS_VIEWBOX
    return (west <= lon <= east) and (south <= lat <= north)

def cache_key(cep_fmt: str, numero: str, logradouro: str, raw: str) -> str:
    base = normaliza(logradouro)
    if not base:
        base = normaliza(re.sub(r"\b\d{5}-?\d{3}\b", "", raw or ""))
    return f"{cep_fmt}|{str(numero).upper()}|{base}"

def carregar_cache_csv(path: str, key_field: str):
    data = {}
    if not os.path.exists(path):
        return data
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                k = (row.get(key_field) or "").strip()
                if k:
                    data[k] = row
    except Exception:
        return {}
    return data

def salvar_cache_geocode(cache: dict):
    with open(GEOCODE_CACHE_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["key", "lat", "lon", "updated_at"])
        for k, v in cache.items():
            w.writerow([k, v["lat"], v["lon"], v.get("updated_at", "")])

def salvar_cache_viacep(cache: dict):
    with open(VIACEP_CACHE_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cep", "logradouro", "bairro", "localidade", "uf", "updated_at"])
        for cep, v in cache.items():
            w.writerow([cep, v.get("logradouro",""), v.get("bairro",""), v.get("localidade",""), v.get("uf",""), v.get("updated_at","")])

# ================== APIs ==================
def viacep_get(cep8: str, viacep_cache: dict):
    cep8 = re.sub(r"\D", "", cep8 or "")
    if len(cep8) != 8:
        return None

    if cep8 in viacep_cache:
        return viacep_cache[cep8]

    url = f"https://viacep.com.br/ws/{cep8}/json/"
    r = session.get(url, timeout=TIMEOUT_HTTP)
    if r.status_code != 200:
        return None
    j = r.json()
    if "erro" in j:
        return None

    j["updated_at"] = datetime.now().isoformat(timespec="seconds")
    viacep_cache[cep8] = j
    return j

def nominatim_search(query: str, bounded=True, limit=1):
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

    r = session.get(url, params=params, timeout=TIMEOUT_HTTP)
    time.sleep(SLEEP_NOMINATIM)

    if r.status_code != 200:
        return []
    try:
        return r.json() or []
    except Exception:
        return []

def tentar_1_resultado(q: str, bounded=True):
    data = nominatim_search(q, bounded=bounded, limit=1)
    if not data:
        return None, None, ""
    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    disp = data[0].get("display_name", "") or ""
    return lat, lon, disp

def caca_nome_antigo(raw_original: str, bairro: str, cidade: str, uf: str, cep_fmt: str):
    consulta = f"{raw_original}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}"
    candidatos = nominatim_search(consulta, bounded=True, limit=5)
    if not candidatos:
        candidatos = nominatim_search(consulta, bounded=False, limit=5)

    melhor = (None, None, 0.0, "")
    for c in candidatos:
        try:
            lat = float(c.get("lat"))
            lon = float(c.get("lon"))
        except Exception:
            continue
        if not dentro_de_manaus(lat, lon):
            continue

        display = c.get("display_name", "") or ""
        score = similaridade(raw_original, display)

        cls = (c.get("class") or "").lower()
        typ = (c.get("type") or "").lower()
        if cls in ("highway", "place", "building", "amenity"):
            score += 0.05
        if typ in ("residential", "road", "house", "yes"):
            score += 0.05

        if score > melhor[2]:
            melhor = (lat, lon, score, display)

    if melhor[0] is not None and melhor[2] >= 0.42:
        return melhor[0], melhor[1], melhor[3], melhor[2]
    return None, None, "", 0.0

def montar_fallback_endereco(logradouro: str, numero: str, bairro: str, cep_fmt: str) -> str:
    partes = []
    if logradouro:
        if numero and numero != "S/N":
            partes.append(f"{logradouro}, {numero}")
        else:
            partes.append(logradouro)
    if bairro:
        partes.append(bairro)
    partes.append("Manaus-AM")
    if cep_fmt:
        partes.append(cep_fmt)
    return " - ".join(partes)

def extrair_enderecos_do_texto(texto: str):
    linhas = [limpar_linha(x) for x in (texto or "").splitlines()]
    linhas = [x for x in linhas if x.strip()]

    enderecos = []
    i = 0
    while i < len(linhas):
        ln = linhas[i]
        cep = extrair_cep(ln)

        if cep and tem_palavra_de_via(ln):
            enderecos.append(ln)
            i += 1
            continue

        if tem_palavra_de_via(ln) and not cep:
            combinado = ln
            achou = None
            for j in range(1, 3):
                if i + j >= len(linhas):
                    break
                cand = linhas[i + j]
                cep2 = extrair_cep(cand)
                if cep2:
                    combinado = f"{ln} {cand}"
                    achou = cep2
                    break
            if achou:
                enderecos.append(combinado)
                i += 1
                continue

        if cep and not tem_palavra_de_via(ln) and i > 0:
            prev = linhas[i - 1]
            if tem_palavra_de_via(prev) and not extrair_cep(prev):
                enderecos.append(f"{prev} {ln}")
                i += 1
                continue

        i += 1

    seen = set()
    out = []
    for e in enderecos:
        key = normaliza(e)
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out

# ================== JOB WORKER ==================
def run_job(job_id, text):
    try:
        enderecos = extrair_enderecos_do_texto(text)
        total = len(enderecos)
        if total == 0:
            job_set(job_id, status="error", percent=0, message="Não achei endereços no texto.")
            return

        job_set(job_id, percent=2, message=f"{total} endereços detectados. Carregando cache...")

        # caches
        geocode_cache_raw = carregar_cache_csv(GEOCODE_CACHE_FILE, "key")
        geocode_cache = {}
        for k, row in geocode_cache_raw.items():
            try:
                geocode_cache[k] = {
                    "lat": float(row.get("lat", "")),
                    "lon": float(row.get("lon", "")),
                    "updated_at": row.get("updated_at", ""),
                }
            except Exception:
                continue

        viacep_cache_raw = carregar_cache_csv(VIACEP_CACHE_FILE, "cep")
        viacep_cache = {}
        for cep, row in viacep_cache_raw.items():
            viacep_cache[re.sub(r"\D", "", cep)] = dict(row)

        ok_rows = []
        revisar_count = 0
        cache_hits = 0
        cache_saves = 0
        cacados = 0

        inicio = datetime.now()

        for idx, raw in enumerate(enderecos, start=1):
            pct = 5 + (93 * idx / max(1, total))
            job_set(job_id, percent=pct, message=f"({idx}/{total}) processando...")

            raw = limpar_linha(raw)
            cep8 = extrair_cep(raw)

            if not cep8:
                revisar_count += 1
                ok_rows.append([idx, raw, "", "Manaus", "", "", "", "REVISAO_AUTOMATICA: SEM_CEP"])
                continue

            dados = viacep_get(cep8, viacep_cache)
            if not dados:
                revisar_count += 1
                ok_rows.append([idx, raw, "", "Manaus", formata_cep(cep8), "", "", f"REVISAO_AUTOMATICA: CEP_INVALIDO ({cep8})"])
                continue

            cidade = (dados.get("localidade") or "").strip()
            uf = (dados.get("uf") or "AM").strip()
            if normaliza(cidade) != "manaus":
                revisar_count += 1
                ok_rows.append([idx, raw, (dados.get("bairro") or ""), cidade or "?", formata_cep(cep8), "", "", f"REVISAO_AUTOMATICA: CEP_FORA_MANAUS ({cidade})"])
                continue

            logradouro = (dados.get("logradouro") or "").strip()
            bairro = (dados.get("bairro") or "").strip()
            numero = extrair_numero(raw)
            cep_fmt = formata_cep(cep8)

            k = cache_key(cep_fmt, numero, logradouro, raw)
            if k in geocode_cache:
                lat = geocode_cache[k]["lat"]
                lon = geocode_cache[k]["lon"]
                if dentro_de_manaus(lat, lon):
                    cache_hits += 1
                    ok_rows.append([idx, f"{logradouro}, {numero}".strip(", "), bairro, "Manaus", cep_fmt, lat, lon, "CACHE"])
                    continue

            lat = lon = None
            note = ""

            queries = []
            if logradouro:
                if numero and numero != "S/N":
                    queries.append(f"{logradouro}, {numero}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")
                queries.append(f"{logradouro}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")
            queries.append(f"{raw}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")

            for q in queries:
                lat1, lon1, _ = tentar_1_resultado(q, bounded=True)
                if lat1 is not None and dentro_de_manaus(lat1, lon1):
                    lat, lon = lat1, lon1
                    break

            if lat is None:
                for q in queries:
                    lat1, lon1, _ = tentar_1_resultado(q, bounded=False)
                    if lat1 is not None and dentro_de_manaus(lat1, lon1):
                        lat, lon = lat1, lon1
                        break

            if lat is None:
                lat3, lon3, display3, score3 = caca_nome_antigo(raw, bairro, "Manaus", uf, cep_fmt)
                if lat3 is not None and dentro_de_manaus(lat3, lon3):
                    lat, lon = lat3, lon3
                    cacados += 1
                    note = f"CACADO(score={score3:.2f}): {display3[:90]}"

            if lat is None or lon is None:
                revisar_count += 1
                destino = montar_fallback_endereco(logradouro, numero, bairro, cep_fmt)
                ok_rows.append([idx, destino, bairro, "Manaus", cep_fmt, "", "", f"REVISAO_AUTOMATICA: FALLBACK_SEM_COORD | original: {raw}"])
                continue

            ok_rows.append([idx, f"{logradouro}, {numero}".strip(", "), bairro, "Manaus", cep_fmt, lat, lon, note])
            geocode_cache[k] = {"lat": lat, "lon": lon, "updated_at": datetime.now().isoformat(timespec="seconds")}
            cache_saves += 1

        job_set(job_id, percent=98, message="Salvando cache e montando CSV...")

        salvar_cache_geocode(geocode_cache)
        salvar_cache_viacep(viacep_cache)

        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
        w.writerows(ok_rows)

        data = out.getvalue().encode("utf-8")
        filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        duracao = (datetime.now() - inicio).total_seconds()
        stats = {
            "total": total,
            "tempo_s": round(duracao, 1),
            "cache_hits": cache_hits,
            "cache_saves": cache_saves,
            "cacados": cacados,
            "revisar": revisar_count,
        }

        job_done(job_id, data, filename, stats=stats)

    except Exception as e:
        job_set(job_id, status="error", percent=0, message=f"Falhou: {e}")

# ================== Routes ==================
@app.get("/")
def home():
    return render_template_string(HTML)

@app.post("/start")
def start():
    pwd = (request.form.get("password") or "").strip()
    if not pwd or pwd != APP_PASSWORD:
        abort(401, "Senha inválida.")

    text = request.form.get("text") or ""
    job_id = str(uuid.uuid4())
    job_set(
        job_id,
        status="running",
        percent=0,
        message="Preparando lista...",
        created_at=datetime.now().isoformat(timespec="seconds"),
    )

    t = threading.Thread(target=run_job, args=(job_id, text), daemon=True)
    t.start()
    return {"job_id": job_id}

@app.get("/status/<job_id>")
def status(job_id):
    j = job_get(job_id)
    if not j:
        return {"status": "error", "percent": 0, "message": "Job não encontrado."}, 404
    return {
        "status": j.get("status", "running"),
        "percent": j.get("percent", 0),
        "message": j.get("message", ""),
        "stats": j.get("stats", {}),
    }

@app.get("/download/<job_id>")
def download(job_id):
    j = job_get(job_id)
    if not j or j.get("status") != "done":
        abort(404, "Ainda não finalizou.")
    data = j.get("csv_bytes")
    filename = j.get("filename", f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
