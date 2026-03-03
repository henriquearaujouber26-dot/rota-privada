import os
import io
import re
import csv
import time
import json
import uuid
import threading
import requests
from datetime import datetime

# =========================
# SSL FIX (Windows)
# =========================
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
except Exception:
    pass

# =========================
# CONFIG
# =========================
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")
USER_AGENT = os.environ.get("USER_AGENT", "rota-privada-web/6.0.2")
COUNTRY = "Brazil"

# Aumenta/baixa pra ficar mais rápido sem abusar do Nominatim
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.85"))

# Caixa aproximada de Manaus (Oeste, Sul, Leste, Norte)
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)

# Cache
GEOCODE_CACHE_FILE = os.environ.get("GEOCODE_CACHE_FILE", "geocode_cache.csv")
VIACEP_CACHE_FILE = os.environ.get("VIACEP_CACHE_FILE", "viacep_cache.csv")

# Pasta dos JOBS (use /var/data/jobs com Render Disk; fallback /tmp)
JOB_DIR = os.environ.get("JOB_DIR", "/var/data/jobs")
os.makedirs(JOB_DIR, exist_ok=True)

# =========================
# HELPERS
# =========================
def dentro_de_manaus(lat, lon):
    west, south, east, north = MANAUS_VIEWBOX
    return (west <= lon <= east) and (south <= lat <= north)

def normaliza(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def formata_cep(cep: str) -> str:
    c = re.sub(r"\D", "", cep or "")
    if len(c) == 8:
        return f"{c[:5]}-{c[5:]}"
    return cep or ""

def extrair_cep(texto: str):
    m = re.search(r"\b\d{8}\b", texto or "")
    return m.group() if m else None

def extrair_numero(texto: str):
    # tenta achar número de casa "123" ou "123A"
    m = re.search(r"\b(\d+[A-Za-z]?)\b", texto or "")
    return m.group(1) if m else "S/N"

def limpar_texto_endereco(texto: str) -> str:
    t = (texto or "").replace("N/A", " ")
    # remove códigos grandes (tracking, cpf etc) com 9+ dígitos
    t = re.sub(r"\b\d{9,}\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def linha_parece_endereco(texto: str) -> bool:
    if not texto:
        return False
    via = re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|praca|rodovia)\b", texto.lower())
    cep = re.search(r"\b\d{8}\b", texto)
    return bool(via and cep)

def extrair_enderecos_do_texto_grande(texto_grande: str):
    linhas = []
    for ln in (texto_grande or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        ln = limpar_texto_endereco(ln)
        if linha_parece_endereco(ln):
            linhas.append(ln)
    # se vier duplicado, mantém (Circuit aceita, e às vezes são pacotes diferentes)
    return linhas

# =========================
# VIA CEP (com cache)
# =========================
def viacep_cache_load():
    cache = {}
    if not os.path.exists(VIACEP_CACHE_FILE):
        return cache
    try:
        with open(VIACEP_CACHE_FILE, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                cep = (row.get("cep") or "").strip()
                if cep:
                    cache[cep] = row
    except Exception:
        return {}
    return cache

def viacep_cache_save(cache: dict):
    fields = ["cep", "logradouro", "bairro", "localidade", "uf", "ibge", "gia", "ddd", "siafi", "updated_at"]
    with open(VIACEP_CACHE_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for cep, row in cache.items():
            out = {k: (row.get(k, "") if isinstance(row, dict) else "") for k in fields}
            out["cep"] = cep
            if not out.get("updated_at"):
                out["updated_at"] = datetime.now().isoformat(timespec="seconds")
            w.writerow(out)

def buscar_viacep(cep: str, cache: dict):
    cep_num = re.sub(r"\D", "", cep or "")
    if len(cep_num) != 8:
        return None, False

    cep_fmt = formata_cep(cep_num)
    if cep_fmt in cache:
        return cache[cep_fmt], True

    url = f"https://viacep.com.br/ws/{cep_num}/json/"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            j = r.json()
            if "erro" not in j:
                j2 = dict(j)
                j2["cep"] = formata_cep(j2.get("cep", cep_num))
                j2["updated_at"] = datetime.now().isoformat(timespec="seconds")
                cache[j2["cep"]] = j2
                return j2, False
    except Exception:
        pass

    return None, False

# =========================
# GEOCODE CACHE (lat/lon)
# =========================
def geocode_cache_key(cep_fmt: str, numero: str, logradouro: str, raw: str) -> str:
    base = normaliza(logradouro)
    if not base:
        # fallback: sem cep, limpa e normaliza
        base = normaliza(re.sub(r"\b\d{8}\b", "", raw or ""))
    return f"{cep_fmt}|{(numero or '').upper()}|{base}"

def geocode_cache_load():
    cache = {}
    if not os.path.exists(GEOCODE_CACHE_FILE):
        return cache
    try:
        with open(GEOCODE_CACHE_FILE, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                k = (row.get("key") or "").strip()
                if not k:
                    continue
                try:
                    lat = float(row.get("lat", ""))
                    lon = float(row.get("lon", ""))
                except Exception:
                    continue
                cache[k] = (lat, lon, row.get("updated_at", ""))
    except Exception:
        return {}
    return cache

def geocode_cache_save(cache: dict):
    with open(GEOCODE_CACHE_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["key", "lat", "lon", "updated_at"])
        for k, (lat, lon, ts) in cache.items():
            w.writerow([k, lat, lon, ts])

# =========================
# NOMINATIM
# =========================
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
        # Nominatim: viewbox = left,top,right,bottom
        params["viewbox"] = f"{west},{north},{east},{south}"
        params["bounded"] = 1

    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=25)
    except Exception:
        time.sleep(SLEEP_NOMINATIM)
        return []

    time.sleep(SLEEP_NOMINATIM)
    if r.status_code == 200:
        try:
            return r.json() or []
        except Exception:
            return []
    return []

def tentar_1_resultado(query: str, bounded=True):
    data = nominatim_search(query, bounded=bounded, limit=1)
    if not data:
        return None, None
    try:
        lat = float(data[0].get("lat"))
        lon = float(data[0].get("lon"))
        return lat, lon
    except Exception:
        return None, None

# =========================
# JOB STORE (arquivo)
# =========================
def _job_path(job_id: str) -> str:
    return os.path.join(JOB_DIR, f"{job_id}.json")

def _job_csv_path(job_id: str) -> str:
    return os.path.join(JOB_DIR, f"{job_id}.csv")

def job_save(job_id: str, data: dict):
    tmp = _job_path(job_id) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, _job_path(job_id))

def job_load(job_id: str):
    p = _job_path(job_id)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def job_write_csv(job_id: str, rows: list):
    p = _job_csv_path(job_id)
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
        w.writerows(rows)

# =========================
# PROCESSAMENTO PRINCIPAL
# =========================
def adiciona_fallback_por_cep(ok_rows, seq, raw, bairro, cidade, cep_fmt, motivo):
    # Circuit: se não tiver lat/lon ele ainda resolve por texto (mas pode espalhar)
    # Então a gente põe destino como "CEP, Manaus-AM" e anota motivo
    ok_rows.append([
        seq,
        f"{cep_fmt}, Manaus-AM",
        bairro,
        cidade,
        cep_fmt,
        "",
        "",
        f"REVISAO_AUTOMATICA: {motivo} | original: {raw}"
    ])

def processar_texto_em_job(job_id: str, texto_grande: str):
    # status inicial
    job_save(job_id, {
        "status": "running",
        "percent": 0,
        "done": 0,
        "total": 0,
        "msg": "Iniciando...",
        "created_at": datetime.now().isoformat(timespec="seconds")
    })

    enderecos = extrair_enderecos_do_texto_grande(texto_grande)
    total = len(enderecos)
    if total == 0:
        job_save(job_id, {
            "status": "error",
            "percent": 0,
            "done": 0,
            "total": 0,
            "msg": "Não detectei nenhum endereço. Dica: precisa ter (Rua/Av/Travessa/Beco...) e CEP de 8 dígitos na mesma linha.",
        })
        return

    # carrega caches
    viacep_cache = viacep_cache_load()
    geocode_cache = geocode_cache_load()

    ok_rows = []
    revisar_rows = []
    cache_hits = 0
    cache_saves = 0

    inicio = datetime.now()
    cidade = "Manaus"
    uf = "AM"

    # atualiza job total
    job_save(job_id, {
        "status": "running",
        "percent": 0,
        "done": 0,
        "total": total,
        "msg": "Processando...",
    })

    for i, raw in enumerate(enderecos, start=1):
        # progresso
        percent = round((i / total) * 100, 1)
        job_save(job_id, {
            "status": "running",
            "percent": percent,
            "done": i,
            "total": total,
            "msg": f"({i}/{total}) {raw[:70]}",
        })

        raw = limpar_texto_endereco(raw)
        cep = extrair_cep(raw)
        numero = extrair_numero(raw)

        if not cep:
            revisar_rows.append([raw, "SEM_CEP"])
            continue

        dados, hit_via = buscar_viacep(cep, viacep_cache)
        if not dados:
            revisar_rows.append([raw, f"CEP_INVALIDO ({cep})"])
            continue

        if (dados.get("localidade", "") or "").strip().lower() != "manaus":
            revisar_rows.append([raw, f"CEP_FORA_MANAUS ({dados.get('localidade', '')})"])
            # mesmo assim gera fallback por cep (pra não sumir no CSV)
            cep_fmt = formata_cep(cep)
            adiciona_fallback_por_cep(ok_rows, i, raw, (dados.get("bairro") or ""), cidade, cep_fmt, "CEP_FORA_MANAUS")
            continue

        logradouro = (dados.get("logradouro") or "").strip()
        bairro = (dados.get("bairro") or "").strip()
        cep_fmt = formata_cep(dados.get("cep") or cep)

        # cache key
        k = geocode_cache_key(cep_fmt, numero, logradouro, raw)
        if k in geocode_cache:
            lat, lon, _ts = geocode_cache[k]
            if dentro_de_manaus(lat, lon):
                cache_hits += 1
                ok_rows.append([i, f"{logradouro}, {numero}".strip(", "), bairro, cidade, cep_fmt, lat, lon, "CACHE"])
                continue

        lat = lon = None
        note = ""

        queries = []
        if logradouro:
            queries.append(f"{logradouro}, {numero}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}")
        queries.append(f"{raw}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}")

        # tenta bounded e depois livre
        for q in queries:
            lat1, lon1 = tentar_1_resultado(q, bounded=True)
            if lat1 is not None and dentro_de_manaus(lat1, lon1):
                lat, lon = lat1, lon1
                note = "OK_BOUNDED"
                break

            lat2, lon2 = tentar_1_resultado(q, bounded=False)
            if lat2 is not None and dentro_de_manaus(lat2, lon2):
                lat, lon = lat2, lon2
                note = "OK_UNBOUNDED"
                break

        # se falhou: fallback por CEP (mas mantém no CSV)
        if lat is None or lon is None:
            revisar_rows.append([raw, "NAO_ENCONTRADO"])
            adiciona_fallback_por_cep(ok_rows, i, raw, bairro, cidade, cep_fmt, "NAO_ENCONTRADO")
            continue

        if not dentro_de_manaus(lat, lon):
            revisar_rows.append([raw, f"FORA_MANAUS ({lat},{lon})"])
            adiciona_fallback_por_cep(ok_rows, i, raw, bairro, cidade, cep_fmt, "FORA_MANAUS")
            continue

        ok_rows.append([i, f"{logradouro}, {numero}".strip(", "), bairro, cidade, cep_fmt, lat, lon, note])

        geocode_cache[k] = (lat, lon, datetime.now().isoformat(timespec="seconds"))
        cache_saves += 1

    # salva caches
    try:
        viacep_cache_save(viacep_cache)
    except Exception:
        pass
    try:
        geocode_cache_save(geocode_cache)
    except Exception:
        pass

    # salva CSV do job
    job_write_csv(job_id, ok_rows)

    # também salva revisar_manual.csv (opcional, mas útil)
    revisar_path = os.path.join(JOB_DIR, f"{job_id}_revisar_manual.csv")
    try:
        with open(revisar_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["endereco_original", "motivo"])
            w.writerows(revisar_rows)
    except Exception:
        pass

    dur = (datetime.now() - inicio).total_seconds()
    job_save(job_id, {
        "status": "done",
        "percent": 100,
        "done": total,
        "total": total,
        "msg": "Finalizado. Baixe o CSV.",
        "stats": {
            "ok": len(ok_rows),
            "revisar": len(revisar_rows),
            "cache_hits": cache_hits,
            "cache_saves": cache_saves,
            "tempo_segundos": round(dur, 1),
        }
    })

# =========================
# FLASK APP
# =========================
from flask import Flask, request, jsonify, send_file

app = Flask(__name__)

HTML = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <title>Roteirizador Privado v6.0.2</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    body { font-family: Arial, sans-serif; background:#fff; margin:0; }
    .wrap { max-width: 920px; margin: 40px auto; padding: 0 16px; }
    h1 { text-align:center; font-weight:700; margin: 0 0 16px; }
    .card { border:1px solid #e5e5e5; border-radius:10px; padding:18px; }
    textarea { width:100%; height:320px; padding:10px; border:1px solid #ddd; border-radius:8px; font-family: monospace; font-size: 13px; }
    input { padding:10px; border:1px solid #ddd; border-radius:8px; }
    button { padding:10px 14px; border:0; border-radius:10px; background:#111; color:#fff; cursor:pointer; }
    button:disabled { opacity: .5; cursor:not-allowed; }
    .row { display:flex; gap:10px; align-items:center; flex-wrap: wrap; }
    .hint { color:#666; font-size:13px; margin: 8px 0 12px; }
    .small { font-size:12px; color:#777; margin-top:10px; }
    .barwrap { margin-top:12px; }
    .bar { width:100%; height:12px; background:#eee; border-radius:999px; overflow:hidden; }
    .bar > div { height:100%; width:0%; background:#111; transition: width .2s; }
    .status { font-size:13px; color:#333; margin-top:6px; }
    .badge { display:inline-block; font-size:12px; padding:2px 8px; border:1px solid #ddd; border-radius:999px; margin-left:8px; color:#555; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Roteirizador Privado <span class="badge">v6.0.2</span></h1>
    <div class="card">
      <div class="hint">
        Cole o texto bagunçado (Shopee/Loggi/etc). Eu vou pescar as linhas que têm <b>Rua/Av/Travessa/Beco + CEP</b> e gerar um CSV pro Circuit.
        <br/>Dica: se CEP e rua estiverem na mesma linha, fica perfeito. Se não, ele ignora.
      </div>

      <div class="row" style="margin-bottom:10px;">
        <label>Senha:</label>
        <input id="pw" type="password" placeholder="APP_PASSWORD"/>
        <button id="btn">Gerar CSV</button>
      </div>

      <label>Cole aqui:</label>
      <textarea id="txt" placeholder="Cole aqui sua lista bagunçada..."></textarea>

      <div class="barwrap" id="barwrap" style="display:none;">
        <div class="status" id="statusLine">Processando...</div>
        <div class="bar"><div id="barfill"></div></div>
        <div class="small" id="statSmall"></div>
      </div>

      <div class="small">Saída: <b>circuit_import_*.csv</b>. Se cair em fallback, vai em Notes com o motivo.</div>
    </div>
  </div>

<script>
const btn = document.getElementById("btn");
const pw = document.getElementById("pw");
const txt = document.getElementById("txt");
const barwrap = document.getElementById("barwrap");
const barfill = document.getElementById("barfill");
const statusLine = document.getElementById("statusLine");
const statSmall = document.getElementById("statSmall");

let pollTimer = null;

function setProgress(pct, msg, small){
  barwrap.style.display = "block";
  barfill.style.width = (pct || 0) + "%";
  statusLine.textContent = msg || "Processando...";
  statSmall.textContent = small || "";
}

async function poll(jobId){
  try{
    const r = await fetch(`/progress?id=${encodeURIComponent(jobId)}`);
    if(!r.ok){
      // 404 => job sumiu
      setProgress(0, "Erro: Job não encontrado (servidor reiniciou/dormiu). Tenta de novo.", "");
      btn.disabled = false;
      return;
    }
    const j = await r.json();
    const pct = j.percent ?? 0;
    const done = j.done ?? 0;
    const total = j.total ?? 0;
    const msg = j.msg ?? "Processando...";
    let small = "";
    if(total){
      small = `${pct}% - (${done}/${total}) processando...`;
    }
    setProgress(pct, msg, small);

    if(j.status === "done"){
      btn.disabled = false;
      // baixa automático
      window.location = `/download?id=${encodeURIComponent(jobId)}`;
      // mostra stats
      if(j.stats){
        statSmall.textContent = `OK: ${j.stats.ok} | Revisar: ${j.stats.revisar} | Cache hits: ${j.stats.cache_hits} | Tempo: ${j.stats.tempo_segundos}s`;
      }
      return;
    }
    if(j.status === "error"){
      btn.disabled = false;
      return;
    }

    pollTimer = setTimeout(()=>poll(jobId), 900);
  }catch(e){
    setProgress(0, "Erro consultando progresso. Tenta de novo.", "");
    btn.disabled = false;
  }
}

btn.addEventListener("click", async ()=>{
  const senha = pw.value.trim();
  const texto = txt.value;

  btn.disabled = true;
  setProgress(0, "Iniciando...", "");

  try{
    const r = await fetch("/start", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ password: senha, text: texto })
    });
    const j = await r.json();
    if(!r.ok){
      btn.disabled = false;
      setProgress(0, "Erro: " + (j.error || "falhou"), "");
      return;
    }
    poll(j.job_id);
  }catch(e){
    btn.disabled = false;
    setProgress(0, "Erro ao iniciar.", "");
  }
});
</script>
</body>
</html>
"""

@app.get("/")
def home():
    return HTML

@app.post("/start")
def start():
    data = request.get_json(silent=True) or {}
    password = (data.get("password") or "").strip()
    text = data.get("text") or ""

    if password != APP_PASSWORD:
        return jsonify({"error": "Senha inválida"}), 401

    job_id = uuid.uuid4().hex

    job_save(job_id, {
        "status": "queued",
        "percent": 0,
        "done": 0,
        "total": 0,
        "msg": "Na fila...",
        "created_at": datetime.now().isoformat(timespec="seconds"),
    })

    # roda em thread pra página não ficar presa
    t = threading.Thread(target=processar_texto_em_job, args=(job_id, text), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})

@app.get("/progress")
def progress():
    job_id = (request.args.get("id") or "").strip()
    j = job_load(job_id)
    if not j:
        return jsonify({"status":"missing"}), 404
    return jsonify(j)

@app.get("/download")
def download():
    job_id = (request.args.get("id") or "").strip()
    j = job_load(job_id)
    if not j:
        return "Job não encontrado.", 404
    if j.get("status") != "done":
        return "Ainda processando.", 400

    csv_path = _job_csv_path(job_id)
    if not os.path.exists(csv_path):
        return "CSV não encontrado.", 404

    # nome amigável
    filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(csv_path, mimetype="text/csv", as_attachment=True, download_name=filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
