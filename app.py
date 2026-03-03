import os
import io
import re
import csv
import time
import json
import requests
import difflib
from datetime import datetime
from flask import Flask, request, send_file, render_template_string, abort

# =========================================
# ROTEIRIZADOR PRIVADO - WEB v6.0
# Cola texto bagunçado -> baixa CSV (Circuit)
# =========================================

# ===== SSL FIX (Windows / alguns ambientes) =====
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
except Exception:
    pass

app = Flask(__name__)

# ===== CONFIG =====
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")  # troque no Render em Environment
USER_AGENT = os.environ.get("USER_AGENT", "rota-privada-web/6.0")
COUNTRY = "Brazil"

# Nominatim: seja educado. Pode ajustar no Render (Environment)
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.70"))
TIMEOUT_HTTP = int(os.environ.get("TIMEOUT_HTTP", "25"))

# Caixa aproximada Manaus (ajuste se quiser)
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)  # (west, south, east, north)

# Cache persistente (Render: fica no disco do serviço enquanto existir)
GEOCODE_CACHE_FILE = os.environ.get("GEOCODE_CACHE_FILE", "geocode_cache.csv")
VIACEP_CACHE_FILE = os.environ.get("VIACEP_CACHE_FILE", "viacep_cache.csv")

# ===== HTML (simples e funcional) =====
HTML = """
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Roteirizador Privado v6.0</title>
  <style>
    body{font-family:Arial, sans-serif; max-width:980px; margin:24px auto; padding:0 14px;}
    .card{border:1px solid #ddd; border-radius:12px; padding:16px; box-shadow:0 2px 10px rgba(0,0,0,.04);}
    textarea{width:100%; height:360px; padding:12px; border-radius:10px; border:1px solid #ccc; font-family:monospace; font-size:13px;}
    input[type=password]{padding:10px; border-radius:10px; border:1px solid #ccc; width:240px;}
    button{padding:12px 16px; border-radius:10px; border:none; cursor:pointer;}
    .row{display:flex; gap:12px; align-items:center; flex-wrap:wrap;}
    .muted{color:#666; font-size:13px;}
    .warn{color:#b45309;}
    .ok{color:#166534;}
    .footer{margin-top:10px; font-size:12px; color:#888;}
    .pill{display:inline-block; padding:4px 10px; border-radius:999px; background:#f3f4f6; font-size:12px;}
  </style>
</head>
<body>
  <h2>Roteirizador Privado <span class="pill">v6.0</span></h2>
  <div class="card">
    <p class="muted">
      Cole o texto bagunçado (Shopee/Loggi/mercado livre etc). Eu vou pescar os endereços e gerar um CSV pronto pro <b>Circuit</b>.
      <br><span class="warn">Dica:</span> se tiver CEP + rua na mesma linha, melhor. Mas eu também tento juntar linhas quebradas.
    </p>

    <form method="POST" action="/process">
      <div class="row">
        <label>Senha:</label>
        <input name="password" type="password" placeholder="APP_PASSWORD" required />
        <button type="submit">Gerar CSV</button>
      </div>

      <p class="muted">Cole aqui:</p>
      <textarea name="text" placeholder="Cole aqui sua lista bagunçada..."></textarea>

      <div class="footer">
        Saída: <b>circuit_import_*.csv</b> (download automático). Se algo ficar duvidoso, vai em Notes com motivo.
      </div>
    </form>
  </div>
</body>
</html>
"""

# ===== Helpers =====
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

def formata_cep(cep: str) -> str:
    c = re.sub(r"\D", "", cep or "")
    return f"{c[:5]}-{c[5:]}" if len(c) == 8 else (cep or "")

def extrair_cep(texto: str):
    m = re.search(r"\b(\d{5}-?\d{3})\b", texto or "")
    if not m:
        return None
    return re.sub(r"\D", "", m.group(1))

def extrair_numero(texto: str) -> str:
    # pega número "mais provável" perto de vírgula ou após rua/av
    t = texto or ""
    m = re.search(r"(?:,|\s)\s*(\d+[A-Za-z]?)\b", t)
    if m:
        return m.group(1)
    # fallback simples
    m2 = re.search(r"\b(\d+[A-Za-z]?)\b", t)
    return m2.group(1) if m2 else "S/N"

def limpar_linha(texto: str) -> str:
    t = (texto or "").replace("N/A", " ")
    # remove sequências gigantes (código, telefone, tracking)
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

def carregar_cache_csv(path: str, fields):
    data = {}
    if not os.path.exists(path):
        return data
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                k = row.get(fields[0], "")
                if not k:
                    continue
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

# ===== HTTP sessions (mais rápido) =====
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

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
    # dorme só se realmente consultou nominatim (cache hit não dorme)
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
    # tenta pegar até 5 e escolhe o melhor por similaridade
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
    # fallback SEM lat/lon, mas com endereço decente pro Circuit geocodar bem
    partes = []
    if logradouro:
        if numero and numero != "S/N":
            partes.append(f"{logradouro}, {numero}")
        else:
            partes.append(logradouro)
    if bairro:
        partes.append(bairro)
    partes.append(f"Manaus-AM")
    if cep_fmt:
        partes.append(cep_fmt)
    return " - ".join(partes)

def extrair_enderecos_do_texto(texto: str):
    """
    Pega texto bagunçado e tenta montar endereços mesmo quando:
      - rua/av em uma linha e CEP na linha seguinte
      - CEP aparece separado
    Estratégia:
      - lê linhas não vazias
      - junta pedaços próximos quando faz sentido (janela curta)
    """
    linhas = [limpar_linha(x) for x in (texto or "").splitlines()]
    linhas = [x for x in linhas if x.strip()]

    enderecos = []
    i = 0
    while i < len(linhas):
        ln = linhas[i]
        cep = extrair_cep(ln)

        # caso 1: linha já tem via + cep
        if cep and tem_palavra_de_via(ln):
            enderecos.append(ln)
            i += 1
            continue

        # caso 2: tem via, mas sem cep -> tenta buscar cep nas próximas 1-2 linhas
        if tem_palavra_de_via(ln) and not cep:
            combinado = ln
            achou = None
            for j in range(1, 3):
                if i + j >= len(linhas):
                    break
                cand = linhas[i + j]
                cep2 = extrair_cep(cand)
                # se achou cep numa linha curta, junta
                if cep2:
                    combinado = f"{ln} {cand}"
                    achou = cep2
                    break
            if achou:
                enderecos.append(combinado)
                i += 1
                continue

        # caso 3: linha tem cep mas não tem via -> tenta colar com linha anterior se ela tem via
        if cep and not tem_palavra_de_via(ln) and i > 0:
            prev = linhas[i - 1]
            if tem_palavra_de_via(prev) and not extrair_cep(prev):
                enderecos.append(f"{prev} {ln}")
                i += 1
                continue

        i += 1

    # dedup simples mantendo ordem
    seen = set()
    out = []
    for e in enderecos:
        key = normaliza(e)
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out

# ===== Routes =====
@app.get("/")
def home():
    return render_template_string(HTML)

@app.post("/process")
def process():
    pwd = (request.form.get("password") or "").strip()
    if not pwd or pwd != APP_PASSWORD:
        abort(401, "Senha inválida.")

    text = request.form.get("text") or ""
    enderecos = extrair_enderecos_do_texto(text)
    total = len(enderecos)
    if total == 0:
        abort(400, "Não achei endereços no texto. Dica: precisa ter CEP ou via (Rua/Av/Travessa...).")

    # caches
    geocode_cache_raw = carregar_cache_csv(GEOCODE_CACHE_FILE, ["key"])
    geocode_cache = {}
    for k, row in geocode_cache_raw.items():
        try:
            geocode_cache[k] = {
                "lat": float(row.get("lat", "")),
                "lon": float(row.get("lon", "")),
                "updated_at": row.get("updated_at", "")
            }
        except Exception:
            continue

    viacep_cache_raw = carregar_cache_csv(VIACEP_CACHE_FILE, ["cep"])
    viacep_cache = {}
    for cep, row in viacep_cache_raw.items():
        viacep_cache[re.sub(r"\D", "", cep)] = dict(row)

    ok_rows = []
    revisar_count = 0
    cache_hits = 0
    cache_saves = 0
    cacados = 0

    inicio = datetime.now()

    for seq, raw in enumerate(enderecos, start=1):
        raw = limpar_linha(raw)
        cep8 = extrair_cep(raw)
        if not cep8:
            revisar_count += 1
            # sem cep, ainda dá pra mandar “cru” pro Circuit tentar, mas vira loteria
            ok_rows.append([seq, raw, "", "Manaus", "", "", "", "REVISAO_AUTOMATICA: SEM_CEP"])
            continue

        dados = viacep_get(cep8, viacep_cache)
        if not dados:
            revisar_count += 1
            ok_rows.append([seq, raw, "", "Manaus", formata_cep(cep8), "", "", f"REVISAO_AUTOMATICA: CEP_INVALIDO ({cep8})"])
            continue

        cidade = (dados.get("localidade") or "").strip()
        uf = (dados.get("uf") or "AM").strip()
        if normaliza(cidade) != "manaus":
            revisar_count += 1
            ok_rows.append([seq, raw, (dados.get("bairro") or ""), cidade or "?", formata_cep(cep8), "", "", f"REVISAO_AUTOMATICA: CEP_FORA_MANAUS ({cidade})"])
            continue

        logradouro = (dados.get("logradouro") or "").strip()
        bairro = (dados.get("bairro") or "").strip()
        numero = extrair_numero(raw)
        cep_fmt = formata_cep(cep8)

        # cache key
        k = cache_key(cep_fmt, numero, logradouro, raw)
        if k in geocode_cache:
            lat = geocode_cache[k]["lat"]
            lon = geocode_cache[k]["lon"]
            if dentro_de_manaus(lat, lon):
                cache_hits += 1
                ok_rows.append([seq, f"{logradouro}, {numero}".strip(", "), bairro, "Manaus", cep_fmt, lat, lon, "CACHE"])
                continue

        lat = lon = None
        note = ""

        # Queries mais “curtas e certeiras” (menos tentativa = mais rápido)
        queries = []
        if logradouro:
            # 1) oficial do ViaCEP (tende a ser mais limpo)
            if numero and numero != "S/N":
                queries.append(f"{logradouro}, {numero}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")
            queries.append(f"{logradouro}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")

        # 2) o texto original (às vezes tem referência útil)
        queries.append(f"{raw}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")

        # tenta bounded primeiro
        for q in queries:
            lat1, lon1, _d1 = tentar_1_resultado(q, bounded=True)
            if lat1 is not None and dentro_de_manaus(lat1, lon1):
                lat, lon = lat1, lon1
                break

        # se falhou, tenta solto
        if lat is None:
            for q in queries:
                lat1, lon1, _d1 = tentar_1_resultado(q, bounded=False)
                if lat1 is not None and dentro_de_manaus(lat1, lon1):
                    lat, lon = lat1, lon1
                    break

        # se ainda falhou, caça nome antigo
        if lat is None:
            lat3, lon3, display3, score3 = caca_nome_antigo(raw, bairro, "Manaus", uf, cep_fmt)
            if lat3 is not None and dentro_de_manaus(lat3, lon3):
                lat, lon = lat3, lon3
                cacados += 1
                note = f"CACADO(score={score3:.2f}): {display3[:90]}"

        # se ainda falhou: fallback bom (logradouro+numero+bairro+cep) e SEM lat/lon
        if lat is None or lon is None:
            revisar_count += 1
            destino = montar_fallback_endereco(logradouro, numero, bairro, cep_fmt)
            ok_rows.append([seq, destino, bairro, "Manaus", cep_fmt, "", "", f"REVISAO_AUTOMATICA: FALLBACK_SEM_COORD | original: {raw}"])
            continue

        # salva OK
        ok_rows.append([seq, f"{logradouro}, {numero}".strip(", "), bairro, "Manaus", cep_fmt, lat, lon, note])

        geocode_cache[k] = {"lat": lat, "lon": lon, "updated_at": datetime.now().isoformat(timespec="seconds")}
        cache_saves += 1

    # salvar caches
    salvar_cache_geocode(geocode_cache)
    salvar_cache_viacep(viacep_cache)

    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()

    # CSV pro Circuit
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
    w.writerows(ok_rows)

    data = out.getvalue().encode("utf-8")
    filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # (extra) log no servidor (Render logs)
    print("===== v6.0 RESULT =====")
    print(f"Total detectado: {total}")
    print(f"Tempo: {duracao:.1f}s | Cache hits: {cache_hits} | Cache saves: {cache_saves} | Caçados: {cacados} | Revisar: {revisar_count}")
    print("=======================")

    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
