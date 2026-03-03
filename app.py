import os
import io
import re
import csv
import time
import requests
from datetime import datetime
from flask import Flask, request, send_file, render_template_string, abort

app = Flask(__name__)

# ================== CONFIG ==================
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")

USER_AGENT = "rota-privada-web/6.0"
COUNTRY = "Brazil"

# Mais rápido, mas ainda “educado” com o Nominatim (uso privado)
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.65"))

# Área aproximada de Manaus (Oeste, Sul, Leste, Norte)
MANAUS_BOX = (-60.30, -3.25, -59.80, -2.85)

# Cache persistente (mesma pasta do app)
GEOCODE_CACHE_FILE = "geocode_cache.csv"
VIACEP_CACHE_FILE = "viacep_cache.csv"

# ================== HTML ==================
HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Roteirizador Privado v6.0</title>
<style>
  body { font-family: Arial, sans-serif; max-width: 980px; margin: 26px auto; }
  textarea { width: 100%; height: 380px; font-size: 14px; }
  input, button { font-size: 16px; padding: 10px; }
  .row { display:flex; gap:12px; align-items:center; margin: 12px 0; }
  .hint { color:#555; }
  code { background:#f3f3f3; padding:2px 6px; border-radius:6px; }
</style>
</head>
<body>
<h2>Roteirizador Privado v6.0</h2>
<p class="hint">
Cole a lista bagunçada. A v6.0 aceita CEP com traço (ex: <code>69030-570</code>) e junta linha quebrada (rua numa linha, CEP na outra).
</p>

<form method="POST" action="/process">
  <div class="row">
    <label>Senha:</label>
    <input name="password" type="password" required />
    <span class="hint">(privado)</span>
  </div>

  <textarea name="lista" placeholder="Cole aqui a lista completa..."></textarea>

  <div class="row">
    <button type="submit">Gerar CSV</button>
  </div>
</form>
</body>
</html>
"""

# ================== Sessões HTTP ==================
S = requests.Session()
S.headers.update({"User-Agent": USER_AGENT})

# ================== Util ==================
RE_CEP_ANY = re.compile(r"\b(\d{5}-\d{3}|\d{8})\b")
RE_STREET = re.compile(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça)\b", re.IGNORECASE)

def dentro_manaus(lat: float, lon: float) -> bool:
    w, s, e, n = MANAUS_BOX
    return (w <= lon <= e) and (s <= lat <= n)

def normaliza_cep(cep: str) -> str:
    digits = re.sub(r"\D", "", cep or "")
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return cep.strip()

def extrair_cep_any(texto: str):
    m = RE_CEP_ANY.search(texto or "")
    return m.group(1) if m else None

def limpar_linha(texto: str) -> str:
    t = (texto or "").replace("N/A", " ")
    t = re.sub(r"\b\d{9,}\b", " ", t)  # remove telefones longos
    t = re.sub(r"\s+", " ", t).strip()
    return t

def parece_rua(texto: str) -> bool:
    return bool(RE_STREET.search(texto or ""))

def extrair_numero(texto: str) -> str:
    # pega o primeiro número razoável
    m = re.search(r"\b\d+[A-Za-z]?\b", texto or "")
    return m.group() if m else "S/N"

def csv_load(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    out = {}
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                k = (row.get("key") or "").strip()
                if not k:
                    continue
                out[k] = row
    except Exception:
        return {}
    return out

def csv_save(path: str, rows: list, fieldnames: list):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)

def geocode_cache_key(cep_fmt: str, numero: str, base: str) -> str:
    base_n = re.sub(r"\s+", " ", (base or "").strip().lower())
    return f"{cep_fmt}|{numero.strip().upper()}|{base_n}"

# ================== Parser v6.0 ==================
def extrair_enderecos_da_lista(texto: str) -> list[str]:
    """
    Aceita lista bagunçada.
    - CEP pode vir com traço ou sem
    - Se rua estiver numa linha e o CEP na próxima, junta
    - Se CEP vier sozinho numa linha, junta com a última linha 'parece_rua'
    """
    linhas = [limpar_linha(l) for l in (texto or "").splitlines()]
    linhas = [l for l in linhas if l]  # remove vazias

    enderecos = []
    pendente_rua = ""

    for l in linhas:
        cep = extrair_cep_any(l)

        if cep:
            # tem CEP nesta linha
            cep_fmt = normaliza_cep(cep)

            # se a linha já parece endereço completo, usa ela
            if parece_rua(l):
                # junta com pendente, se existir, porque às vezes a rua veio antes
                if pendente_rua and pendente_rua.lower() not in l.lower():
                    combinado = f"{pendente_rua} {l}"
                else:
                    combinado = l
                enderecos.append(combinado.replace(cep, cep_fmt))
                pendente_rua = ""
                continue

            # CEP numa linha "solta": junta com pendente_rua
            if pendente_rua:
                combinado = f"{pendente_rua} {cep_fmt}"
                enderecos.append(combinado)
                pendente_rua = ""
                continue

            # CEP solto sem rua: guarda como "endereço fraco" mesmo (vai virar fallback)
            enderecos.append(cep_fmt)
            pendente_rua = ""
            continue

        # sem CEP
        if parece_rua(l):
            # guarda como candidato para juntar com CEP em seguida
            pendente_rua = l
        else:
            # texto irrelevante, ignora
            continue

    # se sobrou rua sem CEP, deixa de fora (sem CEP não tem como ViaCEP)
    enderecos = [e for e in enderecos if extrair_cep_any(e)]
    return enderecos

# ================== ViaCEP com cache ==================
def viacep_get(cep_fmt: str, viacep_cache: dict) -> dict | None:
    """
    Cache por CEP.
    """
    cep_digits = re.sub(r"\D", "", cep_fmt)
    if len(cep_digits) != 8:
        return None

    if cep_digits in viacep_cache:
        row = viacep_cache[cep_digits]
        # reconstrói dicionário igual ao ViaCEP
        return {
            "cep": row.get("cep", ""),
            "logradouro": row.get("logradouro", ""),
            "bairro": row.get("bairro", ""),
            "localidade": row.get("localidade", ""),
            "uf": row.get("uf", ""),
        }

    url = f"https://viacep.com.br/ws/{cep_digits}/json/"
    r = S.get(url, timeout=15)
    if r.status_code == 200:
        j = r.json()
        if "erro" not in j:
            viacep_cache[cep_digits] = {
                "key": cep_digits,
                "cep": j.get("cep", cep_fmt),
                "logradouro": j.get("logradouro", ""),
                "bairro": j.get("bairro", ""),
                "localidade": j.get("localidade", ""),
                "uf": j.get("uf", ""),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            return j
    return None

# ================== Nominatim ==================
def nominatim_search(q: str) -> tuple[float | None, float | None]:
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": q,
        "format": "json",
        "limit": 1,
        "countrycodes": "br",
    }
    r = S.get(url, params=params, timeout=20)
    time.sleep(SLEEP_NOMINATIM)
    if r.status_code == 200:
        data = r.json() or []
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    return None, None

# ================== Circuit rows ==================
def fallback_row(seq, raw, bairro, cidade, cep_fmt, motivo, logradouro="", numero=""):
    # Agora SEM “CEP sem nome”: sempre manda algo legível no Destination Address
    if logradouro.strip():
        destino = f"{logradouro}, {numero}".strip(", ")
    else:
        destino = raw.strip()

    if destino.strip():
        destino = f"{destino}, {bairro}, {cidade}-AM, {cep_fmt}".replace(" ,", ",").strip()
    else:
        destino = f"{cep_fmt}, {bairro}, {cidade}-AM".replace(" ,", ",").strip()

    return [seq, destino, bairro, cidade, cep_fmt, "", "", f"REVISAO_AUTOMATICA: {motivo}"]

# ================== Flask routes ==================
@app.get("/")
def home():
    return render_template_string(HTML)

@app.post("/process")
def process():
    password = (request.form.get("password") or "").strip()
    if password != APP_PASSWORD:
        abort(401)

    texto = request.form.get("lista") or ""

    # 1) extrai endereços do caos
    enderecos = extrair_enderecos_da_lista(texto)
    if not enderecos:
        return "Não encontrei endereços com CEP (8 dígitos, com ou sem traço).", 400

    # 2) dedup (acelera MUITO quando lista repete coisas)
    # chave = cep+linha normalizada
    def k_dedup(s): 
        cep = normaliza_cep(extrair_cep_any(s) or "")
        base = re.sub(r"\s+", " ", (s or "").lower()).strip()
        return f"{cep}|{base}"
    unique_map = {}
    ordered_keys = []
    for s in enderecos:
        k = k_dedup(s)
        if k not in unique_map:
            unique_map[k] = s
            ordered_keys.append(k)

    # caches
    geo_cache = csv_load(GEOCODE_CACHE_FILE)          # key -> {lat,lon,updated_at}
    viacep_cache = csv_load(VIACEP_CACHE_FILE)        # cepdigits -> row

    ok_rows = []
    revisar_rows = []

    # 3) processa únicos e depois expande pro mesmo Sequence (Circuit aceita sequência como você manda)
    seq = 0
    for k in ordered_keys:
        raw = unique_map[k]
        seq += 1

        cep_raw = extrair_cep_any(raw) or ""
        cep_fmt = normaliza_cep(cep_raw)
        cep_digits = re.sub(r"\D", "", cep_fmt)

        dados = viacep_get(cep_fmt, viacep_cache)
        if not dados:
            revisar_rows.append([raw, "CEP_INVALIDO"])
            ok_rows.append(fallback_row(seq, raw, "", "Manaus", cep_fmt, "CEP_INVALIDO"))
            continue

        cidade = (dados.get("localidade") or "Manaus").strip()
        uf = (dados.get("uf") or "AM").strip()
        bairro = (dados.get("bairro") or "").strip()
        logradouro = (dados.get("logradouro") or "").strip()
        numero = extrair_numero(raw)

        # valida manaos via viacep
        if cidade.lower() != "manaus":
            revisar_rows.append([raw, f"CEP_FORA_MANAUS({cidade})"])
            ok_rows.append(fallback_row(seq, raw, bairro, "Manaus", cep_fmt, "CEP_FORA_MANAUS", logradouro, numero))
            continue

        # cache geocode (muito mais agressivo)
        base_for_key = logradouro if logradouro else raw
        gkey = geocode_cache_key(cep_fmt, numero, base_for_key)

        if gkey in geo_cache:
            try:
                lat = float(geo_cache[gkey].get("lat", ""))
                lon = float(geo_cache[gkey].get("lon", ""))
                if dentro_manaus(lat, lon):
                    ok_rows.append([seq, f"{logradouro}, {numero}".strip(", "), bairro, "Manaus", cep_fmt, lat, lon, "CACHE"])
                    continue
            except Exception:
                pass

        # consultas (menos tentativas = mais rápido; suficiente pra uso privado)
        # tenta oficial (ViaCEP) primeiro; se logradouro vazio, tenta raw
        queries = []
        if logradouro:
            queries.append(f"{logradouro}, {numero}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")
        queries.append(f"{raw}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")

        lat = lon = None
        for q in queries:
            lat, lon = nominatim_search(q)
            if lat is not None and dentro_manaus(lat, lon):
                break
            lat = lon = None

        if lat is None or lon is None:
            revisar_rows.append([raw, "NAO_ENCONTRADO"])
            ok_rows.append(fallback_row(seq, raw, bairro, "Manaus", cep_fmt, "NAO_ENCONTRADO", logradouro, numero))
            continue

        # sucesso
        ok_rows.append([seq, f"{logradouro}, {numero}".strip(", "), bairro, "Manaus", cep_fmt, lat, lon, ""])

        # salva cache
        geo_cache[gkey] = {
            "key": gkey,
            "lat": f"{lat}",
            "lon": f"{lon}",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

    # salva caches no disco
    csv_save(
        GEOCODE_CACHE_FILE,
        list(geo_cache.values()),
        fieldnames=["key", "lat", "lon", "updated_at"]
    )
    csv_save(
        VIACEP_CACHE_FILE,
        list(viacep_cache.values()),
        fieldnames=["key", "cep", "logradouro", "bairro", "localidade", "uf", "updated_at"]
    )

    # monta CSV pro Circuit
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
    w.writerows(ok_rows)

    data = out.getvalue().encode("utf-8")
    filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
