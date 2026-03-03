import os
import io
import re
import csv
import time
import requests
from datetime import datetime
from flask import Flask, request, send_file, render_template_string, abort

app = Flask(__name__)

APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")
USER_AGENT = "rota-privada-web"
SLEEP_SECONDS = 1.1

# Área aproximada de Manaus
MANAUS_BOX = (-60.30, -3.25, -59.80, -2.85)

HTML = """
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Roteirizador Privado</title>
<style>
body { font-family: Arial; max-width:900px; margin:30px auto; }
textarea { width:100%; height:350px; }
input, button { padding:10px; font-size:16px; }
.row { margin:15px 0; }
</style>
</head>
<body>
<h2>Roteirizador Privado</h2>
<form method="POST" action="/process">
<div class="row">
<label>Senha:</label>
<input name="password" type="password" required>
</div>
<textarea name="lista" placeholder="Cole aqui a lista da Shopee..."></textarea>
<div class="row">
<button type="submit">Gerar CSV</button>
</div>
</form>
</body>
</html>
"""

def dentro_manaus(lat, lon):
    w, s, e, n = MANAUS_BOX
    return w <= lon <= e and s <= lat <= n

def extrair_cep(texto):
    m = re.search(r"\b\d{8}\b", texto)
    return m.group() if m else None

def extrair_numero(texto):
    m = re.search(r"\b\d+[A-Za-z]?\b", texto)
    return m.group() if m else "S/N"

def buscar_viacep(cep):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    r = requests.get(url, timeout=15)
    if r.status_code == 200:
        j = r.json()
        if "erro" not in j:
            return j
    return None

def nominatim(q):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": q,
        "format": "json",
        "limit": 1,
        "countrycodes": "br"
    }
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, params=params, headers=headers, timeout=20)
    time.sleep(SLEEP_SECONDS)
    if r.status_code == 200 and r.json():
        lat = float(r.json()[0]["lat"])
        lon = float(r.json()[0]["lon"])
        return lat, lon
    return None, None

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/process", methods=["POST"])
def process():
    if request.form.get("password") != APP_PASSWORD:
        abort(401)

    texto = request.form.get("lista", "")
    linhas = [l.strip() for l in texto.splitlines() if l.strip()]
    enderecos = [l for l in linhas if extrair_cep(l)]

    if not enderecos:
        return "Nenhum endereço com CEP encontrado.", 400

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Sequence","Destination Address","Bairro","City","Zipcode/Postal Code","Latitude","Longitude","Notes"])

    for i, raw in enumerate(enderecos, 1):
        cep = extrair_cep(raw)
        numero = extrair_numero(raw)

        dados = buscar_viacep(cep)
        if not dados:
            writer.writerow([i, raw, "", "Manaus", cep, "", "", "REVISAO_AUTOMATICA: CEP_INVALIDO"])
            continue

        logradouro = dados.get("logradouro","")
        bairro = dados.get("bairro","")
        cidade = dados.get("localidade","Manaus")

        consulta = f"{logradouro}, {numero}, {bairro}, {cidade}-AM, {cep}"
        lat, lon = nominatim(consulta)

        if lat and dentro_manaus(lat, lon):
            writer.writerow([i, consulta, bairro, cidade, cep, lat, lon, ""])
        else:
            # fallback
            destino = f"{logradouro}, {numero}, {bairro}, Manaus-AM, {cep}"
            writer.writerow([i, destino, bairro, "Manaus", cep, "", "", "REVISAO_AUTOMATICA"])

    data = output.getvalue().encode("utf-8")
    filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    