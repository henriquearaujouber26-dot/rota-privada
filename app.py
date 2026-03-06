import os
import io
import re
import csv
from flask import Flask, request, send_file, render_template_string, abort

app = Flask(__name__)

APP_VERSION = "6.1.1-min"
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")

HTML = f"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8"/>
  <title>Roteirizador Privado v{APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    body {{ font-family: Arial, sans-serif; background:#fff; margin:0; padding:0; }}
    .wrap {{ max-width:950px; margin:38px auto; padding:0 16px; }}
    h1 {{ text-align:center; }}
    .badge {{ font-size:12px; background:#eee; border-radius:999px; padding:3px 8px; }}
    .card {{ border:1px solid #ddd; border-radius:12px; padding:18px; }}
    textarea {{ width:100%; min-height:360px; margin-top:12px; border:1px solid #ccc; border-radius:10px; padding:12px; font:13px Consolas,monospace; }}
    input[type=password] {{ padding:10px; border:1px solid #ccc; border-radius:10px; width:260px; }}
    button {{ padding:10px 16px; border:0; border-radius:10px; background:#111; color:#fff; font-weight:bold; cursor:pointer; }}
    .row {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-top:12px; }}
    .hint {{ font-size:13px; color:#444; line-height:1.5; }}
    .small {{ font-size:12px; color:#666; margin-top:8px; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>Roteirizador Privado <span class="badge">v{APP_VERSION}</span></h1>
  <div class="card">
    <div class="hint">
      Aceita:
      <br>• nome + endereço + CEP em linhas separadas
      <br>• endereço com CEP na mesma linha
      <br>• CEP com ou sem "CEP:"
    </div>

    <form method="post" action="/process">
      <div class="row">
        <label><b>Senha:</b></label>
        <input name="password" type="password" required>
        <button type="submit">Gerar CSV</button>
      </div>

      <label style="display:block; margin-top:14px;"><b>Cole aqui:</b></label>
      <textarea name="text" required></textarea>
    </form>

    <div class="small">Saída: <b>circuit_import_site.csv</b></div>
  </div>
</div>
</body>
</html>
"""

def limpar(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def extrair_cep(texto: str):
    m = re.search(r"(?:\bcep\b\s*:\s*)?(\d{5})[-\s]?(\d{3})", texto or "", flags=re.I)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"

def linha_tem_endereco(texto: str):
    t = (texto or "").lower()
    if re.search(r"\b(rua|r\b|avenida|av\b|travessa|beco|estrada|alameda|praça|praca|rodovia|rio)\b", t):
        return True
    if re.search(r",\s*\d{1,6}\b", texto or "") and "manaus" in t:
        return True
    return False

def is_nome(texto: str):
    if not texto:
        return False
    if extrair_cep(texto):
        return False
    if linha_tem_endereco(texto):
        return False
    if re.search(r"\d", texto):
        return False
    return len(texto.split()) >= 2

def extrair_bairro(endereco: str):
    partes = [p.strip() for p in re.split(r"\s-\s", endereco or "")]
    if len(partes) >= 2:
        return partes[1]
    return ""

def parse_lista(texto: str):
    linhas = [limpar(l) for l in (texto or "").splitlines() if limpar(l)]
    entregas = []
    i = 0

    while i < len(linhas):
        linha = linhas[i]

        # modelo nome / endereço / cep
        if is_nome(linha) and i + 2 < len(linhas):
            nome = linha
            endereco = linhas[i + 1]
            cep = extrair_cep(linhas[i + 2]) or extrair_cep(endereco)

            if linha_tem_endereco(endereco) and cep:
                entregas.append({
                    "nome": nome,
                    "endereco": endereco,
                    "bairro": extrair_bairro(endereco),
                    "cep": cep
                })
                i += 3
                continue

        # modelo endereço / cep
        if linha_tem_endereco(linha):
            cep = extrair_cep(linha)
            if not cep and i + 1 < len(linhas):
                cep = extrair_cep(linhas[i + 1])

            if cep:
                entregas.append({
                    "nome": "",
                    "endereco": linha,
                    "bairro": extrair_bairro(linha),
                    "cep": cep
                })
                i += 2
                continue

        # modelo rua / bairro / cep
        if i + 2 < len(linhas):
            l1, l2, l3 = linhas[i], linhas[i + 1], linhas[i + 2]
            cep = extrair_cep(l3)
            if cep and (linha_tem_endereco(l1) or re.search(r",\s*\d", l1)):
                endereco = f"{l1} - {l2} - MANAUS/AM"
                entregas.append({
                    "nome": "",
                    "endereco": endereco,
                    "bairro": l2,
                    "cep": cep
                })
                i += 3
                continue

        i += 1

    return entregas

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

    entregas = parse_lista(text)

    if not entregas:
        abort(400, "Não achei endereços. Pode colar no formato nome + endereço + CEP em linhas separadas.")

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])

    for i, e in enumerate(entregas, start=1):
        w.writerow([
            i,
            e["endereco"],
            e["bairro"],
            "Manaus",
            e["cep"],
            "",
            "",
            e["nome"]
        ])

    data = out.getvalue().encode("utf-8")

    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name="circuit_import_site.csv"
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
