import os
import io
import re
import csv
import time
import json
import base64
import difflib
import requests
from datetime import datetime
from flask import Flask, request, Response, abort

# =========================
# SSL fix (Windows) - não atrapalha no Linux
# =========================
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
except Exception:
    pass

app = Flask(__name__)

# =========================
# CONFIG
# =========================
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")
USER_AGENT = os.environ.get("USER_AGENT", "rota-privada-web/6.0A.1")

COUNTRY = "Brazil"

# Mais rápido, mas ainda "educado" com Nominatim (ajuste se precisar)
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.65"))

# Manaus (viewbox aproximado) — segura muito “ponto perdido”
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)  # west, south, east, north

# Timeouts (conexão, leitura) — evita travar infinito
TIMEOUT_VIACEP = (5, 10)
TIMEOUT_NOMINATIM = (5, 12)

# Score mínimo pra aceitar “caça nome antigo”
MIN_SCORE_CACADO = float(os.environ.get("MIN_SCORE_CACADO", "0.42"))

# =========================
# HTML (1 arquivo só)
# =========================
HTML = r"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Roteirizador Privado v6.0A.1</title>
  <style>
    body{font-family:Arial,Helvetica,sans-serif;background:#fff;color:#111;margin:0}
    .wrap{max-width:900px;margin:40px auto;padding:0 18px}
    h1{font-size:26px;margin:0 0 14px 0}
    .card{border:1px solid #ddd;border-radius:10px;padding:16px}
    label{font-weight:700}
    .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
    input[type=password]{padding:10px;border:1px solid #ccc;border-radius:8px;min-width:240px}
    button{padding:10px 14px;border:0;border-radius:8px;background:#111;color:#fff;cursor:pointer}
    button:disabled{opacity:.55;cursor:not-allowed}
    textarea{width:100%;min-height:320px;margin-top:8px;border:1px solid #ccc;border-radius:10px;padding:12px;font-family:ui-monospace,Consolas,monospace;font-size:12px}
    .muted{color:#666;font-size:12px;margin-top:8px}
    .barWrap{margin-top:10px}
    .bar{height:14px;border-radius:999px;background:#eee;overflow:hidden}
    .fill{height:100%;width:0;background:#111;transition:width .12s linear}
    .status{font-family:ui-monospace,Consolas,monospace;font-size:12px;margin-top:6px;color:#333}
    .ok{color:#0a7}
    .warn{color:#c70}
    .err{color:#c00}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Roteirizador Privado <small style="font-size:12px;border:1px solid #ddd;border-radius:999px;padding:3px 8px;color:#333;">v6.0A.1</small></h1>

    <div class="card">
      <div class="muted">
        Cole o texto bagunçado (Shopee/Loggi/Mercado Livre etc). Eu tento achar os endereços e gero um CSV pronto pro Circuit.
        <br><b>Dica:</b> se tiver CEP + rua na mesma linha, melhor. Mas eu também tento juntar linha quebrada.
      </div>

      <div class="row" style="margin-top:10px">
        <div>
          <label>Senha:</label><br/>
          <input id="pwd" type="password" placeholder="APP_PASSWORD" />
        </div>
        <div style="padding-top:18px">
          <button id="btn">Gerar CSV</button>
        </div>
      </div>

      <div>
        <label style="display:block;margin-top:12px">Cole aqui:</label>
        <textarea id="txt" placeholder="Cole aqui sua lista bagunçada..."></textarea>
      </div>

      <div class="barWrap">
        <div class="muted" id="hint">Quando começar, vai aparecer barra de progresso e status (sem drama).</div>
        <div class="bar"><div class="fill" id="fill"></div></div>
        <div class="status" id="status"></div>
      </div>

      <div class="muted">
        Saída: <b>circuit_import_*.csv</b> (download automático). Se cair em fallback, vai em <b>Notes</b> com motivo.
      </div>
    </div>
  </div>

<script>
const btn = document.getElementById("btn");
const txt = document.getElementById("txt");
const pwd = document.getElementById("pwd");
const fill = document.getElementById("fill");
const statusEl = document.getElementById("status");
const hint = document.getElementById("hint");

function setProgress(p){
  fill.style.width = Math.max(0, Math.min(100, p)) + "%";
}

function setStatus(msg, cls){
  statusEl.className = "status " + (cls || "");
  statusEl.textContent = msg || "";
}

function downloadBase64(b64, filename){
  const byteChars = atob(b64);
  const byteNums = new Array(byteChars.length);
  for(let i=0;i<byteChars.length;i++) byteNums[i] = byteChars.charCodeAt(i);
  const blob = new Blob([new Uint8Array(byteNums)], {type:"text/csv;charset=utf-8"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename || "circuit_import.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

btn.addEventListener("click", async () => {
  const payload = txt.value || "";
  const pass = pwd.value || "";

  if(!payload.trim()){
    alert("Cole o texto primeiro :)");
    return;
  }

  btn.disabled = true;
  hint.textContent = "Processando... não fecha a página.";
  setProgress(0);
  setStatus("Iniciando...", "warn");

  try{
    const resp = await fetch("/process_stream", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({password: pass, text: payload})
    });

    if(!resp.ok){
      const t = await resp.text();
      alert("Erro: " + t);
      btn.disabled = false;
      return;
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";

    while(true){
      const {value, done} = await reader.read();
      if(done) break;
      buffer += decoder.decode(value, {stream:true});

      let idx;
      while((idx = buffer.indexOf("\n")) >= 0){
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx+1);

        if(!line) continue;
        let msg;
        try{ msg = JSON.parse(line); }catch(e){ continue; }

        if(msg.type === "progress"){
          setProgress(msg.percent || 0);
          setStatus(`${(msg.percent||0).toFixed(1)}%  (${msg.current}/${msg.total})  ${msg.msg||""}`, "warn");
        }
        else if(msg.type === "done"){
          setProgress(100);
          setStatus(`Finalizado! OK: ${msg.ok} | Revisar: ${msg.review} | Tempo: ${msg.seconds.toFixed(1)}s`, "ok");
          downloadBase64(msg.csv_b64, msg.filename);
        }
        else if(msg.type === "error"){
          setStatus("Erro: " + (msg.message || "desconhecido"), "err");
          alert("Erro: " + (msg.message || "desconhecido"));
        }
      }
    }
  } catch(e){
    console.error(e);
    alert("Falhou: " + e);
  } finally{
    btn.disabled = false;
  }
});
</script>
</body>
</html>
"""

# =========================
# Helpers
# =========================
def dentro_de_manaus(lat, lon):
    west, south, east, north = MANAUS_VIEWBOX
    return (west <= lon <= east) and (south <= lat <= north)

def normaliza(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def similaridade(a, b) -> float:
    a = normaliza(a)
    b = normaliza(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def extrair_cep(texto):
    # aceita 69027000 ou 69027-000
    m = re.search(r"\b(\d{5})-?(\d{3})\b", texto)
    if not m:
        return None
    return m.group(1) + m.group(2)

def formata_cep(cep8):
    c = re.sub(r"\D", "", cep8 or "")
    if len(c) == 8:
        return f"{c[:5]}-{c[5:]}"
    return (cep8 or "").strip()

def extrair_numero(texto):
    # tenta pegar número de porta (bem “brasileiro”: 10, 10A, 437A etc)
    m = re.search(r"\b(\d{1,6}[A-Za-z]?)\b", texto)
    return m.group(1) if m else "S/N"

def limpar_texto_endereco(texto):
    t = (texto or "").replace("N/A", " ")
    # remove códigos gigantes (rastreamento etc)
    t = re.sub(r"\b\d{9,}\b", " ", t)
    # limpa espaços
    t = re.sub(r"\s+", " ", t).strip()
    return t

def linha_parece_endereco(texto):
    low = (texto or "").lower()
    via = re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|praca)\b", low)
    cep = extrair_cep(texto or "")
    # precisa de via e cep pra ser “endereço forte”
    return bool(via and cep)

def eh_linha_ruim(l):
    l = (l or "").strip()
    if not l:
        return True
    low = l.lower()
    if low.startswith("pacote de"):
        return True
    if re.fullmatch(r"[A-Z0-9_]{8,}", l.replace("-", "").replace(".", "").strip()):
        # tracking/código
        return True
    if "shpx" in low or "logistica" in low or "ltda" in low:
        # muito comum em linhas não-endereço
        return True
    return False

def tentar_pegar_nome(linha_anterior):
    # nome costuma ser linha anterior do endereço
    if not linha_anterior:
        return ""
    s = linha_anterior.strip()
    if eh_linha_ruim(s):
        return ""
    # se parece endereço, não é nome
    if linha_parece_endereco(s):
        return ""
    # tira emoticons
    s = s.replace(":^)", "").replace("^)", "").strip()
    # não deixa nome gigante
    if len(s) > 60:
        s = s[:60].rstrip()
    return s

def extrair_enderecos_do_texto(texto):
    """
    Estratégia tradicional (que funciona no mundo real):
    - quebra por linhas
    - tenta juntar linha quebrada: se tem "rua/av/trav" mas não tem CEP, cola com próximas 1-2 linhas
    - aceita endereço quando tem via + CEP
    - tenta capturar nome na linha anterior
    """
    linhas = [l.rstrip() for l in (texto or "").splitlines()]
    linhas = [l for l in linhas if l.strip()]

    enderecos = []
    i = 0
    while i < len(linhas):
        cur = limpar_texto_endereco(linhas[i])

        # pular lixo
        if eh_linha_ruim(cur):
            i += 1
            continue

        cep = extrair_cep(cur)
        tem_via = re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|praca)\b", cur.lower())

        # se tem via mas não tem cep, tenta juntar com próximas
        if tem_via and not cep:
            combo = cur
            nome = tentar_pegar_nome(linhas[i - 1] if i - 1 >= 0 else "")
            for j in range(1, 3):
                if i + j >= len(linhas):
                    break
                nx = limpar_texto_endereco(linhas[i + j])
                if eh_linha_ruim(nx):
                    continue
                combo2 = (combo + " " + nx).strip()
                if linha_parece_endereco(combo2):
                    enderecos.append((combo2, nome))
                    i += j + 1
                    break
            else:
                i += 1
            continue

        # endereço direto
        if linha_parece_endereco(cur):
            nome = tentar_pegar_nome(linhas[i - 1] if i - 1 >= 0 else "")
            enderecos.append((cur, nome))
            i += 1
            continue

        i += 1

    return enderecos

def buscar_viacep(cep8):
    cep_num = re.sub(r"\D", "", cep8 or "")
    if len(cep_num) != 8:
        return None
    url = f"https://viacep.com.br/ws/{cep_num}/json/"
    r = requests.get(url, timeout=TIMEOUT_VIACEP)
    if r.status_code != 200:
        return None
    j = r.json()
    if "erro" in j:
        return None
    return j

def nominatim_search_json(query, bounded=True, limit=1):
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

    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT_NOMINATIM)
    # respeita Nominatim
    time.sleep(SLEEP_NOMINATIM)

    if r.status_code != 200:
        return []
    try:
        return r.json() or []
    except Exception:
        return []

def tentar_1_resultado(query, bounded=True):
    data = nominatim_search_json(query, bounded=bounded, limit=1)
    if not data:
        return None, None
    try:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        return lat, lon
    except Exception:
        return None, None

def cacar_nome_antigo(raw_original, bairro, cidade, uf, cep_fmt):
    consulta = f"{raw_original}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}"
    candidatos = nominatim_search_json(consulta, bounded=True, limit=5)
    if not candidatos:
        candidatos = nominatim_search_json(consulta, bounded=False, limit=5)

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

    if melhor[0] is not None and melhor[2] >= MIN_SCORE_CACADO:
        return melhor[0], melhor[1], melhor[3], melhor[2]
    return None, None, "", 0.0

def csv_json(obj):
    return json.dumps(obj, ensure_ascii=False)

def montar_linha_destino(logradouro, numero, bairro, cidade, cep_fmt, nome):
    """
    Pra evitar o Circuit mostrar “sem nome”, a gente coloca algo humano no Destination Address.
    """
    logradouro = (logradouro or "").strip()
    bairro = (bairro or "").strip()
    cidade = (cidade or "").strip() or "Manaus"

    if logradouro:
        base = f"{logradouro}, {numero}".strip().strip(",")
        if bairro:
            base += f" - {bairro}"
        base += f", {cidade}-AM, {cep_fmt}"
        return base

    # fallback forte (CEP)
    # coloca nome (se tiver) e CEP pra não ficar um “nada”
    if nome:
        return f"{nome} - CEP {cep_fmt}, Manaus-AM"
    return f"CEP {cep_fmt}, Manaus-AM"

# =========================
# Rotas
# =========================
@app.get("/")
def home():
    return HTML

@app.post("/process_stream")
def process_stream():
    data = request.get_json(silent=True) or {}
    password = (data.get("password") or "").strip()
    texto = data.get("text") or ""

    if APP_PASSWORD and password != APP_PASSWORD:
        abort(401, "Senha inválida.")

    # Extrai endereços do texto bagunçado
    pares = extrair_enderecos_do_texto(texto)
    total = len(pares)
    if total == 0:
        abort(400, "Não achei nenhum endereço com via + CEP. (Tenta colar incluindo CEP na mesma linha.)")

    def stream():
        inicio = datetime.now()

        ok_rows = []
        revisar = 0
        cache_viacep = {}
        cache_geo = {}  # cache por request (não depende de disco no Render free)
        cacados = 0

        for idx, (raw, nome) in enumerate(pares, start=1):
            percent = (idx / total) * 100.0

            # status “batendo”
            yield (csv_json({
                "type": "progress",
                "percent": percent,
                "current": idx,
                "total": total,
                "msg": f"Analisando: {raw[:70]}"
            }) + "\n")

            raw_clean = limpar_texto_endereco(raw)
            cep = extrair_cep(raw_clean)
            if not cep:
                revisar += 1
                continue

            cep_fmt = formata_cep(cep)
            numero = extrair_numero(raw_clean)

            # ViaCEP (cache)
            yield (csv_json({
                "type": "progress",
                "percent": percent,
                "current": idx,
                "total": total,
                "msg": f"ViaCEP: {cep_fmt}"
            }) + "\n")

            dados = cache_viacep.get(cep)
            if dados is None:
                try:
                    dados = buscar_viacep(cep)
                except Exception:
                    dados = None
                cache_viacep[cep] = dados

            if not dados:
                revisar += 1
                # fallback mínimo
                ok_rows.append([
                    idx,
                    montar_linha_destino("", numero, "", "Manaus", cep_fmt, nome),
                    "", "Manaus", cep_fmt, "", "",
                    f"REVISAO_AUTOMATICA: CEP_INVALIDO | nome={nome or ''} | original: {raw_clean}"
                ])
                continue

            cidade = (dados.get("localidade") or "").strip() or "Manaus"
            uf = (dados.get("uf") or "").strip() or "AM"
            bairro = (dados.get("bairro") or "").strip()
            logradouro = (dados.get("logradouro") or "").strip()

            # trava Manaus (evita “ponto viajando”)
            if cidade.strip().lower() != "manaus":
                revisar += 1
                ok_rows.append([
                    idx,
                    montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                    bairro, "Manaus", cep_fmt, "", "",
                    f"REVISAO_AUTOMATICA: CEP_FORA_MANAUS({cidade}) | nome={nome or ''} | original: {raw_clean}"
                ])
                continue

            # Cache GEO (por request)
            key = f"{cep_fmt}|{numero}|{normaliza(logradouro) or normaliza(raw_clean)}"
            if key in cache_geo:
                lat, lon, note = cache_geo[key]
                ok_rows.append([idx,
                               montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                               bairro, "Manaus", cep_fmt, lat, lon,
                               note + (f" | nome={nome}" if nome else "")])
                continue

            # tentativas Nominatim
            lat = lon = None
            note = ""

            queries = []
            if logradouro:
                queries.append(f"{logradouro}, {numero}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")
            queries.append(f"{raw_clean}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")

            # bounded + livre (com status dentro do endereço)
            for qi, q in enumerate(queries, start=1):
                yield (csv_json({
                    "type": "progress",
                    "percent": percent,
                    "current": idx,
                    "total": total,
                    "msg": f"Nominatim bounded (tentativa {qi}/{len(queries)})…"
                }) + "\n")

                try:
                    lat, lon = tentar_1_resultado(q, bounded=True)
                except Exception:
                    lat = lon = None

                if lat is not None and dentro_de_manaus(lat, lon):
                    break

                yield (csv_json({
                    "type": "progress",
                    "percent": percent,
                    "current": idx,
                    "total": total,
                    "msg": f"Nominatim livre (tentativa {qi}/{len(queries)})…"
                }) + "\n")

                try:
                    lat2, lon2 = tentar_1_resultado(q, bounded=False)
                except Exception:
                    lat2 = lon2 = None

                if lat2 is not None and dentro_de_manaus(lat2, lon2):
                    lat, lon = lat2, lon2
                    break

            # caça nome antigo
            if lat is None or lon is None or (not dentro_de_manaus(lat, lon)):
                yield (csv_json({
                    "type": "progress",
                    "percent": percent,
                    "current": idx,
                    "total": total,
                    "msg": "Caçando nome antigo (top-5)…"
                }) + "\n")

                try:
                    lat3, lon3, display3, score3 = cacar_nome_antigo(raw_clean, bairro, "Manaus", uf, cep_fmt)
                except Exception:
                    lat3 = lon3 = None
                    display3 = ""
                    score3 = 0.0

                if lat3 is not None and dentro_de_manaus(lat3, lon3):
                    lat, lon = lat3, lon3
                    cacados += 1
                    note = f"CAÇADO(score={score3:.2f}): {display3[:80]}"

            # fallback CEP
            if lat is None or lon is None:
                revisar += 1
                ok_rows.append([
                    idx,
                    montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                    bairro, "Manaus", cep_fmt, "", "",
                    f"REVISAO_AUTOMATICA: NAO_ENCONTRADO | nome={nome or ''} | original: {raw_clean}"
                ])
                cache_geo[key] = ("", "", "FALLBACK_CEP")
                continue

            if not dentro_de_manaus(lat, lon):
                revisar += 1
                ok_rows.append([
                    idx,
                    montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                    bairro, "Manaus", cep_fmt, "", "",
                    f"REVISAO_AUTOMATICA: FORA_MANAUS({lat},{lon}) | nome={nome or ''} | original: {raw_clean}"
                ])
                continue

            # OK
            notes = note or "OK"
            if nome:
                notes += f" | nome={nome}"

            ok_rows.append([
                idx,
                montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                bairro,
                "Manaus",
                cep_fmt,
                lat,
                lon,
                notes
            ])

            cache_geo[key] = (lat, lon, notes)

        # gera CSV em memória
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
        w.writerows(ok_rows)
        csv_bytes = out.getvalue().encode("utf-8")

        duracao = (datetime.now() - inicio).total_seconds()
        filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        b64 = base64.b64encode(csv_bytes).decode("ascii")

        yield (csv_json({
            "type": "done",
            "ok": len(ok_rows),
            "review": revisar,
            "seconds": duracao,
            "filename": filename,
            "csv_b64": b64
        }) + "\n")

    resp = Response(stream(), mimetype="text/plain; charset=utf-8")
    # importantíssimo pra não “segurar” stream
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp

# =========================
# Run local (Render usa PORT)
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
