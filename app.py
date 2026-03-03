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
USER_AGENT = os.environ.get("USER_AGENT", "rota-privada-web/6.0A.3")

COUNTRY = "Brazil"

# Respeitoso, mas mais rápido
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.55"))

# Manaus (viewbox aproximado)
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)  # west, south, east, north

# Timeouts (conexão, leitura) - mais agressivo pra não "parecer travado"
TIMEOUT_VIACEP = (5, 10)
TIMEOUT_NOMINATIM = (5, 9)

MIN_SCORE_CACADO = float(os.environ.get("MIN_SCORE_CACADO", "0.42"))

# =========================
# HTML
# =========================
HTML = r"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Roteirizador Privado v6.0A.3</title>
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
    .tiny{font-size:11px;color:#666;margin-top:4px;font-family:ui-monospace,Consolas,monospace}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Roteirizador Privado <small style="font-size:12px;border:1px solid #ddd;border-radius:999px;padding:3px 8px;color:#333;">v6.0A.3</small></h1>

    <div class="card">
      <div class="muted">
        Cole o texto bagunçado (Shopee/Loggi/Mercado Livre etc). Eu tento achar os endereços e gero um CSV pro Circuit.
        <br><b>Dica:</b> se tiver CEP + rua na mesma linha, melhor. Se estiver quebrado, eu tento juntar.
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
        <div class="muted" id="hint">Quando começar, vai aparecer barra de progresso e status.</div>
        <div class="bar"><div class="fill" id="fill"></div></div>
        <div class="status" id="status"></div>
        <div class="tiny" id="alive"></div>
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
const aliveEl = document.getElementById("alive");

let lastMsgAt = 0;
let aliveTimer = null;

function setProgress(p){
  fill.style.width = Math.max(0, Math.min(100, p)) + "%";
}
function setStatus(msg, cls){
  statusEl.className = "status " + (cls || "");
  statusEl.textContent = msg || "";
}

function startAlive(){
  lastMsgAt = Date.now();
  if(aliveTimer) clearInterval(aliveTimer);
  aliveTimer = setInterval(() => {
    const s = Math.floor((Date.now() - lastMsgAt)/1000);
    if(s <= 1){
      aliveEl.textContent = "";
      return;
    }
    aliveEl.textContent = `Sem atualização há ${s}s (normal quando o geocoder está respondendo).`;
  }, 500);
}
function bumpAlive(){
  lastMsgAt = Date.now();
}

function b64UrlToUint8(b64url){
  let b64 = (b64url || "").replace(/-/g, "+").replace(/_/g, "/");
  while (b64.length % 4 !== 0) b64 += "=";
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i=0;i<binary.length;i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function downloadBytes(bytes, filename){
  const blob = new Blob([bytes], {type:"text/csv;charset=utf-8"});
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
  startAlive();

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
      if(aliveTimer) clearInterval(aliveTimer);
      return;
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";

    while(true){
      const {value, done} = await reader.read();
      if(done) break;

      bumpAlive();
      buffer += decoder.decode(value, {stream:true});

      let idx;
      while((idx = buffer.indexOf("\n")) >= 0){
        const rawLine = buffer.slice(0, idx); // não trim
        buffer = buffer.slice(idx+1);

        if(!rawLine.trim()) continue;

        let msg;
        try{ msg = JSON.parse(rawLine); }catch(e){ continue; }

        if(msg.type === "progress"){
          setProgress(msg.percent || 0);
          setStatus(`${(msg.percent||0).toFixed(1)}%  (${msg.current}/${msg.total})  ${msg.msg||""}`, "warn");
        }
        else if(msg.type === "done"){
          setProgress(100);
          setStatus(`Finalizado! OK: ${msg.ok} | Revisar: ${msg.review} | Tempo: ${msg.seconds.toFixed(1)}s`, "ok");
          if(aliveTimer) clearInterval(aliveTimer);
          aliveEl.textContent = "";

          const bytes = b64UrlToUint8(msg.csv_b64);
          downloadBytes(bytes, msg.filename);
        }
        else if(msg.type === "error"){
          setStatus("Erro: " + (msg.message || "desconhecido"), "err");
          if(aliveTimer) clearInterval(aliveTimer);
          aliveEl.textContent = "";
          alert("Erro: " + (msg.message || "desconhecido"));
        }
      }
    }
  } catch(e){
    console.error(e);
    if(aliveTimer) clearInterval(aliveTimer);
    aliveEl.textContent = "";
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
    m = re.search(r"\b(\d{5})-?(\d{3})\b", texto or "")
    if not m:
        return None
    return m.group(1) + m.group(2)

def formata_cep(cep8):
    c = re.sub(r"\D", "", cep8 or "")
    if len(c) == 8:
        return f"{c[:5]}-{c[5:]}"
    return (cep8 or "").strip()

def extrair_numero(texto):
    m = re.search(r"\b(\d{1,6}[A-Za-z]?)\b", texto or "")
    return m.group(1) if m else "S/N"

def limpar_texto_endereco(texto):
    t = (texto or "").replace("N/A", " ")
    t = re.sub(r"\b\d{9,}\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def linha_parece_endereco(texto):
    low = (texto or "").lower()
    via = re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|praca)\b", low)
    cep = extrair_cep(texto or "")
    return bool(via and cep)

def eh_linha_ruim(l):
    l = (l or "").strip()
    if not l:
        return True
    low = l.lower()
    if low.startswith("pacote de"):
        return True
    if re.fullmatch(r"[A-Z0-9_]{8,}", l.replace("-", "").replace(".", "").strip()):
        return True
    if "shpx" in low or "logistica" in low or "ltda" in low:
        return True
    return False

def tentar_pegar_nome(linha_anterior):
    if not linha_anterior:
        return ""
    s = linha_anterior.strip()
    if eh_linha_ruim(s):
        return ""
    if linha_parece_endereco(s):
        return ""
    s = s.replace(":^)", "").replace("^)", "").strip()
    if len(s) > 60:
        s = s[:60].rstrip()
    return s

def extrair_enderecos_do_texto(texto):
    linhas = [l.rstrip() for l in (texto or "").splitlines()]
    linhas = [l for l in linhas if l.strip()]

    enderecos = []
    i = 0
    while i < len(linhas):
        cur = limpar_texto_endereco(linhas[i])

        if eh_linha_ruim(cur):
            i += 1
            continue

        cep = extrair_cep(cur)
        tem_via = re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|praca)\b", cur.lower())

        if tem_via and not cep:
            combo = cur
            nome = tentar_pegar_nome(linhas[i - 1] if i - 1 >= 0 else "")
            achou = False
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
                    achou = True
                    break
            if not achou:
                i += 1
            continue

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

def tentar_com_retry(query, bounded=True, tries=2):
    # retry curto pra não "parar o mundo"
    last = (None, None)
    for t in range(1, tries + 1):
        try:
            lat, lon = tentar_1_resultado(query, bounded=bounded)
            if lat is not None:
                return lat, lon
            last = (lat, lon)
        except Exception:
            last = (None, None)
        time.sleep(0.25 * t)
    return last

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
    logradouro = (logradouro or "").strip()
    bairro = (bairro or "").strip()
    cidade = (cidade or "").strip() or "Manaus"

    if logradouro:
        base = f"{logradouro}, {numero}".strip().strip(",")
        if bairro:
            base += f" - {bairro}"
        base += f", {cidade}-AM, {cep_fmt}"
        return base

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

    pares = extrair_enderecos_do_texto(texto)
    total = len(pares)
    if total == 0:
        abort(400, "Não achei nenhum endereço com via + CEP. (Tenta colar incluindo CEP na mesma linha.)")

    def stream():
        inicio = datetime.now()

        ok_rows = []
        revisar = 0
        cache_viacep = {}
        cache_geo = {}
        cacados = 0

        for idx, (raw, nome) in enumerate(pares, start=1):
            percent = (idx / total) * 100.0

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

            if cidade.strip().lower() != "manaus":
                revisar += 1
                ok_rows.append([
                    idx,
                    montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                    bairro, "Manaus", cep_fmt, "", "",
                    f"REVISAO_AUTOMATICA: CEP_FORA_MANAUS({cidade}) | nome={nome or ''} | original: {raw_clean}"
                ])
                continue

            key = f"{cep_fmt}|{numero}|{normaliza(logradouro) or normaliza(raw_clean)}"
            if key in cache_geo:
                lat, lon, note = cache_geo[key]
                ok_rows.append([
                    idx,
                    montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                    bairro, "Manaus", cep_fmt, lat, lon,
                    note + (f" | nome={nome}" if nome else "")
                ])
                continue

            lat = lon = None
            note = ""

            # Estratégia: bounded primeiro (Manaus), depois livre só se necessário
            queries = []
            if logradouro:
                queries.append(f"{logradouro}, {numero}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")
            queries.append(f"{raw_clean}, {bairro}, Manaus-{uf}, {cep_fmt}, {COUNTRY}")

            # 1) bounded com retry curtinho
            for qi, q in enumerate(queries, start=1):
                yield (csv_json({
                    "type": "progress",
                    "percent": percent,
                    "current": idx,
                    "total": total,
                    "msg": f"Nominatim bounded (tentativa {qi}/{len(queries)})…"
                }) + "\n")

                lat, lon = tentar_com_retry(q, bounded=True, tries=2)
                if lat is not None and dentro_de_manaus(lat, lon):
                    break
                lat = lon = None

            # 2) livre só se ainda falhou (também com retry)
            if lat is None or lon is None:
                for qi, q in enumerate(queries, start=1):
                    yield (csv_json({
                        "type": "progress",
                        "percent": percent,
                        "current": idx,
                        "total": total,
                        "msg": f"Nominatim livre (tentativa {qi}/{len(queries)})…"
                    }) + "\n")

                    lat2, lon2 = tentar_com_retry(q, bounded=False, tries=2)
                    if lat2 is not None and dentro_de_manaus(lat2, lon2):
                        lat, lon = lat2, lon2
                        break

            # 3) caça nome antigo (top-5)
            if lat is None or lon is None:
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

            # 4) fallback (mas com endereço útil, não só CEP seco)
            if lat is None or lon is None:
                revisar += 1
                ok_rows.append([
                    idx,
                    montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                    bairro, "Manaus", cep_fmt, "", "",
                    f"REVISAO_AUTOMATICA: NAO_ENCONTRADO | nome={nome or ''} | original: {raw_clean}"
                ])
                cache_geo[key] = ("", "", "FALLBACK")
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

            notes = note or "OK"
            if nome:
                notes += f" | nome={nome}"

            ok_rows.append([
                idx,
                montar_linha_destino(logradouro, numero, bairro, "Manaus", cep_fmt, nome),
                bairro, "Manaus", cep_fmt,
                lat, lon,
                notes
            ])

            cache_geo[key] = (lat, lon, notes)

        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
        w.writerows(ok_rows)
        csv_bytes = out.getvalue().encode("utf-8")

        duracao = (datetime.now() - inicio).total_seconds()
        filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Base64 URL-safe e sem padding
        b64url = base64.urlsafe_b64encode(csv_bytes).decode("ascii").rstrip("=")

        yield (csv_json({
            "type": "done",
            "ok": len(ok_rows),
            "review": revisar,
            "seconds": duracao,
            "filename": filename,
            "csv_b64": b64url,
            "cacados": cacados
        }) + "\n")

    resp = Response(stream(), mimetype="text/plain; charset=utf-8")
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
