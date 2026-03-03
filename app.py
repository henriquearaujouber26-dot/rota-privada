import os
import io
import re
import csv
import time
import json
import base64
import threading
import requests
from datetime import datetime
import difflib
from flask import Flask, request, Response, abort

# =========================
# CONFIG (v6.0A.4)
# =========================
APP_VERSION = "v6.0A.4"
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")

USER_AGENT = os.environ.get("USER_AGENT", f"rota-privada-web/{APP_VERSION}")
COUNTRY = "Brazil"

# Hard cap por endereço (o que você pediu)
MAX_SECONDS_PER_ADDRESS = float(os.environ.get("MAX_SECONDS_PER_ADDRESS", "15"))

# Pausa educada pro Nominatim (tende a reduzir ban / rate limit)
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.65"))

# Viewbox aproximada Manaus (pra evitar pular pra outro bairro/estado)
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)  # (west, south, east, north)

# Cache só em memória (Render free NÃO tem disk persistente)
CACHE_MAX = int(os.environ.get("CACHE_MAX", "5000"))  # limite simples


app = Flask(__name__)

# cache em memória: key -> (lat, lon)
_cache = {}
_cache_lock = threading.Lock()


# =========================
# HELPERS
# =========================
def dentro_de_manaus(lat, lon):
    west, south, east, north = MANAUS_VIEWBOX
    return (west <= lon <= east) and (south <= lat <= north)

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def normaliza(s):
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def similaridade(a, b):
    a = normaliza(a)
    b = normaliza(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def formata_cep(cep):
    c = re.sub(r"\D", "", cep or "")
    return f"{c[:5]}-{c[5:]}" if len(c) == 8 else (cep or "")

def extrair_cep(texto):
    m = re.search(r"\b\d{8}\b", texto or "")
    return m.group() if m else None

def extrair_numero(texto):
    # pega o primeiro número "provável" (evita telefone)
    # ex: "Rua X, 115 ..." -> 115
    # se tiver "Apto 203" não atrapalha: ainda pega 115
    t = texto or ""
    # remove telefones grandes
    t = re.sub(r"\b\d{9,}\b", " ", t)
    m = re.search(r"\b(\d{1,5}[A-Za-z]?)\b", t)
    return m.group(1) if m else "S/N"

def limpar_texto_endereco(texto):
    t = (texto or "").replace("N/A", " ")
    # remove telefones (9+ dígitos)
    t = re.sub(r"\b\d{9,}\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def linha_parece_endereco(texto):
    if not texto:
        return False
    via = re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|lote|conjunto)\b", texto.lower())
    cep = re.search(r"\b\d{8}\b", texto)
    return bool(via and cep)

def juntar_linhas_quebradas(linhas):
    """
    Junta linha quebrada (nome numa linha, endereço na outra, etc).
    Estratégia simples e eficiente:
      - se a linha atual não tem CEP, segura como buffer
      - quando aparecer uma linha com CEP, cola buffer + linha
    """
    out = []
    buf = ""
    for ln in linhas:
        ln = limpar_texto_endereco(ln)
        if not ln:
            continue
        if extrair_cep(ln):
            if buf:
                out.append(limpar_texto_endereco(buf + " " + ln))
                buf = ""
            else:
                out.append(ln)
        else:
            # acumula possíveis pedaços
            if len(ln) <= 120:  # evita colar textos enormes tipo "Pacote de..."
                buf = (buf + " " + ln).strip() if buf else ln
            else:
                # joga fora lixo muito grande
                buf = buf
    return out

def extrair_enderecos_do_texto(texto_bruto):
    linhas = (texto_bruto or "").splitlines()
    linhas = [l.rstrip() for l in linhas if l.strip()]
    # 1) tenta pegar direto linhas que parecem endereço
    diretas = [limpar_texto_endereco(l) for l in linhas if linha_parece_endereco(l)]
    # 2) junta quebradas e tenta novamente
    juntas = juntar_linhas_quebradas(linhas)
    juntas = [l for l in juntas if linha_parece_endereco(l)]
    # mistura mantendo ordem (sem duplicar)
    seen = set()
    out = []
    for l in diretas + juntas:
        k = normaliza(l)
        if k and k not in seen:
            seen.add(k)
            out.append(l)
    return out

def cache_key(cep_fmt, numero, logradouro_oficial, raw):
    base = normaliza(logradouro_oficial)
    if not base:
        base = normaliza(re.sub(r"\b\d{8}\b", "", raw or ""))
    return f"{cep_fmt}|{str(numero).upper()}|{base}"

def cache_get(k):
    with _cache_lock:
        return _cache.get(k)

def cache_set(k, lat, lon):
    with _cache_lock:
        # limite simples (FIFO tosco): se estourar, limpa geral
        if len(_cache) >= CACHE_MAX:
            _cache.clear()
        _cache[k] = (lat, lon)

def req_get(url, *, params=None, headers=None, timeout=10):
    # timeout pode ser float ou (connect, read)
    return requests.get(url, params=params, headers=headers, timeout=timeout)

def buscar_viacep(cep, timeout_s):
    cep_num = re.sub(r"\D", "", cep or "")
    if len(cep_num) != 8:
        return None
    url = f"https://viacep.com.br/ws/{cep_num}/json/"
    try:
        r = req_get(url, timeout=min(max(2.0, timeout_s), 12.0))
        if r.status_code == 200:
            j = r.json()
            if "erro" not in j:
                return j
    except Exception:
        return None
    return None

def nominatim_search_json(query, *, bounded=True, limit=1, timeout_s=10):
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
    try:
        r = req_get(url, params=params, headers=headers, timeout=min(max(2.5, timeout_s), 25.0))
        # dorme só se ainda faz sentido (pra não estourar o hardcap)
        if SLEEP_NOMINATIM > 0:
            time.sleep(SLEEP_NOMINATIM)
        if r.status_code == 200:
            try:
                return r.json() or []
            except Exception:
                return []
    except Exception:
        return []
    return []

def tentar_1_resultado(query, *, bounded=True, timeout_s=8):
    data = nominatim_search_json(query, bounded=bounded, limit=1, timeout_s=timeout_s)
    if not data:
        return None, None, ""
    try:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        disp = data[0].get("display_name", "") or ""
        return lat, lon, disp
    except Exception:
        return None, None, ""

def caca_nome_antigo(raw_original, bairro, cidade, uf, cep_fmt, timeout_s):
    """
    Pega top-5 e escolhe melhor por similaridade.
    Só roda se ainda tiver tempo sobrando dentro do hardcap.
    """
    consulta = f"{raw_original}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}"
    candidatos = nominatim_search_json(consulta, bounded=True, limit=5, timeout_s=timeout_s)
    if not candidatos:
        candidatos = nominatim_search_json(consulta, bounded=False, limit=5, timeout_s=timeout_s)

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

def montar_fallback_viacep(dados_viacep, numero, cep_fmt):
    """
    Fallback bonito: não fica só "CEP seco".
    Se ViaCEP tiver logradouro/bairro, monta uma linha melhor.
    """
    logradouro = (dados_viacep.get("logradouro") or "").strip()
    bairro = (dados_viacep.get("bairro") or "").strip()

    partes = []
    if logradouro:
        if numero and numero != "S/N":
            partes.append(f"{logradouro}, {numero}")
        else:
            partes.append(logradouro)
    if bairro:
        partes.append(bairro)

    # sempre inclui o CEP
    partes.append(f"{cep_fmt}")
    partes.append("Manaus-AM")

    # Ex: "Rua X, 115, Compensa, 69035-000, Manaus-AM"
    return ", ".join([p for p in partes if p])

def sse_event(obj):
    # SSE: data: {json}\n\n
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

def processar_1_endereco(seq, raw, send_progress):
    """
    Retorna:
      (ok_row, revisar_row, stats)
    ok_row no formato do Circuit:
      [Sequence, Destination Address, Bairro, City, Zipcode/Postal Code, Latitude, Longitude, Notes]
    revisar_row:
      [endereco_original, motivo]
    """
    t0 = time.monotonic()
    deadline = t0 + MAX_SECONDS_PER_ADDRESS

    def remaining():
        return max(0.0, deadline - time.monotonic())

    cep = extrair_cep(raw)
    numero = extrair_numero(raw)

    if not cep:
        return None, [raw, "SEM_CEP"], {"fallback": False, "cacado": False, "cache": False}

    # 1) ViaCEP (rápido)
    send_progress(stage="ViaCEP")
    dados = buscar_viacep(cep, timeout_s=min(remaining(), 6.0))
    if not dados:
        # sem viacep, fallback CEP seco
        cep_fmt = formata_cep(cep)
        ok = [
            seq,
            f"{cep_fmt}, Manaus-AM",
            "",
            "Manaus",
            cep_fmt,
            "",
            "",
            f"FALLBACK: CEP_INVALIDO/VIACEP_FAIL | original: {raw}",
        ]
        return ok, [raw, f"CEP_INVALIDO ({cep})"], {"fallback": True, "cacado": False, "cache": False}

    if str(dados.get("localidade", "")).strip().lower() != "manaus":
        cep_fmt = formata_cep(cep)
        ok = [
            seq,
            f"{cep_fmt}, Manaus-AM",
            (dados.get("bairro") or "").strip(),
            "Manaus",
            cep_fmt,
            "",
            "",
            f"FALLBACK: CEP_FORA_MANAUS({dados.get('localidade','')}) | original: {raw}",
        ]
        return ok, [raw, f"CEP_FORA_MANAUS ({dados.get('localidade','')})"], {"fallback": True, "cacado": False, "cache": False}

    logradouro = (dados.get("logradouro") or "").strip()
    bairro = (dados.get("bairro") or "").strip()
    cidade = "Manaus"
    uf = "AM"
    cep_fmt = formata_cep(cep)

    # 2) Cache
    k = cache_key(cep_fmt, numero, logradouro, raw)
    cached = cache_get(k)
    if cached:
        lat, lon = cached
        if dentro_de_manaus(lat, lon):
            ok = [
                seq,
                (f"{logradouro}, {numero}".strip(", ") if logradouro else montar_fallback_viacep(dados, numero, cep_fmt)),
                bairro,
                cidade,
                cep_fmt,
                lat,
                lon,
                "CACHE",
            ]
            return ok, None, {"fallback": False, "cacado": False, "cache": True}

    # Se já passou o tempo (pode acontecer em pico), corta
    if remaining() <= 0.2:
        fallback_addr = montar_fallback_viacep(dados, numero, cep_fmt)
        ok = [
            seq, fallback_addr, bairro, cidade, cep_fmt, "", "", f"FALLBACK: TIMEOUT({MAX_SECONDS_PER_ADDRESS}s) | original: {raw}"
        ]
        return ok, [raw, "TIMEOUT_HARDCAP"], {"fallback": True, "cacado": False, "cache": False}

    # 3) Tentativas Nominatim: bounded -> unbounded
    lat = lon = None
    note = ""
    cacado = False

    queries = []
    if logradouro:
        queries.append(f"{logradouro}, {numero}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}")
    queries.append(f"{raw}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}")

    for idx, q in enumerate(queries, start=1):
        if remaining() <= 0.2:
            break

        # bounded
        send_progress(stage=f"Nominatim bounded (tentativa {idx}/2)")
        lat1, lon1, _disp1 = tentar_1_resultado(q, bounded=True, timeout_s=min(remaining(), 7.5))
        if lat1 is not None and dentro_de_manaus(lat1, lon1):
            lat, lon = lat1, lon1
            break

        if remaining() <= 0.2:
            break

        # unbounded
        send_progress(stage=f"Nominatim livre (tentativa {idx}/2)")
        lat2, lon2, _disp2 = tentar_1_resultado(q, bounded=False, timeout_s=min(remaining(), 7.5))
        if lat2 is not None and dentro_de_manaus(lat2, lon2):
            lat, lon = lat2, lon2
            break

    # 4) Caça nome antigo (top-5) se ainda tiver tempo decente
    if (lat is None or lon is None or not dentro_de_manaus(lat, lon)) and remaining() >= 3.0:
        send_progress(stage="Caçando nome antigo (top-5)…")
        lat3, lon3, disp3, score3 = caca_nome_antigo(raw, bairro, cidade, uf, cep_fmt, timeout_s=min(remaining(), 10.0))
        if lat3 is not None and dentro_de_manaus(lat3, lon3):
            lat, lon = lat3, lon3
            cacado = True
            note = f"CAÇADO(score={score3:.2f}): {disp3[:80]}"

    # 5) Se falhou ou fora da caixa, fallback ViaCEP (bonito)
    if lat is None or lon is None or not dentro_de_manaus(lat, lon):
        motivo = "NAO_ENCONTRADO" if (lat is None or lon is None) else f"FORA_MANAUS({lat},{lon})"
        fallback_addr = montar_fallback_viacep(dados, numero, cep_fmt)
        ok = [
            seq,
            fallback_addr,
            bairro,
            cidade,
            cep_fmt,
            "",
            "",
            f"FALLBACK: {motivo} | original: {raw}",
        ]
        return ok, [raw, motivo], {"fallback": True, "cacado": False, "cache": False}

    # 6) OK normal
    destino = (f"{logradouro}, {numero}".strip(", ") if logradouro else montar_fallback_viacep(dados, numero, cep_fmt))
    ok = [
        seq,
        destino,
        bairro,
        cidade,
        cep_fmt,
        lat,
        lon,
        note
    ]

    cache_set(k, lat, lon)
    return ok, None, {"fallback": False, "cacado": cacado, "cache": False}


# =========================
# HTML (interface)
# =========================
HTML = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Roteirizador Privado {APP_VERSION}</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#fff; margin:0; padding:0; }}
    .wrap {{ max-width: 980px; margin: 40px auto; padding: 0 16px; }}
    h1 {{ text-align:center; margin:0 0 10px; }}
    .ver {{ font-size: 12px; padding: 2px 8px; border:1px solid #ddd; border-radius: 999px; vertical-align: middle; }}
    .card {{ border:1px solid #e6e6e6; border-radius: 12px; padding: 18px; }}
    textarea {{ width:100%; min-height: 300px; padding: 12px; border-radius: 10px; border:1px solid #ddd; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
    input[type="password"] {{ padding:10px; border:1px solid #ddd; border-radius:10px; width: 240px; }}
    button {{ padding:10px 16px; border:0; border-radius: 10px; cursor:pointer; background:#111; color:#fff; font-weight:600; }}
    button:disabled {{ opacity:.5; cursor:not-allowed; }}
    .row {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .hint {{ color:#444; font-size: 13px; margin: 0 0 10px; }}
    .small {{ color:#666; font-size: 12px; margin-top:10px; }}
    .progress-wrap {{ margin-top: 12px; }}
    .bar {{ width:100%; height: 12px; border-radius: 999px; background:#eee; overflow:hidden; }}
    .fill {{ height: 12px; width:0%; background:#111; transition: width .15s linear; }}
    .status {{ margin-top: 8px; font-size: 13px; color:#111; }}
    .bad {{ color:#b00020; font-weight: 600; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Roteirizador Privado <span class="ver">{APP_VERSION}</span></h1>
    <div class="card">
      <p class="hint">
        Cole o texto bagunçado (Shopee/Loggi/Mercado Livre etc). Eu extraio endereços e gero um CSV pronto pro Circuit.<br/>
        <b>Dica raiz:</b> se a linha tiver <b>rua + número + CEP</b>, é sucesso. Se cair em fallback, vai em <b>Notes</b> com motivo.
        <br/><b>Hard cap:</b> no máximo <b>{MAX_SECONDS_PER_ADDRESS:.0f}s</b> por endereço.
      </p>

      <div class="row">
        <label><b>Senha:</b></label>
        <input id="senha" type="password" placeholder="APP_PASSWORD"/>
        <button id="btn">Gerar CSV</button>
      </div>

      <div style="margin-top:12px;">
        <label><b>Cole aqui:</b></label>
        <textarea id="txt" placeholder="Cole aqui sua lista bagunçada..."></textarea>
      </div>

      <div class="progress-wrap" id="pw" style="display:none;">
        <div class="bar"><div class="fill" id="fill"></div></div>
        <div class="status" id="status">Processando...</div>
      </div>

      <div class="small">
        Saída: <b>circuit_import_*.csv</b> (download automático).
      </div>
    </div>
  </div>

<script>
const btn = document.getElementById("btn");
const txt = document.getElementById("txt");
const senha = document.getElementById("senha");
const pw = document.getElementById("pw");
const fill = document.getElementById("fill");
const statusEl = document.getElementById("status");

function setProgress(pct, msg) {{
  pw.style.display = "block";
  fill.style.width = Math.max(0, Math.min(100, pct)).toFixed(1) + "%";
  statusEl.textContent = msg || "";
}}

function downloadBase64Csv(b64, filename) {{
  // Base64 -> bytes
  const binary = atob(b64);
  const len = binary.length;
  const bytes = new Uint8Array(len);
  for (let i=0; i<len; i++) bytes[i] = binary.charCodeAt(i);

  const blob = new Blob([bytes], {{type: "text/csv;charset=utf-8"}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename || "circuit_import.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}}

btn.addEventListener("click", async () => {{
  const payload = {{
    senha: senha.value || "",
    texto: txt.value || ""
  }};

  if (!payload.texto.trim()) {{
    alert("Cole algum texto primeiro 🙂");
    return;
  }}

  btn.disabled = true;
  setProgress(0, "Iniciando... não fecha a página.");

  try {{
    const res = await fetch("/process", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json" }},
      body: JSON.stringify(payload)
    }});

    if (!res.ok) {{
      const t = await res.text();
      throw new Error(t || ("HTTP " + res.status));
    }}

    const reader = res.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let buffer = "";

    while (true) {{
      const {{ value, done }} = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {{ stream: true }});

      // SSE events separated by double newline
      let idx;
      while ((idx = buffer.indexOf("\\n\\n")) >= 0) {{
        const chunk = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);

        // read "data: ..."
        const lines = chunk.split("\\n");
        for (const line of lines) {{
          if (!line.startsWith("data: ")) continue;
          const jsonStr = line.slice(6);
          if (!jsonStr) continue;
          const ev = JSON.parse(jsonStr);

          if (ev.type === "progress") {{
            setProgress(ev.percent || 0, ev.message || "Processando...");
          }} else if (ev.type === "done") {{
            setProgress(100, "Finalizado. Baixando CSV...");
            downloadBase64Csv(ev.csv_base64, ev.filename);
          }} else if (ev.type === "error") {{
            statusEl.innerHTML = '<span class="bad">Deu ruim: ' + (ev.message || "erro") + '</span>';
          }} else if (ev.type === "ping") {{
            // só keepalive
          }}
        }}
      }}
    }}
  }} catch (e) {{
    console.error(e);
    alert("Erro: " + (e.message || e));
  }} finally {{
    btn.disabled = false;
  }}
}});
</script>
</body>
</html>
"""


# =========================
# ROUTES
# =========================
@app.get("/")
def home():
    return HTML

@app.post("/process")
def process():
    data = request.get_json(silent=True) or {}
    senha = (data.get("senha") or "").strip()
    texto = data.get("texto") or ""

    if senha != APP_PASSWORD:
        abort(401, "Senha inválida.")

    enderecos = extrair_enderecos_do_texto(texto)
    total = len(enderecos)
    if total == 0:
        abort(400, "Não achei endereços. Dica: precisa ter RUA/AV/TRAVESSA/Beco + CEP (8 dígitos).")

    def stream():
        ok_rows = []
        revisar_rows = []
        inicio = time.monotonic()

        cache_hits = 0
        cacos = 0
        fallbacks = 0

        # header SSE
        yield "retry: 1500\n\n"

        # ping inicial
        yield sse_event({"type":"ping","ts":now_iso()})

        last_ping = time.monotonic()

        for i, raw in enumerate(enderecos, start=1):
            percent = (i / total) * 100.0
            base_msg = f"{percent:.1f}% ({i}/{total})"

            def send_progress(stage=""):
                msg = f"{base_msg} {stage}".strip()
                yield sse_event({"type":"progress","percent":percent,"message":msg})

            # manda uma atualização logo no começo do endereço
            yield sse_event({"type":"progress","percent":percent,"message":f"{base_msg} Iniciando… {raw[:70]}"})

            # keepalive ping (evita “travou” por idle)
            if time.monotonic() - last_ping >= 2.5:
                yield sse_event({"type":"ping","ts":now_iso()})
                last_ping = time.monotonic()

            # processa endereço (com hardcap interno)
            # pra poder mandar progresso por stage, passamos um "sender"
            # que vai yieldar eventos durante o processamento
            stage_msgs = []

            def stage_sender(stage):
                stage_msgs.append(stage)

            try:
                ok, rev, stats = processar_1_endereco(i, raw, send_progress=lambda stage="": stage_sender(stage))
                # despeja os stages coletados (mantém UI viva)
                for st in stage_msgs:
                    yield sse_event({"type":"progress","percent":percent,"message":f"{base_msg} {st}"})

                if stats.get("cache"):
                    cache_hits += 1
                if stats.get("cacado"):
                    cacos += 1
                if stats.get("fallback"):
                    fallbacks += 1

                if ok:
                    ok_rows.append(ok)
                if rev:
                    revisar_rows.append(rev)

            except Exception as e:
                # erro inesperado: não mata tudo, só joga em revisão + fallback
                try:
                    cep = extrair_cep(raw) or ""
                    cep_fmt = formata_cep(cep) if cep else ""
                    ok_rows.append([
                        i,
                        (f"{cep_fmt}, Manaus-AM" if cep_fmt else "Manaus-AM"),
                        "",
                        "Manaus",
                        cep_fmt,
                        "",
                        "",
                        f"FALLBACK: EXCEPTION({str(e)[:80]}) | original: {raw}",
                    ])
                except Exception:
                    pass
                revisar_rows.append([raw, f"EXCEPTION: {str(e)[:120]}"])

            # ping extra
            if time.monotonic() - last_ping >= 2.5:
                yield sse_event({"type":"ping","ts":now_iso()})
                last_ping = time.monotonic()

        # monta CSV em memória
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
        w.writerows(ok_rows)
        csv_bytes = out.getvalue().encode("utf-8")

        # base64 limpo (sem newline)
        b64 = base64.b64encode(csv_bytes).decode("ascii")

        dur = time.monotonic() - inicio
        filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # evento final
        yield sse_event({
            "type": "done",
            "filename": filename,
            "csv_base64": b64,
            "stats": {
                "total": total,
                "ok": len(ok_rows),
                "revisar": len(revisar_rows),
                "cache_hits": cache_hits,
                "cacados": cacos,
                "fallbacks": fallbacks,
                "tempo_s": round(dur, 1),
                "hardcap_s": MAX_SECONDS_PER_ADDRESS
            }
        })

    return Response(stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    # Render usa PORT no ambiente
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
