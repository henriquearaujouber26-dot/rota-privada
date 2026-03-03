import os
import io
import re
import csv
import time
import base64
import requests
from datetime import datetime
import difflib
from flask import Flask, request, Response

# =============================
# SSL FIX (Windows / requests)
# =============================
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
except Exception:
    pass

app = Flask(__name__)

# =============================
# CONFIG
# =============================
APP_PASSWORD = os.environ.get("APP_PASSWORD", "1234")
USER_AGENT = os.environ.get("USER_AGENT", "rota-privada-web/6.0A")
COUNTRY = "Brazil"

# Nominatim etiquette: 1 req/s-ish. Dá pra baixar um pouco, mas não abusa.
SLEEP_NOMINATIM = float(os.environ.get("SLEEP_NOMINATIM", "0.85"))

# “Caixa” aproximada de Manaus (pra evitar mandar ponto pra outro bairro/estado/planeta)
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)

# Cache em memória (Render Free pode reiniciar — ok, cache é bônus)
# chave -> (lat, lon, updated_at_iso)
GEO_CACHE = {}

# Cache ViaCEP em memória (evita repetir consulta de CEP)
VIA_CACHE = {}


# =============================
# HTML (página simples)
# =============================
HTML = r"""
<!doctype html>
<html lang="pt-br">
<head>
  <meta charset="utf-8" />
  <title>Roteirizador Privado v6.0A</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: Arial, sans-serif; background:#fff; color:#111; }
    .wrap { max-width: 860px; margin: 40px auto; padding: 0 16px; }
    h1 { text-align:center; font-size: 28px; margin-bottom: 6px; }
    .sub { text-align:center; color:#555; margin-top:0; margin-bottom: 24px; }
    .card { border:1px solid #ddd; border-radius: 12px; padding: 18px; }
    label { display:block; font-weight: 600; margin-top: 10px; }
    input[type=password] { width: 260px; padding: 10px; border:1px solid #ccc; border-radius: 10px; }
    textarea { width: 100%; height: 320px; padding: 12px; border:1px solid #ccc; border-radius: 10px; font-family: Consolas, monospace; font-size: 13px; }
    button { padding: 10px 14px; border:0; border-radius: 10px; background:#111; color:#fff; cursor:pointer; font-weight: 700; }
    button[disabled] { opacity: .5; cursor:not-allowed; }
    .row { display:flex; gap: 12px; align-items:center; flex-wrap: wrap; }
    .hint { color:#666; margin: 10px 0 6px; font-size: 13px; }
    .status { margin-top: 10px; font-size: 14px; color:#333; }
    .barbox { margin-top: 10px; height: 14px; background:#eee; border-radius: 999px; overflow:hidden; display:none; }
    .bar { height: 14px; width: 0%; background:#111; }
    .pct { margin-top: 6px; font-size: 13px; color:#555; display:none; }
    .footer { margin-top: 10px; font-size: 12px; color:#777; }
    .ok { color: #0a7a2a; font-weight: 700; }
    .err { color: #b00020; font-weight: 700; }
  </style>
</head>
<body>
<div class="wrap">
  <h1>Roteirizador Privado <span style="font-size:12px;color:#777;border:1px solid #ddd;border-radius:10px;padding:3px 8px;vertical-align:middle;">v6.0A</span></h1>
  <p class="sub">Cole o texto bagunçado (Shopee/Loggi/etc). Eu extraio endereços e gero um CSV pronto pro Circuit.</p>

  <div class="card">
    <div class="hint">
      Dica raiz: se a linha tiver <b>rua + número + CEP</b>, é sucesso. Se estiver quebrado, eu tento juntar.
      Se algum cair em “fallback”, vai com <b>logradouro/bairro do ViaCEP</b> (não fica só “CEP seco”).
    </div>

    <div class="row">
      <div>
        <label>Senha</label>
        <input id="pw" type="password" placeholder="APP_PASSWORD" />
      </div>
      <div style="margin-top:28px;">
        <button id="btn">Gerar CSV</button>
      </div>
    </div>

    <label style="margin-top:14px;">Cole aqui</label>
    <textarea id="txt" placeholder="Cole aqui sua lista bagunçada..."></textarea>

    <div class="status" id="status"></div>
    <div class="barbox" id="barbox"><div class="bar" id="bar"></div></div>
    <div class="pct" id="pct"></div>

    <div class="footer">
      Saída: <b>circuit_import_*.csv</b> (download automático). Se algo ficar duvidoso, vai em <b>Notes</b> com o motivo.
    </div>
  </div>
</div>

<script>
const btn = document.getElementById('btn');
const statusEl = document.getElementById('status');
const barbox = document.getElementById('barbox');
const bar = document.getElementById('bar');
const pct = document.getElementById('pct');

function setProgress(p, msg){
  barbox.style.display = 'block';
  pct.style.display = 'block';
  bar.style.width = `${p}%`;
  pct.textContent = `${p.toFixed(1)}% — ${msg || ""}`;
}

function setStatus(html){
  statusEl.innerHTML = html || "";
}

function downloadBase64Csv(b64, filename){
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i=0;i<bin.length;i++) bytes[i] = bin.charCodeAt(i);
  const blob = new Blob([bytes], {type: "text/csv;charset=utf-8"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename || "circuit_import.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(()=>URL.revokeObjectURL(url), 2000);
}

btn.addEventListener('click', async () => {
  const text = document.getElementById('txt').value || "";
  const password = document.getElementById('pw').value || "";

  if (!password) { setStatus('<span class="err">Bota a senha aí.</span>'); return; }
  if (text.trim().length < 10) { setStatus('<span class="err">Cole alguma coisa no texto.</span>'); return; }

  btn.disabled = true;
  setStatus('Iniciando… não fecha a página.');
  setProgress(0.1, "preparando");

  try {
    const resp = await fetch('/process_stream', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ password, text })
    });

    if (!resp.ok) {
      const t = await resp.text();
      throw new Error(t || `HTTP ${resp.status}`);
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {stream:true});

      // processa linhas JSONL
      let idx;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line) continue;

        let evt;
        try { evt = JSON.parse(line); } catch(e){ continue; }

        if (evt.type === "progress") {
          setProgress(evt.percent || 0, evt.msg || "");
          setStatus(`Processando: <b>${evt.current}</b>/<b>${evt.total}</b>`);
        }
        if (evt.type === "warn") {
          setStatus(`<span class="err">${evt.msg}</span>`);
        }
        if (evt.type === "done") {
          setProgress(100, "finalizado");
          const resumo = `
            <span class="ok">Finalizado.</span>
            OK: <b>${evt.ok}</b> — Revisar: <b>${evt.revisar}</b> — Tempo: <b>${evt.seconds.toFixed(1)}s</b>
          `;
          setStatus(resumo);
          downloadBase64Csv(evt.csv_base64, evt.filename);
        }
      }
    }
  } catch (e) {
    setStatus(`<span class="err">Deu ruim:</span> ${String(e.message || e)}`);
  } finally {
    btn.disabled = false;
  }
});
</script>
</body>
</html>
"""


# =============================
# Helpers
# =============================
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
    if len(c) == 8:
        return f"{c[:5]}-{c[5:]}"
    return cep or ""

def extrair_cep(texto: str):
    if not texto:
        return None
    m = re.search(r"\b(\d{5})-?(\d{3})\b", texto)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    return None

def linha_tem_via(texto: str) -> bool:
    if not texto:
        return False
    return bool(re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|loteamento|conjunto)\b", texto.lower()))

def extrair_numero(texto: str) -> str:
    if not texto:
        return "S/N"
    # tenta achar número “bonito” primeiro
    m = re.search(r"\b(n[ºo]\s*)?(\d{1,5}[A-Za-z]?)\b", texto)
    if m:
        return m.group(2)
    return "S/N"

def limpar_texto_endereco(texto: str) -> str:
    t = (texto or "").replace("N/A", " ")
    # remove “rastros” gigantes (telefone/códigos)
    t = re.sub(r"\b\d{9,}\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def dentro_de_manaus(lat: float, lon: float) -> bool:
    west, south, east, north = MANAUS_VIEWBOX
    return (west <= lon <= east) and (south <= lat <= north)

def buscar_viacep(cep8: str):
    cep_num = re.sub(r"\D", "", cep8 or "")
    if len(cep_num) != 8:
        return None

    if cep_num in VIA_CACHE:
        return VIA_CACHE[cep_num]

    url = f"https://viacep.com.br/ws/{cep_num}/json/"
    try:
        r = requests.get(url, timeout=18)
        if r.status_code == 200:
            j = r.json()
            if "erro" not in j:
                VIA_CACHE[cep_num] = j
                return j
    except Exception:
        return None
    return None

def nominatim_search_json(query: str, bounded=True, limit=1):
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
        r = requests.get(url, params=params, headers=headers, timeout=22)
    except Exception:
        return []
    finally:
        # respeita o Nominatim
        time.sleep(SLEEP_NOMINATIM)

    if r.status_code == 200:
        try:
            return r.json() or []
        except Exception:
            return []
    return []

def tentar_1_resultado(query: str, bounded=True):
    data = nominatim_search_json(query, bounded=bounded, limit=1)
    if not data:
        return None, None
    try:
        lat = float(data[0].get("lat"))
        lon = float(data[0].get("lon"))
        return lat, lon
    except Exception:
        return None, None

def cache_key(cep_fmt: str, numero: str, logradouro_oficial: str, raw: str) -> str:
    base = normaliza(logradouro_oficial)
    if not base:
        base = normaliza(raw)
    return f"{cep_fmt}|{str(numero).upper()}|{base}"

def cacar_nome_antigo(raw_original: str, bairro: str, cidade: str, uf: str, cep_fmt: str):
    # busca top-5 e pega o mais parecido com o texto original
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

    if melhor[0] is not None and melhor[2] >= 0.42:
        return melhor[0], melhor[1], melhor[3], melhor[2]
    return None, None, "", 0.0


# =============================
# EXTRAÇÃO de endereços do texto bagunçado
# =============================
def extrair_enderecos_do_texto(texto: str):
    """
    Heurística:
    - Divide em linhas
    - Quando uma linha tem CEP, tenta montar endereço usando:
      - a própria linha
      - ou a linha anterior (se ela tem "Rua/Avenida/..." e a linha atual só tem CEP/UF/Cidade)
    """
    linhas = [l.rstrip() for l in (texto or "").splitlines()]
    candidatos = []

    for i, linha in enumerate(linhas):
        l = limpar_texto_endereco(linha)
        if not l:
            continue

        cep = extrair_cep(l)
        if not cep:
            continue

        # tenta juntar com a linha anterior se ajudar
        prev = limpar_texto_endereco(linhas[i - 1]) if i > 0 else ""
        merged = l
        if prev and linha_tem_via(prev) and (not linha_tem_via(l)):
            merged = f"{prev} {l}"

        # filtro mínimo: tem CEP e pelo menos alguma cara de endereço
        if linha_tem_via(merged) or linha_tem_via(prev):
            candidatos.append(merged)
        else:
            # ainda aceita, mas com cuidado (às vezes vem "Av Brasil 123 Manaus 690xxxxx")
            candidatos.append(merged)

    # tira duplicados mantendo ordem (mesmo cep+texto)
    seen = set()
    out = []
    for c in candidatos:
        key = normaliza(c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)

    return out


def montar_csv_base64(rows, filename):
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
    w.writerows(rows)
    data = out.getvalue().encode("utf-8")
    b64 = base64.b64encode(data).decode("ascii")
    return b64, filename


# =============================
# Rotas
# =============================
@app.get("/")
def home():
    return HTML


@app.post("/process_stream")
def process_stream():
    payload = request.get_json(silent=True) or {}
    password = (payload.get("password") or "").strip()
    text = payload.get("text") or ""

    if password != APP_PASSWORD:
        return Response("Senha inválida.", status=401)

    enderecos = extrair_enderecos_do_texto(text)
    total = len(enderecos)
    if total == 0:
        return Response("Não achei nenhum endereço com CEP no texto. Cola de novo (tem que ter CEP).", status=400)

    def stream():
        inicio = datetime.now()

        ok_rows = []
        revisar_rows = []
        cache_hits = 0
        cache_updates = 0
        cacados = 0

        for idx, raw in enumerate(enderecos, start=1):
            percent = (idx / total) * 100.0
            yield (csv_json({"type": "progress", "percent": percent, "current": idx, "total": total, "msg": raw[:70]}) + "\n")

            raw_limpo = limpar_texto_endereco(raw)
            cep8 = extrair_cep(raw_limpo)
            if not cep8:
                revisar_rows.append([raw_limpo, "SEM_CEP"])
                continue

            dados = buscar_viacep(cep8)
            if not dados:
                revisar_rows.append([raw_limpo, f"CEP_INVALIDO ({cep8})"])
                # sem ViaCEP, fallback só com CEP mesmo
                cep_fmt = formata_cep(cep8)
                ok_rows.append([idx, f"{cep_fmt}, Manaus-AM", "", "Manaus", cep_fmt, "", "", f"FALLBACK: CEP_INVALIDO | original: {raw_limpo}"])
                continue

            cidade_via = (dados.get("localidade") or "").strip().lower()
            if cidade_via and cidade_via != "manaus":
                revisar_rows.append([raw_limpo, f"CEP_FORA_MANAUS ({dados.get('localidade','')})"])
                cep_fmt = formata_cep(cep8)
                ok_rows.append([idx, f"{cep_fmt}, Manaus-AM", "", "Manaus", cep_fmt, "", "", f"FALLBACK: CEP_FORA_MANAUS | original: {raw_limpo}"])
                continue

            logradouro = (dados.get("logradouro") or "").strip()
            bairro = (dados.get("bairro") or "").strip()
            cidade = "Manaus"
            uf = "AM"
            cep_fmt = formata_cep(cep8)
            numero = extrair_numero(raw_limpo)

            # Cache memória
            k = cache_key(cep_fmt, numero, logradouro, raw_limpo)
            if k in GEO_CACHE:
                lat, lon, _ts = GEO_CACHE[k]
                if dentro_de_manaus(lat, lon):
                    cache_hits += 1
                    dest = montar_destino(logradouro, numero, bairro, cidade, uf, cep_fmt, raw_limpo)
                    ok_rows.append([idx, dest, bairro, cidade, cep_fmt, lat, lon, "CACHE"])
                    continue

            # Tentativas de geocode
            lat = lon = None
            note = ""

            queries = []
            if logradouro:
                queries.append(f"{logradouro}, {numero}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}")
            queries.append(f"{raw_limpo}, {bairro}, {cidade}-{uf}, {cep_fmt}, {COUNTRY}")

            for q in queries:
                lat, lon = tentar_1_resultado(q, bounded=True)
                if lat is not None and dentro_de_manaus(lat, lon):
                    break
                lat2, lon2 = tentar_1_resultado(q, bounded=False)
                if lat2 is not None and dentro_de_manaus(lat2, lon2):
                    lat, lon = lat2, lon2
                    break

            # caça “nome antigo” (top-5)
            if lat is None or lon is None or (not dentro_de_manaus(lat, lon)):
                lat3, lon3, display3, score3 = cacar_nome_antigo(raw_limpo, bairro, cidade, uf, cep_fmt)
                if lat3 is not None and dentro_de_manaus(lat3, lon3):
                    lat, lon = lat3, lon3
                    cacados += 1
                    note = f"CAÇADO(score={score3:.2f}): {display3[:90]}"

            dest = montar_destino(logradouro, numero, bairro, cidade, uf, cep_fmt, raw_limpo)

            # Fallback inteligente (não fica “CEP pelado”)
            if lat is None or lon is None:
                revisar_rows.append([raw_limpo, "NAO_ENCONTRADO"])
                ok_rows.append([idx, dest, bairro, cidade, cep_fmt, "", "", f"FALLBACK: NAO_ENCONTRADO | original: {raw_limpo}"])
                continue

            if not dentro_de_manaus(lat, lon):
                revisar_rows.append([raw_limpo, f"FORA_MANAUS ({lat},{lon})"])
                ok_rows.append([idx, dest, bairro, cidade, cep_fmt, "", "", f"FALLBACK: FORA_MANAUS | original: {raw_limpo}"])
                continue

            ok_rows.append([idx, dest, bairro, cidade, cep_fmt, lat, lon, note])

            GEO_CACHE[k] = (lat, lon, datetime.now().isoformat(timespec="seconds"))
            cache_updates += 1

        dur = (datetime.now() - inicio).total_seconds()
        filename = f"circuit_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        b64, _ = montar_csv_base64(ok_rows, filename)

        yield (csv_json({
            "type": "done",
            "ok": len(ok_rows),
            "revisar": len(revisar_rows),
            "seconds": dur,
            "filename": filename,
            "csv_base64": b64,
            "cache_hits": cache_hits,
            "cache_updates": cache_updates,
            "cacados": cacados
        }) + "\n")

    return Response(stream(), mimetype="text/plain; charset=utf-8")


def montar_destino(logradouro, numero, bairro, cidade, uf, cep_fmt, raw_limpo):
    # Prioriza o “oficial” do ViaCEP, mas não perde informação
    parts = []
    if logradouro:
        parts.append(f"{logradouro}, {numero}".strip().strip(","))
    else:
        parts.append(raw_limpo)

    # Bairro ajuda MUITO o Circuit a “puxar pro lugar”
    if bairro:
        parts.append(bairro)

    parts.append(f"{cidade}-{uf}")
    if cep_fmt:
        parts.append(cep_fmt)

    return ", ".join([p for p in parts if p]).strip()


def csv_json(d: dict) -> str:
    # JSON simples sem depender de lib
    # (evita import json? mas pode usar json tranquilo. aqui é só pra manter leve)
    import json
    return json.dumps(d, ensure_ascii=False)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
