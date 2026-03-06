import re
import csv
import time
import os
import requests
from datetime import datetime
import difflib

# =========================
# ROTEIRIZADOR OFFLINE v6.1 (CMD)
# - Parser robusto (2+ modelos)
# - Trava opcional de bairro
# - CEP manda / rua refina
# - Mantém duplicados
# - Barra de progresso
# - Timeout por endereço (max 15s)
# - Anti "ponto doido" (river/waterway/etc)
# =========================

# ===== SSL FIX (Windows) =====
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
except Exception:
    pass

# ===== CONFIG =====
USER_AGENT = "rota-privada-cmd/6.1"
COUNTRY = "Brazil"

# Ajuste fino: menor = mais rápido, maior = mais educado com Nominatim
SLEEP_NOMINATIM = 0.65

# Manaus viewbox aproximada
MANAUS_VIEWBOX = (-60.30, -3.25, -59.80, -2.85)  # west, south, east, north

# Caches
GEOCODE_CACHE_FILE = "geocode_cache.csv"
VIACEP_CACHE_FILE = "viacep_cache.csv"

# Limites
MAX_SECONDS_PER_ADDRESS = 15.0
NOMINATIM_TIMEOUT = 8.0
VIACEP_TIMEOUT = 10.0

# Trava opcional: se preencher, só aceita resultados desse bairro
# Ex: "GILBERTO MESTRINHO"
BAIRRO_FIXO = ""  # <-- coloque aqui se quiser travar geral


# ============ HELPERS ============

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def formata_cep(cep: str) -> str:
    c = only_digits(cep)
    if len(c) == 8:
        return f"{c[:5]}-{c[5:]}"
    return cep.strip()

def normaliza(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def barra_progresso(i, total, extra=""):
    if total <= 0:
        return ""
    pct = (i / total) * 100
    total_bar = 30
    filled = int(total_bar * pct / 100)
    bar = "█" * filled + "░" * (total_bar - filled)
    return f"[{bar}] {pct:5.1f}% ({i}/{total}) {extra}".strip()

def dentro_de_manaus(lat, lon):
    west, south, east, north = MANAUS_VIEWBOX
    return (west <= lon <= east) and (south <= lat <= north)

def similaridade(a, b):
    a = normaliza(a)
    b = normaliza(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def limpar_linha(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def canonicaliza_via(s: str) -> str:
    """R/AV/TRAV/BCO etc => forma mais legível pro Nominatim"""
    s0 = s or ""
    s0 = re.sub(r"^\s*R\b\.?\s*", "Rua ", s0, flags=re.IGNORECASE)
    s0 = re.sub(r"^\s*AV\b\.?\s*", "Avenida ", s0, flags=re.IGNORECASE)
    s0 = re.sub(r"^\s*TV\b\.?\s*", "Travessa ", s0, flags=re.IGNORECASE)
    s0 = re.sub(r"^\s*TRAV\b\.?\s*", "Travessa ", s0, flags=re.IGNORECASE)
    s0 = re.sub(r"^\s*BCO\b\.?\s*", "Beco ", s0, flags=re.IGNORECASE)
    s0 = re.sub(r"^\s*EST\b\.?\s*", "Estrada ", s0, flags=re.IGNORECASE)
    return limpar_linha(s0)

def extrair_cep_de_linha(line: str):
    # aceita "CEP: 69086645" ou "69086645"
    m = re.search(r"\b(\d{5}-?\d{3})\b", line)
    if not m:
        m = re.search(r"\b(\d{8})\b", line)
    if m:
        return only_digits(m.group(1))
    return None

def extrair_numero(endereco: str):
    # pega o primeiro número "bonitinho"
    m = re.search(r"\b(\d{1,6}[A-Za-z]?)\b", endereco)
    return m.group(1) if m else "S/N"

def extrair_bairro_de_endereco(endereco: str):
    """
    Pega bairro quando vem tipo:
      '..., 457 - GILBERTO MESTRI - MANAUS/AM'
    ou
      '..., 457 - GILBERTO MESTRI - MANAUS/AM CEP:...'
    """
    # tenta pelo padrão " - BAIRRO - "
    parts = [p.strip() for p in re.split(r"\s-\s", endereco)]
    # Ex: ["R SERRA DO SOL, 457", "GILBERTO MESTRI", "MANAUS/AM"]
    if len(parts) >= 2:
        # bairro costuma ser a segunda parte
        bairro = parts[1].strip()
        # limpa lixo
        bairro = re.sub(r"\b(manaus|am|amazonas)\b", "", bairro, flags=re.IGNORECASE).strip()
        if bairro and len(bairro) >= 3:
            return bairro
    return ""

def extrair_cidade_uf(endereco: str):
    # tenta "MANAUS/AM" ou "MANAUS - AM"
    m = re.search(r"\b([A-Za-zÀ-ÿ\s]+)\s*(?:/|-)\s*([A-Za-z]{2})\b", endereco)
    if m:
        city = limpar_linha(m.group(1))
        uf = m.group(2).upper()
        return city, uf
    return "Manaus", "AM"

def strip_cep_text(s: str):
    s = re.sub(r"\bCEP\b\s*:?\s*\d{5}-?\d{3}\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\b\d{5}-?\d{3}\b", "", s)
    s = re.sub(r"\b\d{8}\b", "", s)
    return limpar_linha(s)

def is_linha_nome(line: str):
    # Heurística: nome geralmente tem poucas vírgulas e não tem via/cep
    if not line:
        return False
    if extrair_cep_de_linha(line):
        return False
    if re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça)\b", line, flags=re.IGNORECASE):
        return False
    # evita códigos BR... / BLI_...
    if re.search(r"\bBR\d{6,}\b", line) or re.search(r"\bBLI[_-]?\d+\b", line, flags=re.IGNORECASE):
        return False
    # nome costuma ter 2+ palavras
    toks = [t for t in line.split() if t.strip()]
    return len(toks) >= 2

def is_linha_endereco(line: str):
    if not line:
        return False
    if re.search(r"\b(rua|avenida|av\.|travessa|beco|estrada|alameda|praça|rio)\b", line, flags=re.IGNORECASE):
        return True
    # ou linha com número e vírgula
    if re.search(r",\s*\d", line):
        return True
    # ou linha com " - MANAUS" etc
    if re.search(r"\bmanaus\b", line, flags=re.IGNORECASE):
        return True
    return False


# ============ VIA CEP CACHE ============

def carregar_viacep_cache():
    cache = {}
    if not os.path.exists(VIACEP_CACHE_FILE):
        return cache
    try:
        with open(VIACEP_CACHE_FILE, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                cep = only_digits(row.get("cep", ""))
                if len(cep) != 8:
                    continue
                cache[cep] = {
                    "logradouro": row.get("logradouro", "") or "",
                    "bairro": row.get("bairro", "") or "",
                    "localidade": row.get("localidade", "") or "",
                    "uf": row.get("uf", "") or "",
                    "updated_at": row.get("updated_at", "") or "",
                }
    except Exception:
        return {}
    return cache

def salvar_viacep_cache(cache):
    try:
        with open(VIACEP_CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["cep", "logradouro", "bairro", "localidade", "uf", "updated_at"])
            for cep, d in cache.items():
                w.writerow([
                    cep,
                    d.get("logradouro", ""),
                    d.get("bairro", ""),
                    d.get("localidade", ""),
                    d.get("uf", ""),
                    d.get("updated_at", ""),
                ])
    except Exception:
        pass

def buscar_viacep(cep8, viacep_cache):
    cep8 = only_digits(cep8)
    if len(cep8) != 8:
        return None

    if cep8 in viacep_cache:
        return viacep_cache[cep8]

    url = f"https://viacep.com.br/ws/{cep8}/json/"
    try:
        r = requests.get(url, timeout=VIACEP_TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            if "erro" in j:
                return None
            viacep_cache[cep8] = {
                "logradouro": (j.get("logradouro") or "").strip(),
                "bairro": (j.get("bairro") or "").strip(),
                "localidade": (j.get("localidade") or "").strip(),
                "uf": (j.get("uf") or "").strip(),
                "updated_at": now_iso(),
            }
            return viacep_cache[cep8]
    except Exception:
        return None
    return None


# ============ GEOCODE CACHE ============

def carregar_geocode_cache():
    cache = {}
    if not os.path.exists(GEOCODE_CACHE_FILE):
        return cache
    try:
        with open(GEOCODE_CACHE_FILE, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                k = row.get("key", "")
                try:
                    lat = float(row.get("lat", ""))
                    lon = float(row.get("lon", ""))
                except Exception:
                    continue
                cache[k] = (lat, lon, row.get("updated_at", ""))
    except Exception:
        return {}
    return cache

def salvar_geocode_cache(cache):
    try:
        with open(GEOCODE_CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["key", "lat", "lon", "updated_at"])
            for k, (lat, lon, ts) in cache.items():
                w.writerow([k, lat, lon, ts])
    except Exception:
        pass

def cache_key(cep_fmt, numero, bairro, base_rua):
    base = normaliza(base_rua)
    b = normaliza(bairro)
    return f"{cep_fmt}|{str(numero).upper()}|{b}|{base}"


# ============ NOMINATIM ============

def nominatim_search(query, bounded=True, limit=5):
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
        r = requests.get(url, params=params, headers=headers, timeout=NOMINATIM_TIMEOUT)
        time.sleep(SLEEP_NOMINATIM)
        if r.status_code == 200:
            try:
                return r.json() or []
            except Exception:
                return []
    except Exception:
        return []
    return []

def resultado_e_lugar_ruim(item):
    # filtros anti "ponto doido"
    cls = (item.get("class") or "").lower()
    typ = (item.get("type") or "").lower()
    display = (item.get("display_name") or "").lower()

    ruins_cls = {"waterway", "natural"}
    ruins_typ = {"river", "stream", "canal", "lake", "reservoir", "bay", "wetland"}

    if cls in ruins_cls:
        return True
    if typ in ruins_typ:
        return True
    if any(w in display for w in [" rio ", " igarapé", " igarape", " lago ", " canal "]):
        # às vezes o endereço tem "Rio X" e é rua. então só marca ruim se também vier como waterway/natural.
        if cls in ruins_cls or typ in ruins_typ:
            return True
    return False

def pega_bairro_do_resultado(item):
    addr = item.get("address") or {}
    # Nominatim pode usar suburb/neighbourhood/city_district
    cand = (
        addr.get("suburb")
        or addr.get("neighbourhood")
        or addr.get("city_district")
        or addr.get("district")
        or ""
    )
    return (cand or "").strip()

def bairro_bate(bairro_esperado, item):
    if not bairro_esperado:
        return True
    b = normaliza(bairro_esperado)
    if not b:
        return True
    b_res = normaliza(pega_bairro_do_resultado(item)) or normaliza(item.get("display_name", ""))
    # tenta match por "contém" ou similaridade
    if b and b in b_res:
        return True
    if similaridade(bairro_esperado, pega_bairro_do_resultado(item)) >= 0.62:
        return True
    return False

def escolher_melhor_candidato(candidatos, rua_raw, bairro_esperado):
    melhor = (None, None, -1.0, None)  # lat, lon, score, item
    for c in candidatos:
        try:
            lat = float(c.get("lat"))
            lon = float(c.get("lon"))
        except Exception:
            continue

        if not dentro_de_manaus(lat, lon):
            continue
        if resultado_e_lugar_ruim(c):
            continue
        if not bairro_bate(bairro_esperado, c):
            continue

        display = c.get("display_name", "") or ""
        score = similaridade(rua_raw, display)

        cls = (c.get("class") or "").lower()
        typ = (c.get("type") or "").lower()
        # bônus para endereços residenciais/ruas
        if cls in ("highway", "building", "amenity"):
            score += 0.08
        if typ in ("residential", "road", "house", "yes"):
            score += 0.08

        if score > melhor[2]:
            melhor = (lat, lon, score, c)

    if melhor[0] is not None:
        return melhor[0], melhor[1], melhor[2], melhor[3]
    return None, None, 0.0, None


# ============ PARSER ROBUSTO (2+ MODELOS) ============

def parse_entregas(texto_colado: str):
    """
    Aceita:
    - Modelo A (3 linhas): NOME / ENDERECO / CEP
    - Modelo B: NOME / ENDERECO (com CEP dentro) / ...
    - Modelo C: ENDERECO / CEP / (nome opcional)
    - Com ou sem 'CEP:'
    Mantém duplicados.
    """
    linhas = [limpar_linha(l) for l in (texto_colado or "").splitlines()]
    # remove linhas vazias, mas mantém separadores lógicos
    # vamos guardar com vazios para detectar blocos
    raw_lines = [l for l in linhas]

    entregas = []
    i = 0

    while i < len(raw_lines):
        line = raw_lines[i].strip()

        # pula vazios
        if not line:
            i += 1
            continue

        cep_here = extrair_cep_de_linha(line)

        if cep_here:
            # quando linha é CEP, olha pra trás pra montar registro
            cep8 = cep_here
            # pega até 3 linhas anteriores não vazias
            prev = []
            j = i - 1
            while j >= 0 and len(prev) < 3:
                if raw_lines[j].strip():
                    prev.append(raw_lines[j].strip())
                j -= 1
            prev = prev[::-1]

            nome = ""
            endereco = ""
            bairro = ""

            # heurística: último anterior que parece endereço
            for p in prev[::-1]:
                if is_linha_endereco(p):
                    endereco = p
                    break

            # nome: algum anterior que parece nome e não é o endereço escolhido
            for p in prev:
                if p != endereco and is_linha_nome(p):
                    nome = p
                    break

            if endereco:
                bairro = extrair_bairro_de_endereco(endereco)

            entregas.append({
                "nome": nome,
                "endereco_raw": endereco,
                "bairro_raw": bairro,
                "cep8": cep8
            })
            i += 1
            continue

        # se não é CEP, pode ser linha com CEP dentro
        cep_inline = extrair_cep_de_linha(line)
        if cep_inline:
            # trata como endereço (linha inteira)
            cep8 = cep_inline
            endereco = strip_cep_text(line)
            nome = ""
            bairro = extrair_bairro_de_endereco(endereco)

            # tenta nome na linha anterior
            if i - 1 >= 0 and raw_lines[i - 1].strip() and is_linha_nome(raw_lines[i - 1]):
                nome = raw_lines[i - 1].strip()

            entregas.append({
                "nome": nome,
                "endereco_raw": endereco,
                "bairro_raw": bairro,
                "cep8": cep8
            })
            i += 1
            continue

        i += 1

    # pós-limpeza: filtra só quem tem cep
    entregas = [e for e in entregas if len(only_digits(e.get("cep8", ""))) == 8]

    # tenta completar endereço vazio olhando 1-2 linhas acima do CEP (às vezes não pegou)
    # (já fizemos isso, mas algumas listas são doidas)
    entregas_fix = []
    for e in entregas:
        end = (e.get("endereco_raw") or "").strip()
        if end:
            entregas_fix.append(e)
        else:
            # deixa mesmo assim: vai fallback ViaCEP
            entregas_fix.append(e)

    return entregas_fix


# ============ PROCESSAMENTO ============

def montar_queries(cep_fmt, numero, bairro_alvo, rua_lista, via_logradouro, city="Manaus", uf="AM"):
    rua_lista = canonicaliza_via(rua_lista)
    via_logradouro = canonicaliza_via(via_logradouro)

    # Ordem forte: CEP + numero, depois ViaCEP, depois rua_lista
    queries = []

    # 1) CEP + número (às vezes resolve quando rua muda)
    # (Nominatim às vezes não entende só CEP, mas ajuda combinado)
    if cep_fmt:
        queries.append(f"{cep_fmt}, {numero}, {bairro_alvo}, {city}-{uf}, {COUNTRY}".strip(", "))

    # 2) Rua oficial do ViaCEP + número + bairro + CEP
    if via_logradouro:
        queries.append(f"{via_logradouro}, {numero}, {bairro_alvo}, {city}-{uf}, {cep_fmt}, {COUNTRY}".strip(", "))

    # 3) Rua da lista + número + bairro + CEP
    if rua_lista:
        queries.append(f"{rua_lista}, {numero}, {bairro_alvo}, {city}-{uf}, {cep_fmt}, {COUNTRY}".strip(", "))

    # 4) Rua (sem número) - às vezes número bagunça
    if via_logradouro:
        queries.append(f"{via_logradouro}, {bairro_alvo}, {city}-{uf}, {cep_fmt}, {COUNTRY}".strip(", "))

    return [q for q in queries if len(q) >= 8]


def extrair_rua_da_linha(endereco_raw: str):
    # remove " - BAIRRO - MANAUS/AM"
    s = endereco_raw or ""
    # corta depois do primeiro " - "
    s = re.split(r"\s-\s", s)[0].strip()
    s = strip_cep_text(s)
    s = canonicaliza_via(s)
    return s

def detectar_bairro_alvo(entrega_bairro_raw: str, viacep_bairro: str):
    # prioridade: trava global > bairro da lista > bairro do viacep
    if BAIRRO_FIXO.strip():
        return BAIRRO_FIXO.strip()
    if entrega_bairro_raw and entrega_bairro_raw.strip():
        return entrega_bairro_raw.strip()
    return (viacep_bairro or "").strip()

def geocodificar_entrega(entrega, geocode_cache, viacep_cache):
    """
    Retorna:
      (lat, lon, bairro_final, dest_address, notes, usado_cache)
    Sempre respeita:
      - máximo 15s por endereço
      - filtro por bairro (se tiver)
      - viewbox Manaus
    """
    start = time.monotonic()
    def estourou():
        return (time.monotonic() - start) > MAX_SECONDS_PER_ADDRESS

    cep8 = only_digits(entrega.get("cep8", ""))
    cep_fmt = formata_cep(cep8)
    endereco_raw = (entrega.get("endereco_raw") or "").strip()
    rua_lista = extrair_rua_da_linha(endereco_raw)
    numero = extrair_numero(rua_lista)

    # ViaCEP
    via = buscar_viacep(cep8, viacep_cache)
    if not via:
        # sem ViaCEP => fallback seco, mas com bairro da lista se houver
        bairro_alvo = detectar_bairro_alvo(entrega.get("bairro_raw", ""), "")
        dest = f"{rua_lista or cep_fmt}, {numero}, {bairro_alvo}, Manaus-AM".strip(", ")
        return None, None, bairro_alvo, dest, "FALLBACK: VIACEP_FALHOU", False

    if normaliza(via.get("localidade")) != "manaus":
        bairro_alvo = detectar_bairro_alvo(entrega.get("bairro_raw", ""), via.get("bairro", ""))
        dest = f"{via.get('logradouro','') or rua_lista}, {numero}, {bairro_alvo}, {via.get('localidade','')}-{via.get('uf','')}".strip(", ")
        return None, None, bairro_alvo, dest, f"FALLBACK: CEP_FORA_MANAUS ({via.get('localidade','')})", False

    via_logradouro = via.get("logradouro", "")
    via_bairro = via.get("bairro", "")
    city = via.get("localidade", "Manaus") or "Manaus"
    uf = via.get("uf", "AM") or "AM"

    bairro_alvo = detectar_bairro_alvo(entrega.get("bairro_raw", ""), via_bairro)

    # Cache key forte
    base_rua = via_logradouro or rua_lista or endereco_raw or cep_fmt
    k = cache_key(cep_fmt, numero, bairro_alvo, base_rua)

    if k in geocode_cache:
        lat, lon, ts = geocode_cache[k]
        if dentro_de_manaus(lat, lon):
            dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
            return lat, lon, bairro_alvo, dest, f"CACHE({ts})", True

    # Se estourou tempo, nem tenta
    if estourou():
        dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
        return None, None, bairro_alvo, dest, "FALLBACK: TIMEOUT_PRE", False

    # Monta queries
    queries = montar_queries(
        cep_fmt=cep_fmt,
        numero=numero,
        bairro_alvo=bairro_alvo,
        rua_lista=rua_lista,
        via_logradouro=via_logradouro,
        city=city,
        uf=uf
    )

    # tenta bounded -> unbounded (mas sempre filtrando Manaus e bairro)
    melhor_lat = melhor_lon = None
    melhor_score = 0.0
    melhor_note = ""

    for idx, q in enumerate(queries, start=1):
        if estourou():
            break

        # bounded top-5
        cand = nominatim_search(q, bounded=True, limit=5)
        lat, lon, score, item = escolher_melhor_candidato(cand, rua_lista or endereco_raw or q, bairro_alvo)
        if lat is not None and score >= melhor_score:
            melhor_lat, melhor_lon, melhor_score = lat, lon, score
            melhor_note = f"NOMINATIM_BOUNDED(q{idx},score={score:.2f})"
            if score >= 0.78:
                break

        if estourou():
            break

        # unbounded top-5
        cand2 = nominatim_search(q, bounded=False, limit=5)
        lat2, lon2, score2, item2 = escolher_melhor_candidato(cand2, rua_lista or endereco_raw or q, bairro_alvo)
        if lat2 is not None and score2 >= melhor_score:
            melhor_lat, melhor_lon, melhor_score = lat2, lon2, score2
            melhor_note = f"NOMINATIM_FREE(q{idx},score={score2:.2f})"
            if score2 >= 0.78:
                break

    # Se achou
    if melhor_lat is not None and melhor_lon is not None and dentro_de_manaus(melhor_lat, melhor_lon):
        geocode_cache[k] = (melhor_lat, melhor_lon, now_iso())
        dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
        return melhor_lat, melhor_lon, bairro_alvo, dest, melhor_note, False

    # fallback: NÃO deixa "CEP seco" — usa ViaCEP + número + bairro
    dest = f"{via_logradouro or rua_lista}, {numero}".strip(", ")
    note = "FALLBACK: NAO_ENCONTRADO"
    return None, None, bairro_alvo, dest, note, False


def main():
    print("\n==============================")
    print("ROTEIRIZADOR PRIVADO CMD v6.1")
    print("==============================")
    print("Cole sua lista (qualquer modelo) e digite FIM numa linha nova.")
    if BAIRRO_FIXO.strip():
        print(f"🔒 TRAVA DE BAIRRO ATIVA: {BAIRRO_FIXO.strip()}")
    else:
        print("🔓 Trava de bairro: DESLIGADA (usa bairro da lista / ViaCEP)")
    print(f"⏱️ Máximo {MAX_SECONDS_PER_ADDRESS:.0f}s por endereço (anti-travamento)")
    print("")

    # lê tudo até FIM
    linhas = []
    while True:
        try:
            l = input()
        except EOFError:
            break
        if l.strip().upper() == "FIM":
            break
        linhas.append(l)

    texto = "\n".join(linhas).strip()
    if not texto:
        print("\nNada pra processar. Saindo...")
        print("Dica: cole a lista e finalize com FIM.")
        input("Pressione Enter para sair...")
        return

    entregas = parse_entregas(texto)

    total = len(entregas)
    print(f"\nTotal de entregas detectadas: {total}")

    if total == 0:
        print("\nNada detectado (precisa ter CEP de 8 dígitos).")
        print("Dica: o CEP pode estar em linha separada ou 'CEP: 69086645'.")
        input("Pressione Enter para sair...")
        return

    geocode_cache = carregar_geocode_cache()
    viacep_cache = carregar_viacep_cache()

    ok_rows = []
    revisar_rows = []

    cache_hits = 0
    cache_updates = 0
    t0 = datetime.now()

    for idx, ent in enumerate(entregas, start=1):
        # progress
        preview = ent.get("endereco_raw", "") or f"CEP {formata_cep(ent.get('cep8',''))}"
        print(barra_progresso(idx, total, extra=preview[:70]))

        lat, lon, bairro_final, dest, note, used_cache = geocodificar_entrega(ent, geocode_cache, viacep_cache)
        cep_fmt = formata_cep(ent.get("cep8", ""))
        city, uf = "Manaus", "AM"

        if used_cache:
            cache_hits += 1
        else:
            # se salvou algo em cache no geocode, conta como update (não dá pra saber 100% aqui, mas ok)
            if note.startswith("NOMINATIM_"):
                cache_updates += 1

        # row pro Circuit (mantém duplicados porque estamos iterando entregas, não deduplicando)
        ok_rows.append([
            idx,
            dest,
            bairro_final,
            city,
            cep_fmt,
            (f"{lat:.6f}" if isinstance(lat, float) else ""),
            (f"{lon:.6f}" if isinstance(lon, float) else ""),
            note
        ])

        if note.startswith("FALLBACK"):
            revisar_rows.append([
                ent.get("nome", ""),
                ent.get("endereco_raw", ""),
                cep_fmt,
                bairro_final,
                note
            ])

    # salva caches
    salvar_geocode_cache(geocode_cache)
    salvar_viacep_cache(viacep_cache)

    # saída única
    out_csv = "circuit_import_v6_1.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Sequence", "Destination Address", "Bairro", "City", "Zipcode/Postal Code", "Latitude", "Longitude", "Notes"])
        w.writerows(ok_rows)

    # lista de revisão (opcional, mas útil)
    rev_csv = "revisar_manual_v6_1.csv"
    with open(rev_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["nome", "endereco_original", "cep", "bairro", "motivo"])
        w.writerows(revisar_rows)

    dt = (datetime.now() - t0).total_seconds()

    print("\n==============================")
    print("Processamento finalizado v6.1")
    print(f"Tempo total: {dt:.1f}s")
    print(f"OK (inclui fallback): {len(ok_rows)}")
    print(f"Revisar (fallback): {len(revisar_rows)}")
    print(f"Cache hits: {cache_hits}")
    print(f"Cache updates (aprox): {cache_updates}")
    print(f"Arquivo Circuit: {out_csv}")
    print(f"Arquivo Revisão: {rev_csv}")
    print("==============================")
    input("Pressione Enter para sair...")


if __name__ == "__main__":
    main()
