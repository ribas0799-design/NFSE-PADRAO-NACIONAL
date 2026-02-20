# -*- coding: utf-8 -*-
"""
Robô NFS-e (Portal Nacional) - Premier by Matheus Tecnologia
V3.1.2 — Fix vRetCP + Python 3.14 WMI + Retenções Corretas + Interface Moderna
"""

# ═══════════════════════════════════════════════════════
# FIX PYTHON 3.14 — DEVE SER O PRIMEIRO CÓDIGO DO SCRIPT
# ═══════════════════════════════════════════════════════
import sys
import platform as _platform

if sys.version_info >= (3, 14):
    if hasattr(_platform, '_wmi_query'):
        _orig_wmi = _platform._wmi_query
        def _safe_wmi_query(*args, **kwargs):
            try:
                return _orig_wmi(*args, **kwargs)
            except (OSError, TimeoutError, Exception):
                return {}
        _platform._wmi_query = _safe_wmi_query
    try:
        _platform.win32_ver()
    except Exception:
        pass

# ═══════════════════════════════════════════════════════
# IMPORTS NORMAIS (agora seguros)
# ═══════════════════════════════════════════════════════
import os
import re
import time
import logging
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as UrllibRetry
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)
import xml.etree.ElementTree as ET

# ═══════════════════════════════════════════════════════
# CONFIGURAÇÕES — VELOCIDADE MÁXIMA
# ═══════════════════════════════════════════════════════
CLICK_DELAY  = 0.15
POPUP_WAIT   = 1.8
PAGE_WAIT    = 1.0
FILTER_WAIT  = 1.2
RETRY_WAIT   = 5
MAX_RETRIES  = 3
DL_WORKERS   = 8
DL_TIMEOUT   = 15
SESSION_POOL = 20

RE_DATE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
RE_ISO  = re.compile(r"(\d{4})-(\d{2})")
RE_BR   = re.compile(r"(\d{2})/(\d{2})/(\d{4})")

# ═══════════════════════════════════════════════════════
# TABELAS DE DESCRIÇÃO
# ═══════════════════════════════════════════════════════
DESC_RET_ISSQN = {
    "1": "Não Retido",
    "2": "Retido pelo Tomador",
    "3": "Retido pelo Intermediário",
}

DESC_RET_PISCOFINS = {
    "1": "Retido",
    "2": "Não Retido",
}

DESC_CST_PISCOFINS = {
    "00": "Nenhum",
    "01": "Tributável – Alíquota Básica",
    "02": "Tributável – Alíquota Diferenciada",
    "03": "Tributável – Alíq. Unid. Medida Produto",
    "04": "Tributável Monofásica – Revenda Alíq. Zero",
    "05": "Tributável por Substituição Tributária",
    "06": "Tributável a Alíquota Zero",
    "07": "Isenta da Contribuição",
    "08": "Sem Incidência da Contribuição",
    "09": "Com Suspensão da Contribuição",
}

COLUNAS = [
    "Página", "Linha", "Nº NFSe", "Chave", "Competência", "Data Emissão",
    "CNPJ Prestador", "Razão Social Prestador",
    "CNPJ Tomador", "CPF Tomador", "Razão Social Tomador",
    "Código Tributação Nacional", "Descrição Serviço", "Local da Prestação",
    "Valor dos Serviços", "Valor Deduções",
    "Desconto Incondicionado", "Desconto Condicionado",
    "Base de Cálculo", "Alíquota ISS", "Valor ISS",
    "tpRetISSQN", "Desc. Ret. ISSQN", "ISS Retido",
    "CST PIS/COFINS", "Desc. CST PIS/COFINS",
    "Base PIS/COFINS", "Alíq PIS", "Alíq COFINS",
    "Valor PIS", "Valor COFINS",
    "tpRetPisCofins", "Desc. Ret. PIS/COFINS",
    "PIS Retido", "COFINS Retido",
    "IR Retido", "CSLL Retido", "INSS Retido",
    "Outras Retenções", "Total Retenções", "Valor Líquido",
    "Situação", "Tipo",
]


# ═══════════════════════════════════════════════════════
# TEMA
# ═══════════════════════════════════════════════════════
class Theme:
    BG         = "#0f1117"
    BG2        = "#161b22"
    BG3        = "#1c2333"
    CARD       = "#21262d"
    BORDER     = "#30363d"
    FG         = "#e6edf3"
    FG2        = "#8b949e"
    ACCENT     = "#58a6ff"
    GREEN      = "#3fb950"
    RED        = "#f85149"
    BTN_BG     = "#238636"
    BTN_BG2    = "#1f6feb"
    BTN_HOVER  = "#2ea043"
    BTN_HOVER2 = "#388bfd"
    ENTRY_BG   = "#0d1117"
    ENTRY_BD   = "#30363d"
    FONT       = "Segoe UI"
    FONT_MONO  = "Cascadia Code"


# ═══════════════════════════════════════════════════════
# LOGGER
# ═══════════════════════════════════════════════════════
_log_callback = None


def configure_logger(enable_file=False, path="automat.log"):
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
    handlers = [logging.StreamHandler()]
    if enable_file:
        handlers.append(logging.FileHandler(path, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s │ %(levelname)-7s │ %(message)s",
        handlers=handlers,
    )


def log_info(msg):
    logging.info(msg)
    if _log_callback:
        try:
            _log_callback(msg)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
# PASTAS
# ═══════════════════════════════════════════════════════
def ensure_dirs(base):
    for f in ("Recebidas/XML", "Recebidas/PDF", "Emitidas/XML", "Emitidas/PDF"):
        os.makedirs(os.path.join(base, f), exist_ok=True)


# ═══════════════════════════════════════════════════════
# NAVEGADOR
# ═══════════════════════════════════════════════════════
def start_browser():
    opts = Options()
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-logging")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option(
        "excludeSwitches", ["enable-logging", "enable-automation"]
    )
    opts.page_load_strategy = "eager"

    driver = webdriver.Edge(service=Service(), options=opts)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(0)
    wait = WebDriverWait(driver, 20)

    try:
        driver.delete_all_cookies()
    except Exception:
        pass

    driver.get("https://www.nfse.gov.br/EmissorNacional/Login")
    messagebox.showinfo(
        "Login",
        "Faça o LOGIN para inicar.\nClique OK quando estiver no Dashboard.",
    )
    try:
        wait.until(
            EC.any_of(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "img[src*='menu-recebidas']")
                ),
                EC.url_contains("/Dashboard"),
            )
        )
        driver.minimize_window()
    except Exception:
        pass
    log_info("✓ Login realizado")
    return driver, wait


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════
def _has_action(row):
    return bool(row.find_elements(By.CSS_SELECTOR, "a.icone-trigger"))


def _valid_rows(driver):
    return [
        r for r in driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if _has_action(r)
    ]


def _sem_reg(driver):
    try:
        return not _valid_rows(driver)
    except Exception:
        return True


def _erro_pg(driver):
    try:
        t = driver.find_element(By.TAG_NAME, "body").text.lower()
        return any(
            m in t for m in (
                "não foi possível", "tente novamente", "erro ao carregar",
                "serviço indisponível", "ocorreu um erro",
            )
        )
    except Exception:
        return False


def _aguardar(driver, wait, tipo):
    for tentativa in range(1, MAX_RETRIES + 1):
        if _sem_reg(driver):
            return "SEM"
        if _erro_pg(driver):
            if tentativa < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                driver.refresh()
                time.sleep(1)
                continue
            return False
        try:
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            if _valid_rows(driver):
                return True
            if _sem_reg(driver):
                return "SEM"
            if tentativa < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                driver.refresh()
                time.sleep(1)
                continue
            return "SEM"
        except TimeoutException:
            if _sem_reg(driver):
                return "SEM"
            if tentativa < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                driver.refresh()
                time.sleep(1)
            else:
                return False
    return False


# ═══════════════════════════════════════════════════════
# FILTRO
# ═══════════════════════════════════════════════════════
def _filtrar(driver, di, df):
    driver.execute_script(
        """
        var a=document.getElementById('datainicio'),
            b=document.getElementById('datafim');
        if(a){a.value=arguments[0];a.dispatchEvent(new Event('change',{bubbles:true}));}
        if(b){b.value=arguments[1];b.dispatchEvent(new Event('change',{bubbles:true}));}
        """,
        di, df,
    )
    time.sleep(0.3)
    ok = driver.execute_script(
        """
        var bs=document.querySelectorAll('button');
        for(var i=0;i<bs.length;i++){
            var t=bs[i].innerText.trim().toUpperCase();
            var img=bs[i].querySelector('img[src*="filtrar"]');
            if(t==='FILTRAR'||img){bs[i].click();return true;}
        }
        return false;
        """
    )
    if ok:
        time.sleep(FILTER_WAIT)
        if _sem_reg(driver):
            return "SEM"
    else:
        raise RuntimeError("Botão Filtrar não encontrado")
    return "OK"


# ═══════════════════════════════════════════════════════
# SESSÃO HTTP
# ═══════════════════════════════════════════════════════
def _criar_sessao(driver):
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=SESSION_POOL,
        pool_maxsize=SESSION_POOL,
        max_retries=UrllibRetry(total=2, backoff_factor=0.1),
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    for c in driver.get_cookies():
        dom = (c.get("domain") or "").lstrip(".")
        try:
            s.cookies.set(c["name"], c["value"], domain=dom)
        except Exception:
            s.cookies.set(c["name"], c["value"])
    try:
        ua = driver.execute_script("return navigator.userAgent;")
    except Exception:
        ua = "Mozilla/5.0"
    s.headers.update({"User-Agent": ua, "Connection": "keep-alive"})
    return s


def _baixar(session, url, path):
    try:
        r = session.get(url, timeout=DL_TIMEOUT, stream=True)
        if r.status_code == 200 and len(r.content) > 100:
            with open(path, "wb") as f:
                f.write(r.content)
            return True
    except Exception:
        pass
    return False


# ═══════════════════════════════════════════════════════
# SITUAÇÃO + POPUP
# ═══════════════════════════════════════════════════════
def _sit(row):
    try:
        for img in row.find_elements(By.CSS_SELECTOR, "img[src*='tb-']"):
            if "cancelada" in (img.get_attribute("src") or "").lower():
                return "Cancelada"
    except Exception:
        pass
    return "Emitida"


def _coletar_links(driver, row, pg, idx, tipo, xdir, pdir):
    try:
        btn = row.find_element(By.CSS_SELECTOR, "a.icone-trigger")
    except (NoSuchElementException, StaleElementReferenceException):
        return None

    sit = _sit(row)
    num = ""
    try:
        for td in row.find_elements(By.TAG_NAME, "td")[:3]:
            t = td.text.strip()
            if t.isdigit():
                num = t
                break
    except Exception:
        pass

    pref = f"{tipo}_p{pg}_l{idx}"
    xh = ph = None

    for tentativa in range(2):
        try:
            if tentativa == 1:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", btn
                )
                time.sleep(0.2)
            driver.execute_script("arguments[0].click();", btn)
            pop = WebDriverWait(driver, POPUP_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.popover"))
            )
            links = driver.execute_script(
                """
                var pop=arguments[0],r={x:null,p:null};
                pop.querySelectorAll('a[href]').forEach(function(a){
                    var h=a.href||'';
                    if(h.indexOf('/Download/NFSe/')>-1&&!r.x) r.x=h;
                    if(h.indexOf('/Download/DANFSe/')>-1&&!r.p) r.p=h;
                });
                return r;
                """,
                pop,
            )
            xh = links.get("x")
            ph = links.get("p")
            break
        except TimeoutException:
            if tentativa == 0:
                continue
        except Exception:
            break

    driver.execute_script(
        "document.querySelectorAll('.popover').forEach(e=>e.remove());"
    )
    time.sleep(CLICK_DELAY)

    return {
        "xh": xh,
        "ph": ph,
        "xp": os.path.join(xdir, f"{pref}.xml") if xh else None,
        "pp": os.path.join(pdir, f"{pref}.pdf") if ph else None,
        "log": {
            "PAGINA": pg, "LINHA": idx, "NUMERO_NFSE": num,
            "XML": f"{pref}.xml" if xh else "",
            "PDF": f"{pref}.pdf" if ph else "",
            "SITUACAO": sit, "TIPO": tipo,
        },
    }


# ═══════════════════════════════════════════════════════
# PAGINAÇÃO
# ═══════════════════════════════════════════════════════
def _next_pg(driver, tipo):
    try:
        ok = driver.execute_script(
            """
            var sels=[
                "a[href*='/""" + tipo + """?pg='][title*='Próxima']",
                "a[data-original-title='Próxima']",
                "a[title='Próxima']",
                "li.active+li a"
            ];
            for(var s=0;s<sels.length;s++){
                var els=document.querySelectorAll(sels[s]);
                for(var i=0;i<els.length;i++){
                    var e=els[i];
                    if(e.offsetParent===null) continue;
                    var p=e.parentElement;
                    if(p&&p.classList.contains('disabled')) return false;
                    e.click(); return true;
                }
            }
            return false;
            """
        )
        if ok:
            time.sleep(PAGE_WAIT)
            return True
    except Exception:
        pass
    return False


# ═══════════════════════════════════════════════════════
# PROCESSAR TABELA
# ═══════════════════════════════════════════════════════
def processar_tabela(driver, wait, tipo, base, di, df, on_progress=None):
    xdir = os.path.join(base, tipo, "XML")
    pdir = os.path.join(base, tipo, "PDF")
    csv_ = os.path.join(base, tipo, "log_notas.csv")

    r = _filtrar(driver, di, df)
    if r == "SEM":
        log_info(f"⊘ {tipo}: sem registros {di}–{df}")
        return "SEM"

    logs = []
    pg = 1
    total = 0

    while True:
        log_info(f"━━━ {tipo} página {pg} ━━━")
        st = _aguardar(driver, wait, tipo)
        if st == "SEM":
            if pg == 1:
                return "SEM"
            break
        if not st:
            if pg == 1:
                return "SEM"
            break

        linhas = _valid_rows(driver)
        if not linhas:
            if pg == 1:
                return "SEM"
            if not _next_pg(driver, tipo):
                break
            pg += 1
            continue

        n = len(linhas)
        log_info(f"  {n} notas encontradas")
        total += n

        session = _criar_sessao(driver)
        futures = []

        with ThreadPoolExecutor(max_workers=DL_WORKERS) as pool:
            for idx, row in enumerate(linhas, 1):
                time.sleep(CLICK_DELAY)
                info = _coletar_links(driver, row, pg, idx, tipo, xdir, pdir)
                if not info:
                    continue
                if info["xh"] and info["xp"]:
                    futures.append(pool.submit(_baixar, session, info["xh"], info["xp"]))
                if info["ph"] and info["pp"]:
                    futures.append(pool.submit(_baixar, session, info["ph"], info["pp"]))
                logs.append(info["log"])
                if on_progress:
                    on_progress(tipo, pg, idx, n, total)

            for f in as_completed(futures):
                try:
                    f.result(timeout=30)
                except Exception:
                    pass

        session.close()

        if not _next_pg(driver, tipo):
            break
        pg += 1

    if logs:
        pd.DataFrame(logs).to_csv(csv_, index=False, encoding="utf-8-sig")
        log_info(f"✓ {tipo}: {total} notas, {pg} pág.")
    return "OK"


# ═══════════════════════════════════════════════════════
# HELPERS DE VALOR
# ═══════════════════════════════════════════════════════
def _fv(v):
    if not v or not str(v).strip():
        return "0"
    try:
        return f"{float(str(v).strip()):.2f}".replace(".", ",")
    except Exception:
        return str(v).strip() or "0"


def _fl(v):
    try:
        return float(str(v).strip()) if v else 0.0
    except Exception:
        return 0.0


def _vz(v):
    return str(v).strip() if v and str(v).strip() else "0"


def _comp(data):
    if not data:
        return "0"
    m = RE_ISO.match(str(data).strip())
    if m:
        return f"{m.group(2)}/{m.group(1)}"
    m = RE_BR.match(str(data).strip())
    if m:
        return f"{m.group(2)}/{m.group(3)}"
    return "0"


# ═══════════════════════════════════════════════════════
# EXTRAÇÃO XML
# ═══════════════════════════════════════════════════════
def extrair_xml(path, sit, pg, ln, tipo):
    try:
        tree = ET.parse(path)
        root = tree.getroot()

        for el in root.iter():
            if "}" in el.tag:
                el.tag = el.tag.split("}", 1)[1]
            el.attrib = {
                (k.split("}")[-1] if "}" in k else k): v
                for k, v in el.attrib.items()
            }

        def t(*tags):
            for tg in tags:
                e = root.find(f".//{tg}")
                if e is not None and e.text and e.text.strip():
                    return e.text.strip()
            return ""

        # ── Identificação ──
        chave = ""
        inf = root.find(".//infNFSe")
        if inf is not None:
            chave = inf.get("Id", "")
            if chave.startswith("NFS"):
                chave = chave[3:]

        num = t("nNFSe", "Numero", "nDFSe")
        dh  = t("infDPS/dhEmi", "dhEmi", "DataEmissao", "dhProc")
        dcp = t("infDPS/dCompet", "dCompet")
        if dcp:
            m = RE_ISO.match(dcp)
            comp = f"{m.group(2)}/{m.group(1)}" if m else _comp(dh)
        else:
            comp = _comp(dh)

        # ── Prestador / Tomador ──
        cp  = t("prest/CNPJ",  "emit/CNPJ")
        rp  = t("prest/xNome", "emit/xNome")
        ct  = t("toma/CNPJ",   "tomador/CNPJ")
        cpt = t("toma/CPF",    "tomador/CPF")
        rt  = t("toma/xNome",  "tomador/xNome")

        # ── Serviço ──
        ctrib = t("cServ/cTribNac", "cTribNac")
        desc  = t("cServ/xDescServ", "xDescServ", "Discriminacao")
        local = t("xLocPrestacao", "xLocIncid", "cLocPrestacao")

        # ── Valores ──
        vs   = t("vServPrest/vServ", "vServ")
        vded = t("vDedRed", "vDeducao")
        vdi  = t("vDescIncond")
        vdc  = t("vDescCond")
        vbc  = t("valores/vBC", "vBC")
        aiss = t("valores/pAliqAplic", "pAliqAplic", "BM/pAliq", "tribMun/pAliq")
        viss = t("valores/vISSQN", "vISSQN", "BM/vISS", "vISS")

        # ═══ ISS RETENÇÃO ═══
        tp_iss   = t("tpRetISSQN", "BM/tpRetISSQN", "tribMun/tpRetISSQN")
        desc_iss = DESC_RET_ISSQN.get(tp_iss, "Não Retido")
        iss_ret  = viss if tp_iss in ("2", "3") else ""

        # ═══ PIS / COFINS ═══
        cst_pc   = t("piscofins/CST", "CST")
        desc_cst = DESC_CST_PISCOFINS.get(cst_pc, "")
        bpc      = t("piscofins/vBCPisCofins", "vBCPisCofins")
        ap       = t("piscofins/pAliqPis",     "pAliqPis")
        ac       = t("piscofins/pAliqCofins",   "pAliqCofins")
        vpis     = t("piscofins/vPis",   "vPis")
        vcof     = t("piscofins/vCofins", "vCofins")
        tp_pc    = t("piscofins/tpRetPisCofins", "tpRetPisCofins")
        desc_pc  = DESC_RET_PISCOFINS.get(tp_pc, "Não Retido")

        pis_ret = vpis if tp_pc == "1" else ""
        cof_ret = vcof if tp_pc == "1" else ""

        # ═══════════════════════════════════════════════
        # DEMAIS RETENÇÕES — CP + INSS = coluna INSS
        # FIX V3.1.2: tag correta é vRetCP (não vRetCPP)
        # ═══════════════════════════════════════════════
        ir   = t("tribFed/vRetIRRF", "vRetIRRF")
        csll = t("tribFed/vRetCSLL", "vRetCSLL")

        v_inss = _fl(t("tribFed/vRetINSS", "vRetINSS"))
        v_cp   = _fl(t("tribFed/vRetCP",   "vRetCP"))   # ← CORRIGIDO
        inss_total = v_inss + v_cp
        inss_ret = str(inss_total) if inss_total > 0 else ""

        outr = t("vOutrasRet", "OutrasRetencoes")

        total_ret = sum(
            _fl(v) for v in (iss_ret, pis_ret, cof_ret, ir, csll, inss_ret, outr)
        )

        vliq = t("valores/vLiq", "vLiq", "ValorLiquidoNfse")

        return {
            "Página": pg, "Linha": ln,
            "Nº NFSe": _vz(num), "Chave": _vz(chave),
            "Competência": comp, "Data Emissão": _vz(dh),
            "CNPJ Prestador": _vz(cp), "Razão Social Prestador": _vz(rp),
            "CNPJ Tomador": _vz(ct), "CPF Tomador": _vz(cpt),
            "Razão Social Tomador": _vz(rt),
            "Código Tributação Nacional": _vz(ctrib),
            "Descrição Serviço": _vz(desc), "Local da Prestação": _vz(local),
            "Valor dos Serviços": _fv(vs), "Valor Deduções": _fv(vded),
            "Desconto Incondicionado": _fv(vdi), "Desconto Condicionado": _fv(vdc),
            "Base de Cálculo": _fv(vbc), "Alíquota ISS": _fv(aiss), "Valor ISS": _fv(viss),
            "tpRetISSQN": _vz(tp_iss) or "1", "Desc. Ret. ISSQN": desc_iss,
            "ISS Retido": _fv(iss_ret),
            "CST PIS/COFINS": _vz(cst_pc),
            "Desc. CST PIS/COFINS": desc_cst if desc_cst else "0",
            "Base PIS/COFINS": _fv(bpc),
            "Alíq PIS": _fv(ap), "Alíq COFINS": _fv(ac),
            "Valor PIS": _fv(vpis), "Valor COFINS": _fv(vcof),
            "tpRetPisCofins": _vz(tp_pc) or "2",
            "Desc. Ret. PIS/COFINS": desc_pc,
            "PIS Retido": _fv(pis_ret), "COFINS Retido": _fv(cof_ret),
            "IR Retido": _fv(ir), "CSLL Retido": _fv(csll),
            "INSS Retido": _fv(inss_ret),
            "Outras Retenções": _fv(outr),
            "Total Retenções": _fv(str(total_ret)) if total_ret else "0",
            "Valor Líquido": _fv(vliq),
            "Situação": sit, "Tipo": tipo,
        }
    except Exception as e:
        logging.error(f"XML {path}: {e}")
    return None


# ═══════════════════════════════════════════════════════
# RELATÓRIO EXCEL
# ═══════════════════════════════════════════════════════
def gerar_excel(base, mostrar=True):
    out = os.path.join(base, "Relatorio_NFSe.xlsx")
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as wr:
            achou = False
            tr = te = 0
            for tipo in ("Recebidas", "Emitidas"):
                xdir = os.path.join(base, tipo, "XML")
                csv_ = os.path.join(base, tipo, "log_notas.csv")
                if not os.path.isfile(csv_):
                    continue
                try:
                    lg = pd.read_csv(csv_)
                except Exception:
                    continue
                rows, vistos = [], set()
                for _, r in lg.iterrows():
                    xf = str(r.get("XML", "")).strip()
                    if not xf or pd.isna(r.get("XML")) or xf in vistos:
                        continue
                    vistos.add(xf)
                    fp = os.path.join(xdir, xf)
                    if not os.path.isfile(fp):
                        continue
                    s = str(r.get("SITUACAO", "Emitida"))
                    if pd.isna(r.get("SITUACAO")):
                        s = "Emitida"
                    d = extrair_xml(
                        fp, s, r.get("PAGINA", 0),
                        r.get("LINHA", 0), r.get("TIPO", tipo),
                    )
                    if d:
                        rows.append(d)
                if rows:
                    df = pd.DataFrame(rows).fillna("0")
                    cols = [c for c in COLUNAS if c in df.columns]
                    df = df[cols + [c for c in df.columns if c not in COLUNAS]]
                    df.to_excel(wr, sheet_name=tipo, index=False)
                    achou = True
                    if tipo == "Recebidas":
                        tr = len(rows)
                    else:
                        te = len(rows)
            if not achou:
                pd.DataFrame({"Info": ["Nenhum XML"]}).to_excel(
                    wr, sheet_name="Info", index=False
                )
        log_info(f"✓ Relatório: {out}")
        if mostrar:
            messagebox.showinfo(
                "Relatório",
                f"✓ Gerado!\n\n📥 {tr} recebidas\n📤 {te} emitidas\n\n{out}",
            )
        return out
    except Exception as e:
        logging.error(f"Excel: {e}")
        if mostrar:
            messagebox.showerror("Erro", str(e))
        raise


# ═══════════════════════════════════════════════════════
# ROBÔ PRINCIPAL
# ═══════════════════════════════════════════════════════
def run_download(base, di, df, opt, logf, auto_rel, on_progress=None, on_done=None):
    if not base:
        messagebox.showerror("Erro", "Selecione pasta!")
        return
    if not di or not df or "AAAA" in di or "AAAA" in df:
        messagebox.showerror("Erro", "Preencha as datas!")
        return
    if not RE_DATE.match(di) or not RE_DATE.match(df):
        messagebox.showerror("Erro", "Use DD/MM/AAAA!")
        return

    configure_logger(logf, os.path.join(base, "automat.log"))
    ensure_dirs(base)

    driver = None
    res = {}
    t0 = time.perf_counter()
    try:
        driver, wait = start_browser()

        if opt in ("RECEBIDAS", "AMBAS"):
            log_info("═══ RECEBIDAS ═══")
            try:
                driver.execute_script(
                    """
                    var m=document.querySelector("img[src*='menu-recebidas']");
                    if(m) m.closest('a').click();
                    else {
                        var a=document.querySelector("a[href*='Recebidas']");
                        if(a) a.click();
                    }
                    """
                )
                time.sleep(1)
                try:
                    sub = driver.find_element(
                        By.CSS_SELECTOR, "a[href*='/NFSe/Recebidas']"
                    )
                    driver.execute_script("arguments[0].click();", sub)
                except Exception:
                    pass
            except Exception:
                pass
            time.sleep(1)
            res["rec"] = processar_tabela(
                driver, wait, "Recebidas", base, di, df, on_progress
            )

        if opt in ("EMITIDAS", "AMBAS"):
            log_info("═══ EMITIDAS ═══")
            driver.get("https://www.nfse.gov.br/EmissorNacional/Notas/Emitidas")
            time.sleep(1)
            res["emi"] = processar_tabela(
                driver, wait, "Emitidas", base, di, df, on_progress
            )

        elapsed = time.perf_counter() - t0
        msg = f"Concluído em {elapsed:.1f}s\n\n"
        if opt in ("RECEBIDAS", "AMBAS"):
            msg += "📥 Recebidas: " + (
                "sem registros" if res.get("rec") == "SEM" else "OK"
            ) + "\n"
        if opt in ("EMITIDAS", "AMBAS"):
            msg += "📤 Emitidas: " + (
                "sem registros" if res.get("emi") == "SEM" else "OK"
            ) + "\n"

        if auto_rel and any(v == "OK" for v in res.values()):
            try:
                gerar_excel(base, mostrar=False)
                msg += "\n📊 Relatório gerado"
            except Exception:
                pass
        elif auto_rel:
            msg += "\n📊 Sem dados para relatório"

        messagebox.showinfo("Concluído", msg)

    except Exception as e:
        logging.error(f"Erro: {e}")
        messagebox.showerror("Erro", str(e))
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        if on_done:
            on_done()


def run_relatorio(base):
    if not base:
        messagebox.showerror("Erro", "Selecione pasta!")
        return
    rx = os.path.join(base, "Recebidas", "XML")
    ex = os.path.join(base, "Emitidas", "XML")
    if not (
        (os.path.isdir(rx) and os.listdir(rx))
        or (os.path.isdir(ex) and os.listdir(ex))
    ):
        messagebox.showwarning("Aviso", "Nenhum XML. Baixe primeiro.")
        return
    configure_logger(True, os.path.join(base, "automat.log"))
    gerar_excel(base, mostrar=True)


# ═══════════════════════════════════════════════════════
# INTERFACE MODERNA
# ═══════════════════════════════════════════════════════
class ModernApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("NFS-e Portal Nacional")
        self.geometry("640x780")
        self.resizable(False, False)
        self.configure(bg=Theme.BG)
        self._center()
        self._build_ui()
        self._running = False

    def _center(self):
        self.update_idletasks()
        w, h = 640, 780
        x = (self.winfo_screenwidth() - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _card(self, parent, **kw):
        return tk.Frame(
            parent, bg=Theme.CARD,
            highlightbackground=Theme.BORDER,
            highlightthickness=1, **kw,
        )

    def _label(self, parent, text, size=10, bold=False, color=None, **kw):
        return tk.Label(
            parent, text=text, bg=parent["bg"],
            fg=color or Theme.FG,
            font=(Theme.FONT, size, "bold" if bold else "normal"), **kw,
        )

    def _entry(self, parent, width=30, placeholder=""):
        e = tk.Entry(
            parent, width=width, bg=Theme.ENTRY_BG, fg=Theme.FG,
            insertbackground=Theme.ACCENT, font=(Theme.FONT, 10),
            relief="flat", highlightbackground=Theme.ENTRY_BD,
            highlightcolor=Theme.ACCENT, highlightthickness=1,
            selectbackground=Theme.ACCENT, selectforeground=Theme.BG,
        )
        if placeholder:
            e.insert(0, placeholder)
            e.bind("<FocusIn>", lambda ev: e.delete(0, tk.END) if e.get() == placeholder else None)
            e.bind("<FocusOut>", lambda ev: e.insert(0, placeholder) if not e.get() else None)
        return e

    def _btn(self, parent, text, command, color=None, fg="white", width=20):
        bg = color or Theme.BTN_BG
        b = tk.Button(
            parent, text=text, command=command,
            bg=bg, fg=fg, activebackground=bg, activeforeground=fg,
            font=(Theme.FONT, 10, "bold"), relief="flat",
            cursor="hand2", width=width, padx=12, pady=8,
        )
        hover = {Theme.BTN_BG: Theme.BTN_HOVER, Theme.BTN_BG2: Theme.BTN_HOVER2}.get(bg, Theme.ACCENT)
        b.bind("<Enter>", lambda e: b.configure(bg=hover))
        b.bind("<Leave>", lambda e: b.configure(bg=bg))
        return b

    def _radio(self, parent, text, variable, value):
        return tk.Radiobutton(
            parent, text=text, variable=variable, value=value,
            bg=parent["bg"], fg=Theme.FG, selectcolor=Theme.BG2,
            activebackground=parent["bg"], activeforeground=Theme.ACCENT,
            font=(Theme.FONT, 10), cursor="hand2", highlightthickness=0,
        )

    def _check(self, parent, text, variable):
        return tk.Checkbutton(
            parent, text=text, variable=variable,
            bg=parent["bg"], fg=Theme.FG, selectcolor=Theme.BG2,
            activebackground=parent["bg"], activeforeground=Theme.ACCENT,
            font=(Theme.FONT, 9), cursor="hand2", highlightthickness=0,
        )

    def _build_ui(self):
        # HEADER
        header = tk.Frame(self, bg=Theme.BG2, height=70)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        hi = tk.Frame(header, bg=Theme.BG2)
        hi.pack(expand=True)
        tk.Label(hi, text="⚡", bg=Theme.BG2, fg=Theme.ACCENT,
                 font=(Theme.FONT, 22)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Label(hi, text="Portal Nacional ", bg=Theme.BG2, fg=Theme.FG,
                 font=(Theme.FONT, 18, "bold")).pack(side=tk.LEFT)
        tk.Label(hi, text=" Premier", bg=Theme.BG2, fg=Theme.ACCENT,
                 font=(Theme.FONT, 10, "bold")).pack(side=tk.LEFT, pady=(6, 0))
        tk.Frame(self, bg=Theme.ACCENT, height=2).pack(fill=tk.X)

        ct = tk.Frame(self, bg=Theme.BG, padx=24, pady=16)
        ct.pack(fill=tk.BOTH, expand=True)

        # PASTA
        c1 = self._card(ct); c1.pack(fill=tk.X, pady=(0, 10))
        i1 = tk.Frame(c1, bg=Theme.CARD, padx=16, pady=12); i1.pack(fill=tk.X)
        self._label(i1, "📁  Pasta de destino", 10, True, Theme.ACCENT).pack(anchor=tk.W)
        rf = tk.Frame(i1, bg=Theme.CARD); rf.pack(fill=tk.X, pady=(8, 0))
        self.folder_entry = self._entry(rf, 48)
        self.folder_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._btn(rf, "Selecionar", self._sel_folder, Theme.BG3, Theme.ACCENT, 10).pack(side=tk.LEFT, padx=(8, 0))

        # DATAS
        c2 = self._card(ct); c2.pack(fill=tk.X, pady=(0, 10))
        i2 = tk.Frame(c2, bg=Theme.CARD, padx=16, pady=12); i2.pack(fill=tk.X)
        self._label(i2, "📅  Período", 10, True, Theme.ACCENT).pack(anchor=tk.W)
        rd = tk.Frame(i2, bg=Theme.CARD); rd.pack(fill=tk.X, pady=(8, 0))

        ld = tk.Frame(rd, bg=Theme.CARD); ld.pack(side=tk.LEFT, expand=True, fill=tk.X)
        self._label(ld, "Início", 9, color=Theme.FG2).pack(anchor=tk.W)
        self.dt_ini = self._entry(ld, 14, "DD/MM/AAAA")
        self.dt_ini.pack(anchor=tk.W, pady=(2, 0))

        lf2 = tk.Frame(rd, bg=Theme.CARD); lf2.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(20, 0))
        self._label(lf2, "Fim", 9, color=Theme.FG2).pack(anchor=tk.W)
        self.dt_fim = self._entry(lf2, 14, "DD/MM/AAAA")
        self.dt_fim.pack(anchor=tk.W, pady=(2, 0))

        # CONFIG
        c3 = self._card(ct); c3.pack(fill=tk.X, pady=(0, 10))
        i3 = tk.Frame(c3, bg=Theme.CARD, padx=16, pady=12); i3.pack(fill=tk.X)
        self._label(i3, "📋  Configurações", 10, True, Theme.ACCENT).pack(anchor=tk.W)

        rt = tk.Frame(i3, bg=Theme.CARD); rt.pack(fill=tk.X, pady=(8, 0))
        self._label(rt, "Tipo:", 9, color=Theme.FG2).pack(side=tk.LEFT, padx=(0, 10))
        self.opt_var = tk.StringVar(value="AMBAS")
        self._radio(rt, "📥 Recebidas", self.opt_var, "RECEBIDAS").pack(side=tk.LEFT, padx=(0, 12))
        self._radio(rt, "📤 Emitidas", self.opt_var, "EMITIDAS").pack(side=tk.LEFT, padx=(0, 12))
        self._radio(rt, "📦 Ambas", self.opt_var, "AMBAS").pack(side=tk.LEFT)

        ro = tk.Frame(i3, bg=Theme.CARD); ro.pack(fill=tk.X, pady=(8, 0))
        self.log_var = tk.BooleanVar(value=True)
        self._check(ro, "💾 Salvar log", self.log_var).pack(side=tk.LEFT, padx=(0, 20))
        self.auto_var = tk.BooleanVar(value=True)
        self._check(ro, "📊 Relatório automático", self.auto_var).pack(side=tk.LEFT)

        # BOTÕES
        rb = tk.Frame(ct, bg=Theme.BG); rb.pack(fill=tk.X, pady=(6, 10))
        self.btn_dl = self._btn(rb, "▶  BAIXAR ARQUIVOS", self._run, Theme.BTN_BG, "white", 22)
        self.btn_dl.pack(side=tk.LEFT, padx=(0, 10))
        self.btn_rp = self._btn(rb, "📊  GERAR RELATÓRIO", self._report, Theme.BTN_BG2, "white", 22)
        self.btn_rp.pack(side=tk.LEFT)

        # STATUS
        c4 = self._card(ct); c4.pack(fill=tk.X, pady=(0, 8))
        i4 = tk.Frame(c4, bg=Theme.CARD, padx=16, pady=10); i4.pack(fill=tk.X)
        self._label(i4, "⚡  Status", 10, True, Theme.ACCENT).pack(anchor=tk.W)
        self.progress = ttk.Progressbar(i4, mode="indeterminate", length=400)
        self.progress.pack(fill=tk.X, pady=(8, 4))
        self.status_lbl = self._label(i4, "Pronto", 9, color=Theme.FG2)
        self.status_lbl.pack(anchor=tk.W)

        # LOG
        c5 = self._card(ct); c5.pack(fill=tk.BOTH, expand=True, pady=(0, 6))
        i5 = tk.Frame(c5, bg=Theme.CARD, padx=12, pady=8); i5.pack(fill=tk.BOTH, expand=True)
        self._label(i5, "📜  Log", 9, True, Theme.FG2).pack(anchor=tk.W)
        self.log_text = tk.Text(
            i5, height=6, bg=Theme.BG, fg=Theme.GREEN,
            insertbackground=Theme.GREEN, font=(Theme.FONT_MONO, 8),
            relief="flat", highlightthickness=0, wrap=tk.WORD,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(4, 0))
        self.log_text.configure(state=tk.DISABLED)

        tk.Label(ct, text="© by Matheus Tecnologia  •  V2.0.1",
                 bg=Theme.BG, fg=Theme.FG2, font=(Theme.FONT, 8, "italic")).pack(side=tk.BOTTOM)

        global _log_callback
        _log_callback = self._append_log

    def _sel_folder(self):
        d = filedialog.askdirectory()
        if d:
            self.folder_entry.delete(0, tk.END)
            self.folder_entry.insert(0, d)

    def _set_status(self, msg):
        self.status_lbl.configure(text=msg)

    def _append_log(self, msg):
        try:
            self.log_text.configure(state=tk.NORMAL)
            self.log_text.insert(tk.END, msg + "\n")
            self.log_text.see(tk.END)
            self.log_text.configure(state=tk.DISABLED)
        except Exception:
            pass

    def _set_running(self, state):
        self._running = state
        st = "disabled" if state else "normal"
        self.btn_dl.configure(state=st)
        self.btn_rp.configure(state=st)
        if state:
            self.progress.start(12)
        else:
            self.progress.stop()

    def _on_progress(self, tipo, pg, idx, n_pg, n_total):
        self._set_status(f"{tipo}  •  Pág {pg}  •  Nota {idx}/{n_pg}  •  Total: {n_total}")

    def _on_done(self):
        self._set_running(False)
        self._set_status("Concluído ✓")

    def _run(self):
        if self._running:
            return
        self._set_running(True)
        self._set_status("Iniciando...")
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)
        threading.Thread(
            target=run_download,
            args=(
                self.folder_entry.get().strip(),
                self.dt_ini.get().strip(),
                self.dt_fim.get().strip(),
                self.opt_var.get(),
                self.log_var.get(),
                self.auto_var.get(),
                self._on_progress,
                self._on_done,
            ),
            daemon=True,
        ).start()

    def _report(self):
        if self._running:
            return
        self._set_running(True)
        self._set_status("Gerando relatório...")
        threading.Thread(
            target=lambda: (
                run_relatorio(self.folder_entry.get().strip()),
                self._on_done(),
            ),
            daemon=True,
        ).start()


def main():
    app = ModernApp()
    app.mainloop()


if __name__ == "__main__":
    main()