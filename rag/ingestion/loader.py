"""
BetterCallSpaceLLM — Phase 1 : Data Pipeline
loader.py — Collecte et téléchargement des documents juridiques spatiaux

Sources couvertes :
  - UNOOSA  : 5 traités ONU + résolutions AGNU (soft law)        [EN/FR/RU/ZH/ES]
  - NASA    : Artemis Accords 2020, ISS IGA 1998                  [EN]
  - ITU     : Règlement des radiocommunications (services spatiaux)[EN/FR]
  - ASTRO   : Lois nationales (France, Canada, Luxembourg, Algérie)[EN/FR/AR]
  - ESA     : Convention ESA 1975                                  [EN/FR]
  - Divers  : Déclaration de Bogotá 1976, LTS Guidelines 2019      [EN/FR]

Catégories :
  treaty | convention | declaration | resolution | guideline
  national_law | regulation | agreement | soft_law

Usage :
  python loader.py                         # télécharge tout
  python loader.py --lang en fr            # langues spécifiques
  python loader.py --category treaty       # catégorie spécifique
  python loader.py --source UNOOSA NASA    # sources spécifiques
  python loader.py --dry-run               # liste les URLs sans télécharger
"""

import os
import json
import time
import hashlib
import argparse
import logging
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR  = DATA_DIR / "raw"
SOURCES_FILE = DATA_DIR / "sources.json"

HEADERS = {
    "User-Agent": "BetterCallSpaceLLM-Research-Bot/1.0 (space-law AI project; educational use)"
}

DELAY_BETWEEN_REQUESTS = 1.5  # secondes (respecter les serveurs)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("BetterCallSpaceLLM.loader")

# ─────────────────────────────────────────────
# Catalogue des documents
# ─────────────────────────────────────────────

DOCUMENTS = [

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 1 — 5 TRAITÉS ONU (corpus fondateur)
    # Source : UNOOSA — multilingues EN/FR/RU/ZH/ES
    # ════════════════════════════════════════════════════════════════════════

    # ── Outer Space Treaty 1967 ──────────────────────────────────────────────
    {
        "id": "OST_1967_EN",
        "title": "Treaty on Principles Governing the Activities of States in the Exploration and Use of Outer Space (Outer Space Treaty)",
        "year": 1967, "lang": "en", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "OST_1967_EN.pdf"
    },
    {
        "id": "OST_1967_FR",
        "title": "Traité sur les principes régissant les activités des États en matière d'exploration et d'utilisation de l'espace extra-atmosphérique",
        "year": 1967, "lang": "fr", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11F.pdf",
        "filename": "OST_1967_FR.pdf"
    },
    {
        "id": "OST_1967_RU",
        "title": "Договор о принципах деятельности государств по исследованию и использованию космического пространства",
        "year": 1967, "lang": "ru", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11R.pdf",
        "filename": "OST_1967_RU.pdf"
    },
    {
        "id": "OST_1967_ZH",
        "title": "关于各国探索和利用包括月球和其他天体在内外层空间活动的原则条约",
        "year": 1967, "lang": "zh", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11C.pdf",
        "filename": "OST_1967_ZH.pdf"
    },
    {
        "id": "OST_1967_ES",
        "title": "Tratado sobre los principios que deben regir las actividades de los Estados en la exploración y utilización del espacio ultraterrestre",
        "year": 1967, "lang": "es", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11S.pdf",
        "filename": "OST_1967_ES.pdf"
    },

    # ── Rescue Agreement 1968 ────────────────────────────────────────────────
    {
        "id": "RESCUE_1968_EN",
        "title": "Agreement on the Rescue of Astronauts, the Return of Astronauts and the Return of Objects Launched into Outer Space",
        "year": 1968, "lang": "en", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "RESCUE_1968_EN.pdf"
    },
    {
        "id": "RESCUE_1968_FR",
        "title": "Accord sur le sauvetage des astronautes, le retour des astronautes et la restitution des objets lancés dans l'espace extra-atmosphérique",
        "year": 1968, "lang": "fr", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11F.pdf",
        "filename": "RESCUE_1968_FR.pdf"
    },

    # ── Liability Convention 1972 ────────────────────────────────────────────
    {
        "id": "LIABILITY_1972_EN",
        "title": "Convention on International Liability for Damage Caused by Space Objects",
        "year": 1972, "lang": "en", "category": "convention", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "LIABILITY_1972_EN.pdf"
    },
    {
        "id": "LIABILITY_1972_FR",
        "title": "Convention sur la responsabilité internationale pour les dommages causés par des objets spatiaux",
        "year": 1972, "lang": "fr", "category": "convention", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11F.pdf",
        "filename": "LIABILITY_1972_FR.pdf"
    },
    {
        "id": "LIABILITY_1972_RU",
        "title": "Конвенция о международной ответственности за ущерб, причинённый космическими объектами",
        "year": 1972, "lang": "ru", "category": "convention", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11R.pdf",
        "filename": "LIABILITY_1972_RU.pdf"
    },

    # ── Registration Convention 1976 ─────────────────────────────────────────
    {
        "id": "REG_1976_EN",
        "title": "Convention on Registration of Objects Launched into Outer Space",
        "year": 1976, "lang": "en", "category": "convention", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "REG_1976_EN.pdf"
    },
    {
        "id": "REG_1976_FR",
        "title": "Convention sur l'immatriculation des objets lancés dans l'espace extra-atmosphérique",
        "year": 1976, "lang": "fr", "category": "convention", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11F.pdf",
        "filename": "REG_1976_FR.pdf"
    },

    # ── Moon Agreement 1979 ──────────────────────────────────────────────────
    {
        "id": "MOON_1979_EN",
        "title": "Agreement Governing the Activities of States on the Moon and Other Celestial Bodies (Moon Agreement)",
        "year": 1979, "lang": "en", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "MOON_1979_EN.pdf"
    },
    {
        "id": "MOON_1979_FR",
        "title": "Accord régissant les activités des États sur la Lune et les autres corps célestes",
        "year": 1979, "lang": "fr", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11F.pdf",
        "filename": "MOON_1979_FR.pdf"
    },
    {
        "id": "MOON_1979_ZH",
        "title": "关于各国在月球和其他天体上活动的协定",
        "year": 1979, "lang": "zh", "category": "treaty", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11C.pdf",
        "filename": "MOON_1979_ZH.pdf"
    },

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 2 — RÉSOLUTIONS AGNU (soft law — 5 principes clés)
    # Source : UNOOSA
    # ════════════════════════════════════════════════════════════════════════

    # ── Déclaration des principes juridiques 1963 ────────────────────────────
    {
        "id": "UNGA_1962_DECL_EN",
        "title": "Declaration of Legal Principles Governing the Activities of States in the Exploration and Use of Outer Space — UNGA Res. 1962 (XVIII)",
        "year": 1963, "lang": "en", "category": "declaration", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "UNGA_1962_DECL_EN.pdf"
    },
    # ── Principes sur la télédiffusion directe 1982 ──────────────────────────
    {
        "id": "UNGA_BROADCAST_1982_EN",
        "title": "Principles Governing the Use by States of Artificial Earth Satellites for International Direct Television Broadcasting — UNGA Res. 37/92",
        "year": 1982, "lang": "en", "category": "resolution", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "UNGA_BROADCAST_1982_EN.pdf"
    },
    # ── Principes sur la télédétection 1986 ─────────────────────────────────
    {
        "id": "UNGA_REMOTE_SENSING_1986_EN",
        "title": "Principles Relating to Remote Sensing of the Earth from Outer Space — UNGA Res. 41/65",
        "year": 1986, "lang": "en", "category": "resolution", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "UNGA_REMOTE_SENSING_1986_EN.pdf"
    },
    # ── Principes sur les sources d'énergie nucléaire 1992 ───────────────────
    {
        "id": "UNGA_NPS_1992_EN",
        "title": "Principles Relevant to the Use of Nuclear Power Sources in Outer Space — UNGA Res. 47/68",
        "year": 1992, "lang": "en", "category": "resolution", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "UNGA_NPS_1992_EN.pdf"
    },
    # ── Déclaration sur la coopération internationale 1996 ───────────────────
    {
        "id": "UNGA_COOP_1996_EN",
        "title": "Declaration on International Cooperation in the Exploration and Use of Outer Space for the Benefit of All States — UNGA Res. 51/122",
        "year": 1996, "lang": "en", "category": "declaration", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/STSPACE11E.pdf",
        "filename": "UNGA_COOP_1996_EN.pdf"
    },

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 3 — DOCUMENTS MODERNES & ÉMERGENTS
    # ════════════════════════════════════════════════════════════════════════

    # ── Artemis Accords 2020 (NASA) ──────────────────────────────────────────
    {
        "id": "ARTEMIS_2020_EN",
        "title": "Artemis Accords — Principles for Cooperation in the Civil Exploration and Use of the Moon, Mars, Comets, and Asteroids for Peaceful Purposes",
        "year": 2020, "lang": "en", "category": "agreement", "source": "NASA",
        "url": "https://www.nasa.gov/wp-content/uploads/2022/11/Artemis-Accords-signed-13Oct2020.pdf",
        "filename": "ARTEMIS_2020_EN.pdf"
    },

    # ── COPUOS LTS Guidelines 2019 ───────────────────────────────────────────
    {
        "id": "LTS_GUIDELINES_2019_EN",
        "title": "Guidelines for the Long-term Sustainability of Outer Space Activities — COPUOS 2019",
        "year": 2019, "lang": "en", "category": "guideline", "source": "UNOOSA",
        "url": "https://www.unoosa.org/res/oosadoc/data/documents/2019/aac_1052019crp/aac_1052019crp_20rev1_0_html/AC105_2019_CRP20Rev1E.pdf",
        "filename": "LTS_GUIDELINES_2019_EN.pdf"
    },
    {
        "id": "LTS_GUIDELINES_2019_FR",
        "title": "Directives relatives à la viabilité à long terme des activités spatiales — COPUOS 2019",
        "year": 2019, "lang": "fr", "category": "guideline", "source": "UNOOSA",
        "url": "https://www.unoosa.org/res/oosadoc/data/documents/2019/aac_1052019crp/aac_1052019crp_20rev1_0_html/AC105_2019_CRP20Rev1F.pdf",
        "filename": "LTS_GUIDELINES_2019_FR.pdf"
    },

    # ── ISS Intergovernmental Agreement 1998 ─────────────────────────────────
    {
        "id": "ISS_IGA_1998_EN",
        "title": "Agreement among the Government of Canada, Governments of Member States of ESA, the Government of Japan, the Government of the Russian Federation, and the Government of the United States of America concerning Cooperation on the Civil International Space Station",
        "year": 1998, "lang": "en", "category": "agreement", "source": "NASA",
        "url": "https://www.nasa.gov/wp-content/uploads/2023/05/iga.pdf",
        "filename": "ISS_IGA_1998_EN.pdf"
    },

    # ── UNISPACE+50 Space2030 Agenda 2018 ───────────────────────────────────
    {
        "id": "UNISPACE50_2018_EN",
        "title": "UNISPACE+50 'Space2030' Agenda — High-level Declaration",
        "year": 2018, "lang": "en", "category": "declaration", "source": "UNOOSA",
        "url": "https://www.unoosa.org/res/oosadoc/data/documents/2018/aac_1052018crp/aac_1052018crp_20_0_html/AC105_2018_CRP20E.pdf",
        "filename": "UNISPACE50_2018_EN.pdf"
    },

    # ── Déclaration de Bogotá 1976 ───────────────────────────────────────────
    {
        "id": "BOGOTA_DECL_1976_EN",
        "title": "Declaration of the First Meeting of Equatorial Countries (Bogotá Declaration) — Claim over Geostationary Orbit",
        "year": 1976, "lang": "en", "category": "declaration", "source": "ITU",
        "url": "https://www.jaxa.jp/library/space_law/chapter_2/2-2-1-2_e.html",
        "filename": "BOGOTA_DECL_1976_EN.pdf",
        "note": "Document HTML — conversion manuelle ou scraping nécessaire"
    },

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 4 — CONVENTION ESA & DROIT EUROPÉEN
    # ════════════════════════════════════════════════════════════════════════

    # ── Convention ESA 1975 ──────────────────────────────────────────────────
    {
        "id": "ESA_CONV_1975_EN",
        "title": "Convention for the Establishment of a European Space Agency (ESA Convention)",
        "year": 1975, "lang": "en", "category": "convention", "source": "ESA",
        "url": "https://esamultimedia.esa.int/docs/LEX-F/ESA-Convention.pdf",
        "filename": "ESA_CONV_1975_EN.pdf"
    },
    {
        "id": "ESA_CONV_1975_FR",
        "title": "Convention portant création d'une Agence Spatiale Européenne",
        "year": 1975, "lang": "fr", "category": "convention", "source": "ESA",
        "url": "https://esamultimedia.esa.int/docs/LEX-F/ESA-Convention-F.pdf",
        "filename": "ESA_CONV_1975_FR.pdf"
    },

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 5 — LOIS NATIONALES (ASTRO / UNOOSA)
    # ════════════════════════════════════════════════════════════════════════

    # ── France — Loi relative aux opérations spatiales 2008 ──────────────────
    {
        "id": "FRANCE_LOS_2008_FR",
        "title": "Loi n° 2008-518 du 3 juin 2008 relative aux opérations spatiales",
        "year": 2008, "lang": "fr", "category": "national_law", "source": "ASTRO",
        "url": "https://www.unoosa.org/documents/pdf/spacelaw/national/france/2008-518F.pdf",
        "filename": "FRANCE_LOS_2008_FR.pdf"
    },

    # ── Luxembourg — Space Resources Law 2017 ────────────────────────────────
    {
        "id": "LUX_SPACE_2017_EN",
        "title": "Law of 20 July 2017 on the Exploration and Use of Space Resources (Luxembourg)",
        "year": 2017, "lang": "en", "category": "national_law", "source": "ASTRO",
        "url": "https://www.unoosa.org/documents/pdf/spacelaw/national/luxembourg/2017E.pdf",
        "filename": "LUX_SPACE_2017_EN.pdf"
    },
    {
        "id": "LUX_SPACE_2017_FR",
        "title": "Loi du 20 juillet 2017 sur l'exploration et l'utilisation des ressources de l'espace (Luxembourg)",
        "year": 2017, "lang": "fr", "category": "national_law", "source": "ASTRO",
        "url": "https://www.unoosa.org/documents/pdf/spacelaw/national/luxembourg/2017F.pdf",
        "filename": "LUX_SPACE_2017_FR.pdf"
    },

    # ── Canada — Remote Sensing Space Systems Act 2005 ────────────────────────
    {
        "id": "CANADA_RSSA_2005_EN",
        "title": "Remote Sensing Space Systems Act (Canada, S.C. 2005, c. 45)",
        "year": 2005, "lang": "en", "category": "national_law", "source": "ASTRO",
        "url": "https://laws-lois.justice.gc.ca/PDF/R-5.4.pdf",
        "filename": "CANADA_RSSA_2005_EN.pdf"
    },

    # ── Algérie — Loi spatiale 2019 ──────────────────────────────────────────
    {
        "id": "ALGERIA_SPACE_2019_FR",
        "title": "Loi n° 19-06 du 17 juillet 2019 relative aux activités spatiales (Algérie)",
        "year": 2019, "lang": "fr", "category": "national_law", "source": "ASTRO",
        "url": "https://www.unoosa.org/documents/pdf/spacelaw/national/algeria/2019F.pdf",
        "filename": "ALGERIA_SPACE_2019_FR.pdf"
    },
    {
        "id": "ALGERIA_SPACE_2019_EN",
        "title": "Law No. 19-06 of 17 July 2019 on Space Activities (Algeria)",
        "year": 2019, "lang": "en", "category": "national_law", "source": "ASTRO",
        "url": "https://www.unoosa.org/documents/pdf/spacelaw/national/algeria/2019E.pdf",
        "filename": "ALGERIA_SPACE_2019_EN.pdf"
    },

    # ── USA — Commercial Space Launch Competitiveness Act 2015 ────────────────
    {
        "id": "USA_CSLCA_2015_EN",
        "title": "U.S. Commercial Space Launch Competitiveness Act (Public Law 114-90, 2015)",
        "year": 2015, "lang": "en", "category": "national_law", "source": "US_GOV",
        "url": "https://www.congress.gov/114/plaws/publ90/PLAW-114publ90.pdf",
        "filename": "USA_CSLCA_2015_EN.pdf"
    },

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 6 — ITU & TÉLÉCOMMUNICATIONS SPATIALES
    # ════════════════════════════════════════════════════════════════════════

    # ── ITU Radio Regulations (Space Services) ───────────────────────────────
    {
        "id": "ITU_RR_SPACE_EN",
        "title": "ITU Radio Regulations — Space Services (Articles 21-22)",
        "year": 2020, "lang": "en", "category": "regulation", "source": "ITU",
        "url": "https://www.itu.int/dms_pub/itu-r/opb/rb/R-RB-RR-2020-PDF-E.pdf",
        "filename": "ITU_RR_SPACE_EN.pdf"
    },
    {
        "id": "ITU_RR_SPACE_FR",
        "title": "Règlement des radiocommunications — Services spatiaux (Articles 21-22)",
        "year": 2020, "lang": "fr", "category": "regulation", "source": "ITU",
        "url": "https://www.itu.int/dms_pub/itu-r/opb/rb/R-RB-RR-2020-PDF-F.pdf",
        "filename": "ITU_RR_SPACE_FR.pdf"
    },

    # ════════════════════════════════════════════════════════════════════════
    # BLOC 7 — DEBRIS & DURABILITÉ
    # ════════════════════════════════════════════════════════════════════════

    # ── IADC Space Debris Mitigation Guidelines ───────────────────────────────
    {
        "id": "IADC_DEBRIS_2007_EN",
        "title": "IADC Space Debris Mitigation Guidelines (Inter-Agency Space Debris Coordination Committee)",
        "year": 2007, "lang": "en", "category": "guideline", "source": "IADC",
        "url": "https://www.unoosa.org/documents/pdf/spacelaw/sd/IADC-2002-01-IADC-Space_Debris-Guidelines-Revision1.pdf",
        "filename": "IADC_DEBRIS_2007_EN.pdf"
    },
    # ── UN Space Debris Mitigation Guidelines 2010 ────────────────────────────
    {
        "id": "UN_DEBRIS_2010_EN",
        "title": "United Nations Space Debris Mitigation Guidelines — COPUOS 2010",
        "year": 2010, "lang": "en", "category": "guideline", "source": "UNOOSA",
        "url": "https://www.unoosa.org/pdf/publications/st_space_49E.pdf",
        "filename": "UN_DEBRIS_2010_EN.pdf"
    },
]

# ─────────────────────────────────────────────
# Session HTTP avec retry automatique
# ─────────────────────────────────────────────

def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ─────────────────────────────────────────────
# Téléchargement
# ─────────────────────────────────────────────

def download_document(session: requests.Session, doc: dict, dry_run: bool = False) -> dict:
    """
    Télécharge un document et retourne un enregistrement de métadonnées.
    Retourne None si le téléchargement échoue.
    """
    lang_dir = RAW_DIR / doc["lang"]
    lang_dir.mkdir(parents=True, exist_ok=True)
    dest = lang_dir / doc["filename"]

    record = {
        **doc,
        "downloaded_at": None,
        "file_path": str(dest.relative_to(BASE_DIR)),
        "file_size_kb": None,
        "sha256": None,
        "status": "pending"
    }

    if dry_run:
        log.info(f"[DRY-RUN] {doc['id']} → {doc['url']}")
        record["status"] = "dry-run"
        return record

    # Éviter les re-téléchargements
    if dest.exists():
        log.info(f"[SKIP] {doc['id']} — déjà présent ({dest.stat().st_size // 1024} Ko)")
        record["status"] = "skipped"
        record["file_size_kb"] = dest.stat().st_size // 1024
        record["downloaded_at"] = datetime.fromtimestamp(dest.stat().st_mtime).isoformat()
        return record

    try:
        log.info(f"[GET] {doc['id']} ← {doc['url']}")
        response = session.get(doc["url"], timeout=30, stream=True)
        response.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Métadonnées post-téléchargement
        raw_bytes = dest.read_bytes()
        record["file_size_kb"] = len(raw_bytes) // 1024
        record["sha256"] = hashlib.sha256(raw_bytes).hexdigest()
        record["downloaded_at"] = datetime.utcnow().isoformat()
        record["status"] = "ok"
        log.info(f"  ✓ {record['file_size_kb']} Ko — sha256: {record['sha256'][:12]}...")

    except requests.exceptions.HTTPError as e:
        log.warning(f"  ✗ HTTP {e.response.status_code} — {doc['id']}")
        record["status"] = f"http_error_{e.response.status_code}"
    except requests.exceptions.ConnectionError:
        log.warning(f"  ✗ Connexion impossible — {doc['id']}")
        record["status"] = "connection_error"
    except requests.exceptions.Timeout:
        log.warning(f"  ✗ Timeout — {doc['id']}")
        record["status"] = "timeout"
    except Exception as e:
        log.error(f"  ✗ Erreur inattendue pour {doc['id']}: {e}")
        record["status"] = f"error: {e}"

    return record


# ─────────────────────────────────────────────
# Registre sources.json
# ─────────────────────────────────────────────

def load_sources() -> list:
    if SOURCES_FILE.exists():
        return json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    return []


def save_sources(records: list):
    SOURCES_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = {r["id"]: r for r in load_sources()}
    for r in records:
        existing[r["id"]] = r
    SOURCES_FILE.write_text(
        json.dumps(list(existing.values()), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    log.info(f"[sources.json] {len(existing)} documents enregistrés → {SOURCES_FILE}")


# ─────────────────────────────────────────────
# Rapport final
# ─────────────────────────────────────────────

def print_report(records: list):
    ok      = [r for r in records if r["status"] == "ok"]
    skipped = [r for r in records if r["status"] == "skipped"]
    failed  = [r for r in records if r["status"] not in ("ok", "skipped", "dry-run", "pending")]

    print("\n" + "═" * 60)
    print("  BetterCallSpaceLLM — Rapport de collecte Phase 1")
    print("═" * 60)
    print(f"  Total    : {len(records)}")
    print(f"  ✓ OK      : {len(ok)}")
    print(f"  ⊘ Ignorés : {len(skipped)}")
    print(f"  ✗ Échecs  : {len(failed)}")

    # Ventilation par source
    sources = {}
    for r in records:
        sources.setdefault(r["source"], {"ok": 0, "fail": 0})
        if r["status"] in ("ok", "skipped"):
            sources[r["source"]]["ok"] += 1
        elif r["status"] not in ("dry-run", "pending"):
            sources[r["source"]]["fail"] += 1
    print("\n  Par source :")
    for src, counts in sorted(sources.items()):
        print(f"    {src:<12} ✓{counts['ok']}  ✗{counts['fail']}")

    # Ventilation par langue
    langs = {}
    for r in records:
        langs.setdefault(r["lang"], 0)
        langs[r["lang"]] += 1
    print("\n  Par langue :")
    for lang, count in sorted(langs.items()):
        print(f"    {lang:<6} {count} document(s)")

    if failed:
        print("\n  Documents en échec :")
        for r in failed:
            print(f"    ✗ {r['id']} [{r['status']}]")

    total_kb = sum(r.get("file_size_kb") or 0 for r in records)
    print(f"\n  Taille totale téléchargée : ~{total_kb} Ko")
    print("═" * 60 + "\n")


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="BetterCallSpaceLLM — collecte des documents juridiques spatiaux",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--lang",     nargs="+", default=None, help="Filtrer par langue(s)    : en fr ru zh es ar")
    parser.add_argument("--category", nargs="+", default=None, help="Filtrer par catégorie(s) : treaty convention declaration resolution guideline national_law regulation agreement soft_law")
    parser.add_argument("--source",   nargs="+", default=None, help="Filtrer par source(s)    : UNOOSA NASA ESA ITU ASTRO IADC US_GOV")
    parser.add_argument("--dry-run",  action="store_true",     help="Lister les URLs sans télécharger")
    parser.add_argument("--delay",    type=float, default=DELAY_BETWEEN_REQUESTS, help="Délai entre requêtes (s)")
    parser.add_argument("--list",     action="store_true",     help="Afficher le catalogue complet et quitter")
    args = parser.parse_args()

    # ── Mode liste ────────────────────────────────────────────────────────
    if args.list:
        print(f"\n{'─'*72}")
        print(f"  {'ID':<30} {'LANG':<5} {'CAT':<14} {'SOURCE':<10} ANNÉE")
        print(f"{'─'*72}")
        for d in DOCUMENTS:
            print(f"  {d['id']:<30} {d['lang']:<5} {d['category']:<14} {d['source']:<10} {d['year']}")
        print(f"{'─'*72}")
        print(f"  Total : {len(DOCUMENTS)} documents\n")
        return

    # ── Filtres ───────────────────────────────────────────────────────────
    docs = DOCUMENTS
    if args.lang:
        docs = [d for d in docs if d["lang"] in args.lang]
        log.info(f"Filtre langue    : {args.lang} → {len(docs)} documents")
    if args.category:
        docs = [d for d in docs if d["category"] in args.category]
        log.info(f"Filtre catégorie : {args.category} → {len(docs)} documents")
    if args.source:
        docs = [d for d in docs if d["source"] in args.source]
        log.info(f"Filtre source    : {args.source} → {len(docs)} documents")

    # Exclure les documents nécessitant un traitement manuel
    manual = [d for d in docs if d.get("note")]
    if manual:
        log.warning(f"{len(manual)} document(s) nécessitent un traitement manuel et seront ignorés :")
        for d in manual:
            log.warning(f"  → {d['id']} : {d.get('note')}")
        docs = [d for d in docs if not d.get("note")]

    log.info(f"BetterCallSpaceLLM Phase 1 — {len(docs)} documents à traiter")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    session = build_session()
    records = []

    for i, doc in enumerate(docs):
        record = download_document(session, doc, dry_run=args.dry_run)
        records.append(record)

        # Délai poli entre requêtes (respecter les serveurs)
        if not args.dry_run and i < len(docs) - 1:
            time.sleep(args.delay)

    if not args.dry_run:
        save_sources(records)

    print_report(records)


if __name__ == "__main__":
    main()