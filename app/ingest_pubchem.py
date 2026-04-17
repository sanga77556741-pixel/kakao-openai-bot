import os
import time
from typing import Any, Dict, List, Optional, Set

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"


def get_connection():
    return psycopg.connect(DATABASE_URL)


def safe_strip(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def normalize_alias(alias: str) -> str:
    return " ".join(alias.strip().lower().split())


def call_pubchem_json(path: str, timeout: int = 30) -> Dict[str, Any]:
    url = f"{PUBCHEM_BASE}/{path}"
    print(f"DEBUG - Requesting: {url}")
    response = requests.get(url, timeout=timeout)
    print("DEBUG - status:", response.status_code)
    print("DEBUG - preview:", response.text[:500])
    response.raise_for_status()
    return response.json()


def ensure_chemical_constraints():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE chemicals
                ADD CONSTRAINT IF NOT EXISTS unique_chemicals_name_ko
                UNIQUE (name_ko)
            """)
            conn.commit()


def upsert_chemical(name_ko: str, summary: Optional[str] = None, physical_state: Optional[str] = None) -> Optional[int]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chemicals (name_ko, summary, physical_state)
                VALUES (%s, %s, %s)
                ON CONFLICT (name_ko) DO UPDATE
                SET
                    summary = COALESCE(EXCLUDED.summary, chemicals.summary),
                    physical_state = COALESCE(EXCLUDED.physical_state, chemicals.physical_state)
                RETURNING id
                """,
                (name_ko, summary, physical_state),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None


def insert_alias(chemical_id: int, alias: Optional[str]) -> None:
    alias = safe_strip(alias)
    if not alias:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chemical_aliases (chemical_id, alias)
                VALUES (%s, %s)
                ON CONFLICT (chemical_id, alias) DO NOTHING
                """,
                (chemical_id, alias),
            )
            conn.commit()


def fetch_cids_by_name(name: str) -> List[int]:
    path = f"compound/name/{requests.utils.quote(name)}/cids/JSON"
    data = call_pubchem_json(path)

    id_list = data.get("IdentifierList", {}).get("CID", [])
    return [int(cid) for cid in id_list[:10]]


def fetch_compound_props_by_cid(cid: int) -> Dict[str, Any]:
    path = (
        f"compound/cid/{cid}/property/"
        "Title,MolecularFormula,MolecularWeight,CanonicalSMILES,IUPACName/JSON"
    )
    data = call_pubchem_json(path)

    props = data.get("PropertyTable", {}).get("Properties", [])
    return props[0] if props else {}


def fetch_synonyms_by_cid(cid: int) -> List[str]:
    path = f"compound/cid/{cid}/synonyms/JSON"
    data = call_pubchem_json(path)

    info = data.get("InformationList", {}).get("Information", [])
    if not info:
        return []

    synonyms = info[0].get("Synonym", [])
    return synonyms[:100]


def fetch_record_description_by_cid(cid: int) -> Optional[str]:
    # PUG-View에서 설명 문구 가져오기
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{cid}/JSON"
    print(f"DEBUG - Requesting: {url}")
    response = requests.get(url, timeout=30)
    print("DEBUG - status:", response.status_code)
    print("DEBUG - preview:", response.text[:500])

    if response.status_code != 200:
        return None

    data = response.json()

    record = data.get("Record", {})
    sections = record.get("Section", [])

    def walk_sections(section_list: List[Dict[str, Any]]) -> Optional[str]:
        for section in section_list:
            heading = section.get("TOCHeading")
            if heading == "Record Description":
                infos = section.get("Information", [])
                for info in infos:
                    value = info.get("Value", {})
                    strings = value.get("StringWithMarkup", [])
                    for s in strings:
                        text = safe_strip(s.get("String"))
                        if text:
                            return text

            nested = section.get("Section", [])
            found = walk_sections(nested)
            if found:
                return found

        return None

    return walk_sections(sections)


def choose_primary_name(props: Dict[str, Any], fallback_name: str) -> str:
    title = safe_strip(props.get("Title"))
    iupac = safe_strip(props.get("IUPACName"))

    if title:
        return title
    if iupac:
        return iupac
    return fallback_name


def ingest_pubchem_name(search_name: str, max_compounds: int = 3) -> None:
    cids = fetch_cids_by_name(search_name)

    if not cids:
        print(f"[SKIP] CID를 찾지 못함: {search_name}")
        return

    for cid in cids[:max_compounds]:
        try:
            props = fetch_compound_props_by_cid(cid)
            synonyms = fetch_synonyms_by_cid(cid)
            description = fetch_record_description_by_cid(cid)

            primary_name = choose_primary_name(props, search_name)
            molecular_formula = safe_strip(props.get("MolecularFormula"))
            molecular_weight = safe_strip(props.get("MolecularWeight"))

            summary_parts = []
            if description:
                summary_parts.append(description)
            if molecular_formula:
                summary_parts.append(f"분자식: {molecular_formula}")
            if molecular_weight:
                summary_parts.append(f"분자량: {molecular_weight}")

            summary = " / ".join(summary_parts) if summary_parts else None

            chemical_id = upsert_chemical(
                name_ko=primary_name,
                summary=summary,
                physical_state=None,
            )

            if not chemical_id:
                print(f"[SKIP] chemical 저장 실패: {search_name}")
                continue

            alias_set: Set[str] = set()

            alias_set.add(primary_name)
            alias_set.add(search_name)

            for syn in synonyms:
                cleaned = safe_strip(syn)
                if cleaned:
                    alias_set.add(cleaned)

            for alias in sorted(alias_set):
                insert_alias(chemical_id, alias)

            print(f"[OK] {search_name} -> CID {cid} -> {primary_name} / aliases {len(alias_set)}개")

            time.sleep(0.2)

        except Exception as e:
            print(f"[ERROR] {search_name} / CID {cid}: {e}")


if __name__ == "__main__":
    # 1차 테스트용 100개보다 먼저, 아래 5개로 확인
    search_terms = [
        "benzene",
        "toluene",
        "ammonia",
        "hydrochloric acid",
        "sulfuric acid",
    ]

    for term in search_terms:
        ingest_pubchem_name(term, max_compounds=1)