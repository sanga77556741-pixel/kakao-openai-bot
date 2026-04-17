import os
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
KOSHA_API_KEY = os.getenv("KOSHA_API_KEY")

BASE_URL = "https://apis.data.go.kr/B552468/msdschem"

print("DEBUG - KOSHA_API_KEY loaded:", bool(KOSHA_API_KEY))
print("DEBUG - DATABASE_URL loaded:", bool(DATABASE_URL))
print("DEBUG - KOSHA_API_KEY preview:", (KOSHA_API_KEY[:12] + "...") if KOSHA_API_KEY else None)
print("DEBUG - DATABASE_URL preview:", (DATABASE_URL[:40] + "...") if DATABASE_URL else None)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")

if not KOSHA_API_KEY:
    raise RuntimeError("KOSHA_API_KEY 환경변수가 설정되지 않았습니다.")


def get_connection():
    return psycopg.connect(DATABASE_URL)


def test_db_connection():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                row = cur.fetchone()
                print("DEBUG - DB connection test:", row)
    except Exception as e:
        print("DEBUG - DB connection failed:", e)
        raise


def safe_strip(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def call_kosha_api(endpoint: str, extra_params: Dict[str, Any]) -> str:
    url = f"{BASE_URL}/{endpoint}"
    params = {
        "serviceKey": KOSHA_API_KEY,
        **extra_params,
    }

    print(f"DEBUG - Requesting: {url}")
    print(f"DEBUG - Params: {params}")

    response = requests.get(url, params=params, timeout=30)
    print("DEBUG - API status code:", response.status_code)
    print("DEBUG - API raw text preview:", response.text[:1000])

    response.raise_for_status()
    return response.text


def xml_to_items(xml_text: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print("DEBUG - XML parse failed:", e)
        return items

    item_nodes = root.findall(".//item")
    for node in item_nodes:
        row: Dict[str, str] = {}
        for child in node:
            tag = child.tag.strip() if child.tag else ""
            text = child.text.strip() if child.text else ""
            row[tag] = text
        items.append(row)

    return items


def get_total_count(xml_text: str) -> int:
    try:
        root = ET.fromstring(xml_text)
        node = root.find(".//totalCount")
        if node is not None and node.text:
            return int(node.text.strip())
    except Exception:
        pass
    return 0


def get_result_code(xml_text: str) -> Optional[str]:
    try:
        root = ET.fromstring(xml_text)
        node = root.find(".//resultCode")
        if node is not None and node.text:
            return node.text.strip()
    except Exception:
        pass
    return None


def get_result_msg(xml_text: str) -> Optional[str]:
    try:
        root = ET.fromstring(xml_text)
        node = root.find(".//resultMsg")
        if node is not None and node.text:
            return node.text.strip()
    except Exception:
        pass
    return None


def pick_first(item: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        value = safe_strip(item.get(key))
        if value:
            return value
    return None


def upsert_chemical(name_ko: str) -> Optional[int]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chemicals (name_ko)
                VALUES (%s)
                ON CONFLICT (name_ko) DO UPDATE
                SET name_ko = EXCLUDED.name_ko
                RETURNING id
                """,
                (name_ko,),
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


def insert_hazard(chemical_id: int, hazard_type: Optional[str], description: Optional[str]) -> None:
    hazard_type = safe_strip(hazard_type) or "유해성"
    description = safe_strip(description) or ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hazards (chemical_id, hazard_type, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (chemical_id, hazard_type, description) DO NOTHING
                """,
                (chemical_id, hazard_type, description),
            )
            conn.commit()


def update_chemical_detail(
    chemical_id: int,
    summary: Optional[str] = None,
    physical_state: Optional[str] = None,
    ppe: Optional[str] = None,
    precautions: Optional[str] = None,
    human_hazard: Optional[str] = None,
    fire_explosion_response: Optional[str] = None,
    marine_spill_response: Optional[str] = None,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE chemicals
                SET
                    summary = COALESCE(%s, summary),
                    physical_state = COALESCE(%s, physical_state),
                    ppe = COALESCE(%s, ppe),
                    precautions = COALESCE(%s, precautions),
                    human_hazard = COALESCE(%s, human_hazard),
                    fire_explosion_response = COALESCE(%s, fire_explosion_response),
                    marine_spill_response = COALESCE(%s, marine_spill_response)
                WHERE id = %s
                """,
                (
                    safe_strip(summary),
                    safe_strip(physical_state),
                    safe_strip(ppe),
                    safe_strip(precautions),
                    safe_strip(human_hazard),
                    safe_strip(fire_explosion_response),
                    safe_strip(marine_spill_response),
                    chemical_id,
                ),
            )
            conn.commit()


def fetch_chemical_list(page_no: int = 1, num_of_rows: int = 100, chem_nm: Optional[str] = None) -> str:
    params: Dict[str, Any] = {
        "pageNo": page_no,
        "numOfRows": num_of_rows,
    }

    if chem_nm:
        params["searchWrd"] = chem_nm

    return call_kosha_api("getChemList", params)


def fetch_chemical_detail_01(chem_id: str) -> str:
    return call_kosha_api("getChemDetail01", {"chemId": chem_id})


def fetch_chemical_detail_02(chem_id: str) -> str:
    return call_kosha_api("getChemDetail02", {"chemId": chem_id})


def fetch_chemical_detail_04(chem_id: str) -> str:
    return call_kosha_api("getChemDetail04", {"chemId": chem_id})


def fetch_chemical_detail_05(chem_id: str) -> str:
    return call_kosha_api("getChemDetail05", {"chemId": chem_id})


def fetch_chemical_detail_06(chem_id: str) -> str:
    return call_kosha_api("getChemDetail06", {"chemId": chem_id})


def map_list_item(item: Dict[str, Any]) -> Dict[str, Optional[str]]:
    chem_id = pick_first(item, ["chemId", "chem_id", "msdsNo", "msds_no", "id", "chemclsno"])
    name_ko = pick_first(item, ["chemNm", "chem_nm", "name", "korNm", "kor_nm", "koreanName", "chmknm"])
    name_en = pick_first(item, ["engNm", "eng_nm", "engName", "englishName", "engnm"])
    cas_no = pick_first(item, ["casNo", "cas_no", "cas", "casno"])

    return {
        "chem_id": chem_id,
        "name_ko": name_ko,
        "name_en": name_en,
        "cas_no": cas_no,
    }


def join_item_text(items: List[Dict[str, Any]]) -> Optional[str]:
    texts: List[str] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        parts = []
        for value in item.values():
            text = safe_strip(value)
            if text:
                parts.append(text)

        if parts:
            texts.append(" / ".join(parts))

    if not texts:
        return None

    return "\n".join(texts[:5])


def ingest_one_list_item(item: Dict[str, Any], debug: bool = False) -> None:
    mapped = map_list_item(item)

    if debug:
        print("[LIST ITEM RAW]")
        print(item)
        print("[LIST ITEM MAPPED]")
        print(mapped)

    name_ko = mapped["name_ko"]
    if not name_ko:
        print("DEBUG - name_ko 없음, 이 항목은 건너뜀")
        return

    chemical_id = upsert_chemical(name_ko)
    if not chemical_id:
        print("DEBUG - chemical_id 생성 실패")
        return

    insert_alias(chemical_id, name_ko)
    insert_alias(chemical_id, mapped["name_en"])
    insert_alias(chemical_id, mapped["cas_no"])

    chem_id = mapped["chem_id"]
    if not chem_id:
        print("DEBUG - chem_id 없음, 상세조회 건너뜀")
        return

    try:
        detail01_xml = fetch_chemical_detail_01(chem_id)
        detail02_xml = fetch_chemical_detail_02(chem_id)
        detail04_xml = fetch_chemical_detail_04(chem_id)
        detail05_xml = fetch_chemical_detail_05(chem_id)
        detail06_xml = fetch_chemical_detail_06(chem_id)

        detail01_items = xml_to_items(detail01_xml)
        detail02_items = xml_to_items(detail02_xml)
        detail04_items = xml_to_items(detail04_xml)
        detail05_items = xml_to_items(detail05_xml)
        detail06_items = xml_to_items(detail06_xml)

        if debug:
            print("[DETAIL01 ITEMS]")
            print(detail01_items[:1])
            print("[DETAIL02 ITEMS]")
            print(detail02_items[:1])
            print("[DETAIL04 ITEMS]")
            print(detail04_items[:1])
            print("[DETAIL05 ITEMS]")
            print(detail05_items[:1])
            print("[DETAIL06 ITEMS]")
            print(detail06_items[:1])

        summary = None
        physical_state = None
        ppe = None
        precautions = None

        if detail01_items:
            d1 = detail01_items[0]

            summary = pick_first(
                d1,
                ["summary", "outline", "substanceOverview", "productName", "chemNm", "chem_nm", "chmknm"],
            )
            physical_state = pick_first(
                d1,
                ["physicalState", "state", "appearance", "physical_form"],
            )
            ppe = pick_first(
                d1,
                ["ppe", "protectiveEquipment", "personalProtection", "personal_protection"],
            )
            precautions = pick_first(
                d1,
                ["precautions", "handlingStorage", "handling_storage", "storagePrecautions"],
            )

            insert_alias(chemical_id, pick_first(d1, ["engNm", "eng_nm", "engName", "engnm"]))
            insert_alias(chemical_id, pick_first(d1, ["casNo", "cas_no", "cas", "casno"]))

        for hazard in detail02_items:
            hazard_type = pick_first(
                hazard,
                ["hazardType", "hazard_type", "category", "title", "classNm", "class_nm"],
            )
            description = pick_first(
                hazard,
                ["description", "content", "detail", "value", "hazardDesc", "hazard_desc"],
            )

            if hazard_type or description:
                insert_hazard(chemical_id, hazard_type, description)

        human_hazard = join_item_text(detail04_items)
        fire_response = join_item_text(detail05_items)
        spill_response = join_item_text(detail06_items)

        update_chemical_detail(
            chemical_id=chemical_id,
            summary=summary,
            physical_state=physical_state,
            ppe=ppe,
            precautions=precautions,
            human_hazard=human_hazard,
            fire_explosion_response=fire_response,
            marine_spill_response=spill_response,
        )

        print(f"[OK] {name_ko}")

    except Exception as e:
        print(f"[DETAIL ERROR] {name_ko}: {e}")


def ingest_pages(
    start_page: int = 1,
    end_page: int = 1,
    num_of_rows: int = 5,
    debug: bool = True,
    chem_nm: Optional[str] = None,
) -> None:
    for page in range(start_page, end_page + 1):
        xml_text = fetch_chemical_list(page_no=page, num_of_rows=num_of_rows, chem_nm=chem_nm)

        if debug:
            print(f"\n=== PAGE {page} RAW PREVIEW ===")
            print(xml_text[:2000])

        result_code = get_result_code(xml_text)
        result_msg = get_result_msg(xml_text)
        total_count = get_total_count(xml_text)
        items = xml_to_items(xml_text)

        print(f"[PAGE {page}] resultCode = {result_code}")
        print(f"[PAGE {page}] resultMsg = {result_msg}")
        print(f"[PAGE {page}] totalCount = {total_count}")
        print(f"[PAGE {page}] item count = {len(items)}")

        if not items:
            print("조회 결과가 없습니다.")
            break

        for idx, item in enumerate(items, start=1):
            print(f"[PAGE {page}] processing item {idx}/{len(items)}")
            ingest_one_list_item(item, debug=debug and idx == 1)
            time.sleep(0.2)


if __name__ == "__main__":
    SEARCH_TERM = "염산"

    print("\n===== STEP 1: ENV CHECK =====")
    print("KOSHA_API_KEY loaded:", bool(KOSHA_API_KEY))
    print("DATABASE_URL loaded:", bool(DATABASE_URL))

    print("\n===== STEP 2: DB CONNECTION TEST =====")
    test_db_connection()

    print("\n===== STEP 3: API LIST TEST =====")
    xml_text = fetch_chemical_list(page_no=1, num_of_rows=5, chem_nm=SEARCH_TERM)
    print("DEBUG - XML preview:")
    print(xml_text[:2000])
    print("DEBUG - resultCode:", get_result_code(xml_text))
    print("DEBUG - resultMsg:", get_result_msg(xml_text))
    print("DEBUG - totalCount:", get_total_count(xml_text))
    print("DEBUG - parsed item count:", len(xml_to_items(xml_text)))

    print("\n===== STEP 4: INGEST TEST =====")
    ingest_pages(start_page=1, end_page=1, num_of_rows=5, debug=True, chem_nm=SEARCH_TERM)