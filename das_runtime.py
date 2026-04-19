from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


TEST_MODE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "SHIPWORK_DT",
    "WH_CD",
    "CLASS_LV1",
    "WAVE_NO",
    "DESCR",
    "DLVRY_NO",
    "DLVRY_NM",
    "ROUTE_DELI_SEQN",
    "CENT_CODE",
    "MAGM_SEQN",
    "CUST_SUMM",
    "FRCU_CODE",
    "SHIPTO_ID",
    "SHIPTO_NM",
    "ITEM_CD",
    "ITEM_NM",
    "CONV_TOPICK_QTY",
    "SUNIT_TOPICK_QTY",
    "SHIP_STRG_CD",
    "SHIPTO_AREA_NM",
    "WORKING_TIME",
    "MHE_ID",
)


BLURRED_API_BUILD_CONFIG: dict[str, str] = {
    "entrypoint": "app:app",
    "host": "<blurred>",
    "port": "<blurred>",
    "reload": "<blurred>",
}


@dataclass(frozen=True, slots=True)
class TestModeConfig:
    date: str = "20260115"
    cent: str = "1000"
    class_lv: str = "상온"
    group_min: int = 30
    max_sku: int = 300
    das_unit: int = 45
    out_dir: Path = Path("data") / "test_mode"

    def default_output_path(self) -> Path:
        return self.out_dir / f"[TEST][{self.class_lv}]클러스터링 결과_{self.cent}_{self.date}.xlsx"

    def to_main_kwargs(self) -> dict[str, object]:
        return {
            "date": self.date,
            "cent": self.cent,
            "CLASS_LV": self.class_lv,
            "group_min": self.group_min,
            "max_SKU": self.max_sku,
            "Das_unit": self.das_unit,
            "test_mode": True,
            "out_xlsx": self.default_output_path(),
        }


DEFAULT_TEST_MODE_CONFIG = TestModeConfig()


def build_dummy_records(config: TestModeConfig | None = None) -> list[dict[str, object]]:
    config = config or DEFAULT_TEST_MODE_CONFIG

    group_specs = (
        {
            "name": "수도권_1차",
            "wave_no": "W01",
            "magm_seqn": "1",
            "area_nm": "수도권",
            "count": 32,
            "item_pool": ("SKU-A01", "SKU-A02", "SKU-A03", "SKU-A04", "SKU-A05", "SKU-A06"),
        },
        {
            "name": "지방_1차",
            "wave_no": "W02",
            "magm_seqn": "1",
            "area_nm": "지방",
            "count": 31,
            "item_pool": ("SKU-B01", "SKU-B02", "SKU-B03", "SKU-B04", "SKU-B05", "SKU-B06"),
        },
        {
            "name": "수도권_2차",
            "wave_no": "W03",
            "magm_seqn": "2",
            "area_nm": "수도권",
            "count": 30,
            "item_pool": ("SKU-C01", "SKU-C02", "SKU-C03", "SKU-C04", "SKU-C05", "SKU-C06"),
        },
        {
            "name": "지방_2차",
            "wave_no": "W04",
            "magm_seqn": "2",
            "area_nm": "지방",
            "count": 33,
            "item_pool": ("SKU-D01", "SKU-D02", "SKU-D03", "SKU-D04", "SKU-D05", "SKU-D06"),
        },
        {
            "name": "수도권_3차",
            "wave_no": "W05",
            "magm_seqn": "3",
            "area_nm": "수도권",
            "count": 34,
            "item_pool": ("SKU-E01", "SKU-E02", "SKU-E03", "SKU-E04", "SKU-E05", "SKU-E06"),
        },
        {
            "name": "지방_3차",
            "wave_no": "W06",
            "magm_seqn": "3",
            "area_nm": "지방",
            "count": 30,
            "item_pool": ("SKU-F01", "SKU-F02", "SKU-F03", "SKU-F04", "SKU-F05", "SKU-F06"),
        },
    )

    records: list[dict[str, object]] = []
    for group_idx, spec in enumerate(group_specs, start=1):
        for store_idx in range(spec["count"]):
            shipto_id = f"{config.cent}{group_idx:02d}{store_idx + 1:03d}"
            shipto_name = f"TEST_{spec['name']}_{store_idx + 1:03d}"
            dlvry_no = f"R{group_idx:02d}{(store_idx % 7) + 1:02d}"
            dlvry_nm = f"TEST_ROUTE_{spec['name']}"
            frcu_code = f"FC{group_idx:02d}"

            item_pool = list(spec["item_pool"])
            start = store_idx % len(item_pool)
            selected_items = [item_pool[start], item_pool[(start + 1) % len(item_pool)]]
            if store_idx % 4 == 0:
                selected_items.append(item_pool[(start + 2) % len(item_pool)])

            for item_idx, item_cd in enumerate(selected_items, start=1):
                hour = 8 + ((store_idx + item_idx) % 10)
                minute = (store_idx * 7 + item_idx * 11) % 60
                second = (store_idx * 13 + item_idx * 17) % 60
                records.append(
                    {
                        "SHIPWORK_DT": config.date,
                        "WH_CD": config.cent,
                        "CLASS_LV1": config.class_lv,
                        "WAVE_NO": spec["wave_no"],
                        "DESCR": f"TEST_{spec['name']}_WAVE",
                        "DLVRY_NO": dlvry_no,
                        "DLVRY_NM": dlvry_nm,
                        "ROUTE_DELI_SEQN": str((store_idx % 3) + 1),
                        "CENT_CODE": config.cent,
                        "MAGM_SEQN": spec["magm_seqn"],
                        "CUST_SUMM": f"{spec['name']} 테스트 더미 데이터",
                        "FRCU_CODE": frcu_code,
                        "SHIPTO_ID": shipto_id,
                        "SHIPTO_NM": shipto_name,
                        "ITEM_CD": item_cd,
                        "ITEM_NM": f"상품_{item_cd}",
                        "CONV_TOPICK_QTY": (store_idx % 4) + 1,
                        "SUNIT_TOPICK_QTY": item_idx,
                        "SHIP_STRG_CD": "D",
                        "SHIPTO_AREA_NM": spec["area_nm"],
                        "WORKING_TIME": f"{config.date}{hour:02d}{minute:02d}{second:02d}",
                        "MHE_ID": "DAS",
                    }
                )

    return records
