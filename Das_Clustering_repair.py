from __future__ import annotations

import numpy as np
import pandas as pd
import os
import json
import oracledb
import argparse
from pathlib import Path

from dataclasses import dataclass
from typing import Dict, List, Set, Optional, Tuple, Iterable

from das_runtime import TestModeConfig, TEST_MODE_REQUIRED_COLUMNS, build_dummy_records

class NoDataError(RuntimeError):
    """Base class for expected no-data cases."""

class NoSqlRowsError(NoDataError):
    """SQL result is empty for the given date/cent."""

class NoCentRowsAfterSummarizeError(NoDataError):
    """No rows remain after cent/shipto summarization."""


DEFAULT_DB_CLIENT_PATH = r"/opt/oracle/instantclient_21_5"


###################### DB #################################
DB_ENV_VAR_MAP = {
    "host": "DAS_DB_HOST",
    "port": "DAS_DB_PORT",
    "sid": "DAS_DB_SID",
    "user": "DAS_DB_USER",
    "password": "DAS_DB_PASSWORD",
}


def _load_db_config(
    db_config_path: str | None = None,
    *,
    host: str | None = None,
    port: int | str | None = None,
    sid: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict[str, str | int]:
    file_conf: dict[str, str | int] = {}
    if db_config_path is not None:
        with open(db_config_path) as json_file:
            raw_conf = json.load(json_file)
        file_conf = {
            "host": raw_conf.get("host"),
            "port": raw_conf.get("port"),
            "sid": raw_conf.get("sid"),
            "user": raw_conf.get("user") or raw_conf.get("id"),
            "password": raw_conf.get("password") or raw_conf.get("pwd"),
        }

    conf: dict[str, str | int | None] = {
        "host": host or os.getenv(DB_ENV_VAR_MAP["host"]) or file_conf.get("host"),
        "port": port or os.getenv(DB_ENV_VAR_MAP["port"]) or file_conf.get("port"),
        "sid": sid
        or os.getenv(DB_ENV_VAR_MAP["sid"])
        or os.getenv("DAS_DB_SERVICE_NAME")
        or file_conf.get("sid"),
        "user": user or os.getenv(DB_ENV_VAR_MAP["user"]) or file_conf.get("user"),
        "password": password or os.getenv(DB_ENV_VAR_MAP["password"]) or file_conf.get("password"),
    }

    missing = [key for key, value in conf.items() if value in (None, "")]
    if missing:
        missing_env = ", ".join(DB_ENV_VAR_MAP[key] for key in missing)
        raise ValueError(
            "Missing DB connection settings: "
            f"{', '.join(missing)}. "
            f"Set {missing_env} environment variables or provide db_config_path."
        )

    conf["port"] = int(conf["port"])
    return conf  # type: ignore[return-value]


def connect_db(
    db_config_path: str | None = None,
    client_path: str = DEFAULT_DB_CLIENT_PATH,
    *,
    host: str | None = None,
    port: int | str | None = None,
    sid: str | None = None,
    user: str | None = None,
    password: str | None = None,
):
    
    try:
        oracledb.init_oracle_client(lib_dir=client_path)
    except oracledb.ProgrammingError:
        # 이미 초기화된 경우 에러가 발생하므로 예외 처리
        pass
    
    conf = _load_db_config(
        db_config_path=db_config_path,
        host=host,
        port=port,
        sid=sid,
        user=user,
        password=password,
    )

    dsn = oracledb.makedsn(conf['host'], conf['port'], service_name=conf['sid'])

    # 연결
    return oracledb.connect(user=conf['user'], password=conf['password'], dsn=dsn)

################# 


def _build_sql(date: str) -> str:
    return f"""    
        SELECT
           A.SHIPWORK_DT
          ,A.WAVE_NO
          ,A.DESCR 
          ,A.DLVRY_NO
          ,D.ROUTE_DELI_SEQN
          ,A.DLVRY_NM 
          ,A.SHIPTO_ID
          ,A.SHIPTO_NM
          ,A.ITEM_CD 
          ,A.ITEM_NM 
          ,A.CONV_TOPICK_QTY 
          ,A.SUNIT_TOPICK_QTY
          ,A.SUNIT_DONE_QTY  
          ,A.SHIP_STRG_CD  
          ,A.WH_CD
          ,A.WORK_UNIT 
          ,A.WORK_TYPE 
          ,A.ZONE_CD
          ,A.WCELL_NO 
          ,A.OUTB_NO 
          ,A.OUTB_DETL_NO
          ,A.MHE_ID 
          ,A.PICK_A_BOX_YN 
          ,A.CLASS_LV1 
          ,A.CLASS_LV2 
          ,A.SHIPTO_AREA 
          ,A.SHIPTO_AREA_NM 
          ,A.PICKED_YN 
          ,A.WORKER_ID 
          ,A.WORKING_TIME
          ,C.CENT_CODE
          ,C.MAGM_SEQN
          ,C.FRCU_CODE
          ,C.CUST_SUMM
          ,ROW_NUMBER() OVER(ORDER BY A.SHIPWORK_DT, A.WAVE_NO) AS SEQN_NMBR
        FROM (
          SELECT
             A.WAVE_NO 
            ,B.DESCR 
            ,B.SHIPWORK_DT
            ,C.DLVRY_NO
            ,C.DLVRY_NM 
            ,A.SHIPTO_ID 
            ,C.SHIPTO_NM
            ,A.ITEM_CD
            ,D.ITEM_NM
            ,MSFS_WMS.WMF_MSFS_CONV_DS(A.STRR_ID,A.ITEM_CD,SUM(A.PICK_QTY),'STR') AS CONV_TOPICK_QTY 
            ,MSFS_WMS.WMF_MSFS_CONV_TOPK_QTY(A.STRR_ID,A.ITEM_CD,SUM(A.PICK_QTY),'P') AS SUNIT_TOPICK_QTY
            ,MSFS_WMS.WMF_MSFS_CONV_TOPK_QTY(A.STRR_ID,A.ITEM_CD,SUM(A.DONE_QTY),'P') AS SUNIT_DONE_QTY 
            ,D.SHIP_STRG_CD   
            ,A.WH_CD 
            ,D.AM_PRD_YN 
            ,D.CATCH_WEIGHT_YN
            ,A.WORK_UNIT
            ,A.WORK_TYPE
            ,A.ZONE_CD
            ,A.WCELL_NO
            ,C.OUTB_SCD
            ,A.OUTB_NO
            ,A.OUTB_DETL_NO
            ,A.MHE_ID 
            ,A.PICK_A_BOX_YN 
            ,A.CLASS_LV1 
            ,A.CLASS_LV2 
            ,A.SHIPTO_AREA 
            ,E.DESCR AS SHIPTO_AREA_NM 
            ,A.PICKED_YN 
            ,A.WORKER_ID 
            ,A.WORKING_TIME  
          FROM
             MSFS_WMS.WMT_COB_WK_INST A
            ,MSFS_WMS.WMT_COB_WAVE_HR B
            ,MSFS_WMS.WMT_COB_OUTBOUND_HR C
            ,MSFS_WMS.WMT_CMD_ITEM D
            ,MSFS_WMS_ADMIN.ADT_CSY_CODE_SR E
          WHERE B.WH_CD = A.WH_CD
            AND B.WAVE_NO = A.WAVE_NO
            AND C.WH_CD = A.WH_CD
            AND C.OUTB_NO = A.OUTB_NO 
            AND D.STRR_ID = A.STRR_ID 
            AND D.ITEM_CD = A.ITEM_CD 
            AND E.CODE = 'SHIPTO_AREA_TCD'
            AND E.LANGTYPE = 'KO'
            AND E.DETAILCODE = A.SHIPTO_AREA 
            -- AND A.WH_CD = '1000'
            AND B.SHIPWORK_DT BETWEEN '{date}' AND '{date}'
            AND A.TOPICK_YN = 'Y' 
          GROUP BY
             A.WAVE_NO
            ,B.DESCR
            ,B.SHIPWORK_DT
            ,C.DLVRY_NO
            ,C.DLVRY_NM
            ,A.SHIPTO_ID
            ,C.SHIPTO_NM
            ,A.STRR_ID
            ,A.ITEM_CD
            ,D.ITEM_NM
            ,D.SHIP_STRG_CD   
            ,A.WH_CD
            ,D.AM_PRD_YN
            ,D.CATCH_WEIGHT_YN
            ,A.WORK_UNIT
            ,A.WORK_TYPE
            ,A.ZONE_CD
            ,A.WCELL_NO
            ,C.OUTB_SCD
            ,A.OUTB_NO
            ,A.OUTB_DETL_NO
            ,A.MHE_ID
            ,A.PICK_A_BOX_YN
            ,A.CLASS_LV1
            ,A.CLASS_LV2
            ,A.SHIPTO_AREA
            ,E.DESCR
            ,A.PICKED_YN
            ,A.WORKER_ID
            ,A.WORKING_TIME
        ) A
        LEFT JOIN CCUSTCOD C
          ON C.CUST_CODE = A.SHIPTO_ID
        LEFT JOIN MSFS_WMS.TMT_M_ROUTE D
          ON D.ROUTE_CD = A.DLVRY_NO
        """


def _fetch_db_dataframe(
    *,
    date: str,
    db_config_path: str | None = None,
    client_path: str = DEFAULT_DB_CLIENT_PATH,
) -> pd.DataFrame:
    conn = connect_db(db_config_path=db_config_path, client_path=client_path)
    cursor = conn.cursor()
    try:
        cursor.execute(_build_sql(date))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    finally:
        cursor.close()
        conn.close()

    return pd.DataFrame(rows, columns=cols)


def _build_test_mode_dataframe(
    *,
    date: str,
    cent: str,
    class_lv: str | None,
    group_min: int,
    max_SKU: int,
    Das_unit: int,
) -> pd.DataFrame:
    test_config = TestModeConfig(
        date=date,
        cent=cent,
        class_lv=class_lv or "상온",
        group_min=group_min,
        max_sku=max_SKU,
        das_unit=Das_unit,
    )
    records = build_dummy_records(test_config)
    return pd.DataFrame(records, columns=TEST_MODE_REQUIRED_COLUMNS)


def _load_source_dataframe(
    *,
    date: str,
    cent: str,
    class_lv: str | None,
    group_min: int,
    max_SKU: int,
    Das_unit: int,
    test_mode: bool,
    db_config_path: str | None = None,
    client_path: str = DEFAULT_DB_CLIENT_PATH,
) -> pd.DataFrame:
    if test_mode:
        return _build_test_mode_dataframe(
            date=date,
            cent=cent,
            class_lv=class_lv,
            group_min=group_min,
            max_SKU=max_SKU,
            Das_unit=Das_unit,
        )
    return _fetch_db_dataframe(
        date=date,
        db_config_path=db_config_path,
        client_path=client_path,
    )

def summarize_by_shipto(
    df: pd.DataFrame,
    shipto_col: str = "SHIPTO_ID",
    item_col: str = "ITEM_CD",
    working_time_col: str = "WORKING_TIME",
    fixed_cols: tuple[str, ...] = ("WAVE_NO", "DESCR", "CENT_CODE", "MAGM_SEQN", "FRCU_CODE", "SHIPTO_AREA_NM"),
    unique_items: bool = True,
    sort_items: bool = True,
    class_lv_col: str = "CLASS_LV1",
    class_lv: str | None = None,
    shipwork_col: str = "SHIPWORK_DT",
    shipwork_dt: str | None = None,   # 예: "20260115"
    wh_col: str = "WH_CD",
    wh: str | None = None,
    dropna_shipto: bool = True,
) -> pd.DataFrame:
    """
    하루치(또는 필터된) DF를 SHIPTO_ID별로 집계:
      - ITEM_CD: 리스트
      - fixed_cols: first
      - WORKING_TIME: min/max -> STRT_TIME / ENDD_TIME
    """

    # --- optional filtering
    work = df.copy()
    
    if class_lv is not None:
        if class_lv_col not in work.columns:
            raise KeyError(f"class_lv_col '{class_lv_col}' not in df")
        work = work[
            work[class_lv_col]
            .astype(str)
            .str.contains(str(class_lv), regex=False, na=False)
        ]

    if shipwork_dt is not None:
        if shipwork_col not in work.columns:
            raise KeyError(f"shipwork_col '{shipwork_col}' not in df")
        work = work[work[shipwork_col].astype(str) == str(shipwork_dt)]

    if wh is not None:
        if wh_col not in work.columns:
            raise KeyError(f"wh_col '{wh_col}' not in df")
        work = work[work[wh_col].astype(str) == str(wh)]

    # --- column checks
    need_cols = {shipto_col, item_col, working_time_col, *fixed_cols}
    missing = need_cols - set(work.columns)
    if missing:
        raise KeyError(f"입력 df에 필요한 컬럼이 없습니다: {sorted(missing)}")

    if dropna_shipto:
        work = work.dropna(subset=[shipto_col])

    def _items(s: pd.Series) -> list:
        vals = s.dropna().tolist()
        if unique_items:
            # 원 순서 유지 + 중복 제거
            vals = list(dict.fromkeys(vals))
        if sort_items:
            vals = sorted(vals)
        return vals

    agg_dict = {c: "first" for c in fixed_cols}
    agg_dict[item_col] = _items
    agg_dict[working_time_col] = ["min", "max"]

    res = (
        work.groupby(shipto_col, as_index=False)
            .agg(agg_dict)
    )

    # flatten columns (WORKING_TIME min/max)
    if isinstance(res.columns, pd.MultiIndex):
        res.columns = [
            ("STRT_TIME" if (a == working_time_col and b == "min")
             else "ENDD_TIME" if (a == working_time_col and b == "max")
             else a)
            for a, b in res.columns.to_list()
        ]

    return res

# =========================
# Data structures
# =========================

@dataclass
class Cluster:
    """A cluster of rows (SHIPTO_ID set) with item_set union."""
    cid: int
    magm_seqn: str
    shipto_ids: Set[str]
    row_idxs: List[int]
    item_set: Set[str]


# =========================
# Helpers
# =========================

def _ensure_list(x) -> List:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, tuple):
        return list(x)
    # allow a single item
    return [x]


def _build_item_set(df: pd.DataFrame, idxs: Iterable[int], item_col: str) -> Set[str]:
    s: Set[str] = set()
    for v in df.loc[list(idxs), item_col].tolist():
        s.update(map(str, _ensure_list(v)))
    s.discard("nan")
    return s


def _unique_shipto_count(df: pd.DataFrame, idxs: Iterable[int], shipto_col: str) -> int:
    return df.loc[list(idxs), shipto_col].nunique(dropna=True)


def _gain(a: Cluster, b: Cluster) -> int:
    return len(a.item_set & b.item_set)

def _merge_clusters(new_cid: int, a: Cluster, b: Cluster) -> Cluster:
    return Cluster(
        cid=new_cid,
        magm_seqn=a.magm_seqn,
        shipto_ids=a.shipto_ids | b.shipto_ids,
        row_idxs=a.row_idxs + b.row_idxs,
        item_set=a.item_set | b.item_set,
    )


# =========================
# Step 3: merge small groups within same MAGM_SEQN
# =========================

def _merge_small_groups_within_magm(
    df: pd.DataFrame,
    groups: Dict[Tuple[str, str], List[int]],
    shipto_col: str,
    n_min: int,
) -> Tuple[Dict[Tuple[str, str], List[int]], Dict[str, List[int]]]:

    fixed_small: Dict[str, List[int]] = {}

    magm_to_keys: Dict[str, List[Tuple[str, str]]] = {}
    for k in groups.keys():
        magm_to_keys.setdefault(k[0], []).append(k)

    for magm, keys in magm_to_keys.items():
        # 초기 sizes 계산
        sizes: Dict[Tuple[str, str], int] = {}
        for k in keys:
            if k in groups and len(groups[k]) > 0:
                sizes[k] = _unique_shipto_count(df, groups[k], shipto_col)

        while True:
            # 현재 small 키 찾기
            small_keys = [k for k, sz in sizes.items() if sz < n_min and k in groups]
            if not small_keys:
                break

            sk = min(small_keys, key=lambda k: sizes[k])

            # 후보: 같은 MAGM 내 살아있는 키(자기 제외)
            candidates = [k for k in sizes.keys() if k != sk and k in groups and len(groups[k]) > 0]
            if not candidates:
                fixed_small[f"마감 {magm} 차수 {sk[1]} 배송 그룹 (단일 유지)"] = groups.pop(sk)
                sizes.pop(sk, None)
                continue

            same_area = [k for k in candidates if k[1] == sk[1]]
            if same_area:
                tgt = max(same_area, key=lambda k: sizes[k])
            else:
                tgt = max(candidates, key=lambda k: sizes[k])

            # 흡수
            groups[tgt].extend(groups[sk])
            groups.pop(sk)

            # sizes 부분 갱신: sk 제거, tgt만 재계산
            sizes.pop(sk, None)
            sizes[tgt] = _unique_shipto_count(df, groups[tgt], shipto_col)

        # rule 4: 종료 후에도 작은 그룹은 fixed
        for k in list(keys):
            if k in groups and len(groups[k]) > 0:
                sz = _unique_shipto_count(df, groups[k], shipto_col)
                if sz < n_min:
                    fixed_small[f"마감 {magm} 차수 {k[1]} 배송 그룹 (단일 유지)"] = groups.pop(k)
                    sizes.pop(k, None)

    return groups, fixed_small


# =========================
# Step 5: greedy clustering within one (MAGM_SEQN, AREA) group
# =========================

def _greedy_cluster_group(
    df: pd.DataFrame,
    row_idxs: List[int],
    shipto_col: str,
    magm_seqn: str,
    item_col: str,
    unit_size: int = 45,
    allow_zero_cost_merge: bool = True,
    max_item_union_cnt: int | None = None,
    max_modules: int | None = 4,          # ✅ 추가
) -> List[Cluster]:
    """
    Greedy clustering minimizing sum cost(|SHIPTO|).
    Tie-break: maximize item overlap gain(=|item intersection|).
    Hard constraint: merged ITEM union size <= max_item_union_cnt (if provided).

    ✅ 비용(cost)은 '짝수 페어링 점유' 기준:
      - m = ceil(|SHIPTO| / unit_size)
      - m==0 -> 0
      - m==1 -> 1
      - m>=2: odd -> m+1, even -> m

    ✅ 성능 최적화(결과 동일):
      - SKU 상한 검사와 gain 계산에서 교집합(inter)을 1번만 계산/재사용
      - (lenA + lenB) <= max_SKU 이면 교집합 계산 자체를 생략(무조건 통과)
      - ceil을 정수 ceil_div로 대체
    """

    # --- sub build (원본 로직 유지: shipto가 중복으로 들어오는 경우도 안전하게 처리)
    sub = df.loc[row_idxs, [shipto_col, item_col]].copy()
    sub["_idx"] = row_idxs

    shipto_to_idxs: Dict[str, List[int]] = {}
    for shipto, ridx in zip(sub[shipto_col].astype(str).values, sub["_idx"].values):
        shipto_to_idxs.setdefault(shipto, []).append(int(ridx))

    clusters: List[Cluster] = []
    cid = 0
    for shipto, idxs in shipto_to_idxs.items():
        clusters.append(
            Cluster(
                cid=cid,
                magm_seqn=magm_seqn,
                shipto_ids={shipto},
                row_idxs=idxs,
                item_set=_build_item_set(df, idxs, item_col),
            )
        )
        cid += 1

    # --- faster ceil_div
    def _module_cnt(shipto_n: int) -> int:
        if shipto_n <= 0:
            return 0
        return (shipto_n + unit_size - 1) // unit_size

    def cost(shipto_n: int) -> int:
        m = _module_cnt(shipto_n)
        if m <= 0:
            return 0
        if m == 1:
            return 1
        return m if (m % 2 == 0) else (m + 1)

    next_cid = cid

    while True:
        best_pair: Optional[Tuple[int, int]] = None
        best_delta_cost = 10**18
        best_item_gain = -10**18

        m = len(clusters)
        if m <= 1:
            break

        for i in range(m):
            a = clusters[i]
            sa = len(a.shipto_ids)
            ca = cost(sa)
            la = len(a.item_set)

            for j in range(i + 1, m):
                b = clusters[j]
                sb = len(b.shipto_ids)
                cb = cost(sb)
                lb = len(b.item_set)
                            
                merged_size = sa + sb

                # ✅ (A) 모듈 최대값 하드 제약: m > 4이면 병합 금지
                if max_modules is not None:
                    if _module_cnt(merged_size) > max_modules:
                        continue

                # ✅ SKU 상한 체크 + gain 계산을 위한 inter를 "최대한 늦게" 계산
                inter: Optional[int] = None

                if max_item_union_cnt is not None:
                    # la+lb가 이미 상한 이하면 무조건 통과 → inter 계산 불필요
                    if la + lb > max_item_union_cnt:
                        inter = len(a.item_set & b.item_set)
                        union_size = la + lb - inter
                        if union_size > max_item_union_cnt:
                            continue

                merged_size = sa + sb
                cm = cost(merged_size)
                delta_cost = cm - ca - cb

                # tie-break용 gain(교집합 크기)은 inter가 있으면 재사용
                if inter is None:
                    inter = len(a.item_set & b.item_set)
                g = inter  # gain == |A∩B|

                # 목적함수 1차: delta_cost 최소
                if delta_cost < best_delta_cost:
                    best_delta_cost = delta_cost
                    best_pair = (i, j)
                    best_item_gain = g

                elif delta_cost == best_delta_cost and best_pair is not None:
                    # 2차: 아이템 겹침 최대
                    if g > best_item_gain:
                        best_pair = (i, j)
                        best_item_gain = g
                    elif g == best_item_gain:
                        # 3차: 더 큰 shipto 합 선호(기존 로직 유지)
                        pi, pj = best_pair
                        cur = len(clusters[pi].shipto_ids) + len(clusters[pj].shipto_ids)
                        new = merged_size
                        if new > cur:
                            best_pair = (i, j)

        if best_pair is None:
            break

        # 병합 허용 조건(기존 로직 유지)
        if best_delta_cost < 0 or (allow_zero_cost_merge and best_delta_cost == 0):
            i, j = best_pair
            a = clusters[i]
            b = clusters[j]
            merged = _merge_clusters(next_cid, a, b)
            next_cid += 1

            for k in sorted([i, j], reverse=True):
                clusters.pop(k)
            clusters.append(merged)
        else:
            break

    for new_id, c in enumerate(clusters):
        c.cid = new_id
    return clusters

def _areas_str(df: pd.DataFrame, idxs: List[int], area_col: str) -> str:
    areas = (
        df.loc[idxs, area_col]
        .astype(str)
        .dropna()
        .unique()
        .tolist()
    )
    areas = sorted(set(a for a in areas if a != "nan"))
    return "+".join(areas) if areas else ""
# =========================
# Public API
# =========================

def build_final_clusters(
    df: pd.DataFrame,
    n_min: int,
    shipto_col: str = "SHIPTO_ID",
    cent_col: str = "CENT_CODE",
    magm_col: str = "MAGM_SEQN",
    area_col: str = "SHIPTO_AREA_NM",
    item_col: str = "ITEM_CD",
    unit_size: int = 45,
    allow_zero_cost_merge: bool = True,
    max_item_union_cnt: int | None = None,  
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    """
    Returns:
      - result_map_df: 원본 row 단위로 최종 클러스터/그룹 라벨을 부여한 DF
      - cluster_summary_df: 최종 클러스터 요약 DF

    Grouping rules:
      1) (MAGM, AREA) 기준 그룹핑
      2) SHIPTO_ID unique count < N -> 같은 MAGM 내 다른 그룹으로 흡수(반복)
      3) 흡수 후에도 < N -> 단일 그룹으로 유지
      4) 각 그룹별 greedy clustering
    """
    required = {shipto_col, cent_col, magm_col, area_col, item_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    work = df.copy()
    work["_row_idx"] = range(len(work))

    fixed_groups: Dict[str, List[int]] = {}

    # ---- Step 1: base grouping
    groups: Dict[Tuple[str, str], List[int]] = {}
    for (magm, area), gdf in work.groupby([magm_col, area_col], dropna=False):
        key = (str(magm), str(area))
        groups[key] = gdf["_row_idx"].tolist()

    # ---- Step 2 & 3: merge small groups within same MAGM
    groups, fixed_small = _merge_small_groups_within_magm(
        df=work,
        groups=groups,
        shipto_col=shipto_col,
        n_min=n_min,
    )
    fixed_groups.update(fixed_small)

    # ---- Step 4: clustering per remaining group
    all_assignments: List[Dict] = []
    cluster_summaries: List[Dict] = []

    # fixed groups -> assign a single cluster per fixed group
    fixed_cluster_global_id = 0
    for gname, idxs in fixed_groups.items():
        shipto_cnt = work.loc[idxs, shipto_col].nunique(dropna=True)
        item_set = _build_item_set(work, idxs, item_col)
        magm_vals = work.loc[idxs, magm_col].astype(str).unique().tolist()
        area_vals = work.loc[idxs, area_col].astype(str).unique().tolist()

        cluster_id = f"F{fixed_cluster_global_id}"
        fixed_cluster_global_id += 1

        for ridx in idxs:
            all_assignments.append(
                {
                    "_row_idx": ridx,
                    "FINAL_GROUP_TYPE": "FIXED",
                    "FINAL_GROUP_KEY": gname,
                    "FINAL_CLUSTER_ID": cluster_id,
                }
            )

        cluster_summaries.append(
            {
                "FINAL_CLUSTER_ID": cluster_id,
                "FINAL_GROUP_TYPE": "FIXED",
                "FINAL_GROUP_KEY": gname,
                "MAGM_SEQN": ",".join(sorted(set(magm_vals))),
                "SHIPTO_AREA_NM": ",".join(sorted(set(area_vals))),
                "SHIPTO_CNT": int(shipto_cnt),
                "ITEM_UNION_CNT": int(len(item_set)),
            }
        )

    # clustered groups
    clustered_cluster_global_id = 0
    for (magm, area), idxs in groups.items():
        clusters = _greedy_cluster_group(
            df=work,
            row_idxs=idxs,
            shipto_col=shipto_col,
            magm_seqn=magm,
            item_col=item_col,
            unit_size=unit_size,
            allow_zero_cost_merge=allow_zero_cost_merge,
            max_item_union_cnt=max_item_union_cnt,
            max_modules=4
        )
    
        # (선택) 보기 좋게 큰 클러스터부터 그룹_1 부여
        clusters = sorted(clusters, key=lambda c: len(c.shipto_ids), reverse=True)
    
        # ✅ AREA 표시는 group의 area(원래 값) 대신, 실제 클러스터에 포함된 권역 조합으로 출력
        #    기존 _areas_str()를 그대로 사용
        for k, c in enumerate(clusters, start=1):
            cluster_id = f"C{clustered_cluster_global_id}"
            clustered_cluster_global_id += 1
    
            area_key = _areas_str(work, c.row_idxs, area_col) or str(area) or "권역미상"
    
            # ✅ 요청하신 포맷
            group_name = f"마감 {magm} 차수 {area_key} 배송 그룹_{k} (클러스터링)"
    
            for ridx in c.row_idxs:
                all_assignments.append(
                    {
                        "_row_idx": ridx,
                        "FINAL_GROUP_TYPE": "CLUSTERED",
                        "FINAL_GROUP_KEY": group_name,     # ✅ 사람이 읽는 이름으로
                        "FINAL_CLUSTER_ID": cluster_id,
                    }
                )
    
            cluster_summaries.append(
                {
                    "FINAL_CLUSTER_ID": cluster_id,
                    "FINAL_GROUP_TYPE": "CLUSTERED",
                    "FINAL_GROUP_KEY": group_name,       # ✅ 사람이 읽는 이름으로
                    "MAGM_SEQN": magm,
                    "SHIPTO_AREA_NM": area_key,
                    "SHIPTO_CNT": int(len(c.shipto_ids)),
                    "ITEM_UNION_CNT": int(len(c.item_set)),
                }
            )
            
    # ---- Build outputs
    assign_df = pd.DataFrame(all_assignments)
    result_map_df = work.merge(assign_df, on="_row_idx", how="left").drop(columns=["_row_idx"])
    cluster_summary_df = pd.DataFrame(cluster_summaries).sort_values(
        ["FINAL_GROUP_TYPE", "MAGM_SEQN", "SHIPTO_AREA_NM", "SHIPTO_CNT"],
        ascending=[True, True, True, False],
    )

    return result_map_df, cluster_summary_df

def assign_rotations_by_modules(
    summary_df: pd.DataFrame,
    shipto_cnt_col: str = "SHIPTO_CNT",
    magm_col: str = "MAGM_SEQN",
    area_col: str = "SHIPTO_AREA_NM",
    group_key_col: str = "FINAL_GROUP_KEY",
    cluster_id_col: str = "FINAL_CLUSTER_ID",
    unit_size: int = 45,
    crossing_row_to_rot1: bool = True,  # ✅ 핵심 옵션
) -> tuple[pd.DataFrame, dict]:
    """
    - modules = ceil(SHIPTO_CNT / unit_size)
    - 우선순위 정렬(MAGM 1>2>3, 그 안에서 AREA 지방>수도권, 그 다음 MODULE 큰 것)
    - 정렬된 순서대로 1회전에 채우다가 절반(half)을 넘는 시점에서 2회전으로 전환.

    crossing_row_to_rot1:
      - True : 절반 초과를 '유발한 행'도 1회전에 포함, 그 다음부터 2회전
      - False: 절반 초과를 '유발한 행'은 2회전에 배치, 그 시점부터 2회전
    """

    if unit_size <= 0:
        raise ValueError(f"unit_size must be positive, got {unit_size}")

    df = summary_df.copy()
    for c in [shipto_cnt_col, magm_col, area_col, group_key_col, cluster_id_col]:
        if c not in df.columns:
            raise ValueError(f"summary_df must have column: {c}")

    # --- module count (robust)
    shipto_cnt = pd.to_numeric(df[shipto_cnt_col], errors="coerce").fillna(0)
    shipto_cnt = shipto_cnt.clip(lower=0).astype(int)

    def ceil_div(a: int, b: int) -> int:
        return (a + b - 1) // b

    df["MODULE_CNT"] = shipto_cnt.map(lambda x: ceil_div(int(x), unit_size))
    df["ROTATION"] = pd.NA
    remain = df.copy()

    # --- priority mapping
    magm_rank = {"1": 0, "2": 1, "3": 2}

    def area_rank(area: str) -> int:
        a = str(area)
        if a == "지방":
            return 0
        if a == "수도권":
            return 1
        return 9

    if not remain.empty:
        remain["_MAGM_R"] = remain[magm_col].astype(str).map(lambda x: magm_rank.get(x, 9))
        remain["_AREA_R"] = remain[area_col].astype(str).map(area_rank)

        remain = remain.sort_values(
            by=["_MAGM_R", "_AREA_R", "MODULE_CNT"],
            ascending=[True, True, False],
            kind="mergesort",
        )

    # --- half threshold
    total_mod = int(df["MODULE_CNT"].sum())
    half = total_mod / 2.0

    mod_1 = float(df.loc[df["ROTATION"].eq("1회전"), "MODULE_CNT"].sum())
    crossed = False

    for idx, row in remain.iterrows():
        m = float(row["MODULE_CNT"])

        if crossed:
            df.at[idx, "ROTATION"] = "2회전"
            continue

        # crossed == False 상태에서 이번 행을 어디에 둘지 판단
        if crossing_row_to_rot1:
            # ✅ 이번 행을 먼저 1회전에 넣고, 넣은 뒤 초과되면 다음부터 2회전
            df.at[idx, "ROTATION"] = "1회전"
            mod_1 += m
            if mod_1 > half:
                crossed = True
        else:
            # ✅ 이번 행을 1회전에 넣으면 초과되는지 "사전" 판단
            if (mod_1 + m) > half:
                # 초과 유발 행은 2회전으로 보내고, 지금부터 2회전 고정
                df.at[idx, "ROTATION"] = "2회전"
                crossed = True
            else:
                df.at[idx, "ROTATION"] = "1회전"
                mod_1 += m

    # cleanup
    for c in ["_MAGM_R", "_AREA_R"]:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)

    if df["ROTATION"].isna().any():
        raise RuntimeError("ROTATION assignment incomplete: some rows remain unassigned.")

    out = {
        "TOTAL_MODULES": total_mod,
        "HALF_THRESHOLD": float(half),
        "CROSSING_ROW_TO_ROT1": bool(crossing_row_to_rot1),
        "ROT1_MODULES": int(df.loc[df["ROTATION"].eq("1회전"), "MODULE_CNT"].sum()),
        "ROT2_MODULES": int(df.loc[df["ROTATION"].eq("2회전"), "MODULE_CNT"].sum()),
        "DIFF_MODULES": int(
            abs(
                df.loc[df["ROTATION"].eq("1회전"), "MODULE_CNT"].sum()
                - df.loc[df["ROTATION"].eq("2회전"), "MODULE_CNT"].sum()
            )
        ),
    }

    return df, out

def _prepare_main_das_dataframe(
    das_df: pd.DataFrame,
    *,
    date: str,
    cent: str,
    CLASS_LV: str | None,
) -> pd.DataFrame:
    if das_df.empty:
        raise NoSqlRowsError(
            f"조회 결과가 없습니다. 작업일자({date}) 또는 센터코드({cent})를 확인해 주세요."
        )

    work = das_df.copy()

    main_das = work[work['MHE_ID'] == 'DAS']
    main_das = main_das[
        [
            'SHIPWORK_DT', 'WH_CD', 'CLASS_LV1', 'WAVE_NO', 'DESCR', 'DLVRY_NO', 'DLVRY_NM',
            'ROUTE_DELI_SEQN', 'CENT_CODE', 'MAGM_SEQN', 'CUST_SUMM', 'FRCU_CODE', 'SHIPTO_ID',
            'SHIPTO_NM', 'ITEM_CD', 'ITEM_NM', 'CONV_TOPICK_QTY',
            'SUNIT_TOPICK_QTY', 'SHIP_STRG_CD', 'SHIPTO_AREA_NM', 'WORKING_TIME'
        ]
    ].copy()

    daily_das_by_cent = summarize_by_shipto(
        main_das,
        shipto_col='SHIPTO_ID',
        item_col='ITEM_CD',
        working_time_col='WORKING_TIME',
        fixed_cols=(
            'WAVE_NO', 'DESCR', 'CENT_CODE', 'MAGM_SEQN', 'FRCU_CODE', 'SHIPTO_NM',
            'SHIPTO_AREA_NM', 'DLVRY_NO', 'ROUTE_DELI_SEQN',
        ),
        unique_items=True,
        sort_items=True,
        class_lv_col='CLASS_LV1',
        class_lv=CLASS_LV,
        shipwork_col='SHIPWORK_DT',
        shipwork_dt=date,
        wh_col='WH_CD',
        wh=cent,
    )

    if daily_das_by_cent.empty:
        raise NoCentRowsAfterSummarizeError(
            f"해당 센터코드({cent})에 대한 작업 내역이 없습니다. 센터코드를 확인해 주세요."
        )

    return daily_das_by_cent



def _export_cluster_result(
    result_df: pd.DataFrame,
    summary_df2: pd.DataFrame,
    *,
    CLASS_LV: str | None,
    cent: str,
    date: str,
    Das_unit: int,
    out_xlsx: Path | None,
    test_mode: bool,
) -> Path:
    prefix = f"[{CLASS_LV}]" if CLASS_LV is not None else ''
    result_df = result_df.copy()
    summary_df2 = summary_df2.copy()
    result_df['FINAL_GROUP_KEY'] = prefix + result_df['FINAL_GROUP_KEY'].astype(str)
    summary_df2['FINAL_GROUP_KEY'] = prefix + summary_df2['FINAL_GROUP_KEY'].astype(str)

    cluster_item_sum = (
        result_df.groupby('FINAL_CLUSTER_ID')['ITEM_CD']
        .apply(lambda s: int(sum(len(v) if isinstance(v, list) else len(_ensure_list(v)) for v in s)))
        .reset_index(name='TOTAL_ITEM_NUM')
    )

    summary_df2 = summary_df2.merge(cluster_item_sum, on='FINAL_CLUSTER_ID', how='left')
    summary_df2['TOTAL_ITEM_NUM'] = summary_df2['TOTAL_ITEM_NUM'].fillna(0).astype(int)

    result_df_ = result_df.rename(columns={
        'SHIPTO_ID': '매장코드',
        'DESCR': '기존웨이브명',
        'SHIPTO_NM': '매장명',
        'CENT_CODE': '출고센터',
        'MAGM_SEQN': '마감차수',
        'SHIPTO_AREA_NM': '권역구분',
        'FRCU_CODE': 'FC코드',
        'DLVRY_NO': '노선코드',
        'ROUTE_DELI_SEQN': '노선배송차수',
        'ITEM_NUM': '주문상품수',
        'ITEM_CD': '주문상품리스트',
        'FINAL_GROUP_KEY': '그룹명',
        'FINAL_CLUSTER_ID': '그룹ID',
    })
    result_df_ = result_df_[
        [
            '기존웨이브명', 'FC코드', '출고센터', '매장코드', '매장명', '마감차수', '권역구분',
            '노선코드', '노선배송차수', '주문상품리스트', '그룹ID', '그룹명'
        ]
    ]
    result_df_ = result_df_.assign(
        주문상품수=result_df_['주문상품리스트'].map(
            lambda x: len(x) if isinstance(x, list) else len(_ensure_list(x))
        )
    )
    result_df_ = result_df_[
        [
            '기존웨이브명', 'FC코드', '출고센터', '매장코드', '매장명', '마감차수', '권역구분',
            '노선코드', '노선배송차수', '주문상품수', '주문상품리스트', '그룹ID', '그룹명'
        ]
    ]

    summary_df2_ = summary_df2.sort_values(by=['ROTATION', 'FINAL_CLUSTER_ID'], kind='mergesort')
    summary_df2_ = summary_df2_.rename(columns={
        'FINAL_CLUSTER_ID': '그룹ID',
        'FINAL_GROUP_KEY': '그룹명',
        'MAGM_SEQN': '마감차수',
        'SHIPTO_AREA_NM': '권역구분',
        'SHIPTO_CNT': '그룹매장수',
        'ITEM_UNION_CNT': '고유SKU수',
        'MODULE_CNT': f'DAS 모듈수({Das_unit}매장단위)',
        'TOTAL_ITEM_NUM': '총작업수',
        'ROTATION': '회전구분',
    })
    summary_df2_ = summary_df2_[
        ['그룹ID', '그룹명', '마감차수', '권역구분', '그룹매장수', '고유SKU수', f'DAS 모듈수({Das_unit}매장단위)', '총작업수', '회전구분']
    ]

    if out_xlsx is None:
        file_prefix = '[TEST]' if test_mode else ''
        class_prefix = f'[{CLASS_LV}]' if CLASS_LV is not None else ''
        out_xlsx = Path('./data') / f"{file_prefix}{class_prefix}클러스터링 결과_{cent}_{date}.xlsx"
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_xlsx, engine='openpyxl') as writer:
        result_df_.to_excel(writer, sheet_name='매장별그룹배치결과', index=False)
        summary_df2_.to_excel(writer, sheet_name='그룹요약', index=False)

    return out_xlsx



def main(
    date: str,
    cent: str,
    CLASS_LV: str | None = None,
    group_min: int = 30,
    max_SKU: int = 300,
    Das_unit: int = 45,
    out_xlsx: Path | None = None,
    *,
    test_mode: bool = False,
    db_config_path: str | None = None,
    client_path: str = DEFAULT_DB_CLIENT_PATH,
):
    das_df = _load_source_dataframe(
        date=date,
        cent=cent,
        class_lv=CLASS_LV,
        group_min=group_min,
        max_SKU=max_SKU,
        Das_unit=Das_unit,
        test_mode=test_mode,
        db_config_path=db_config_path,
        client_path=client_path,
    )

    daily_das_by_cent = _prepare_main_das_dataframe(
        das_df,
        date=date,
        cent=cent,
        CLASS_LV=CLASS_LV,
    )

    result_df, summary_df = build_final_clusters(
        df=daily_das_by_cent,
        n_min=group_min,
        max_item_union_cnt=max_SKU,
        unit_size=Das_unit,
        shipto_col='SHIPTO_ID',
        cent_col='CENT_CODE',
        magm_col='MAGM_SEQN',
        area_col='SHIPTO_AREA_NM',
        item_col='ITEM_CD',
        allow_zero_cost_merge=True,
    )

    summary_df2, _rot_info = assign_rotations_by_modules(
        summary_df,
        shipto_cnt_col='SHIPTO_CNT',
        magm_col='MAGM_SEQN',
        area_col='SHIPTO_AREA_NM',
        group_key_col='FINAL_GROUP_KEY',
        cluster_id_col='FINAL_CLUSTER_ID',
        unit_size=Das_unit,
        crossing_row_to_rot1=False,
    )

    return _export_cluster_result(
        result_df,
        summary_df2,
        CLASS_LV=CLASS_LV,
        cent=cent,
        date=date,
        Das_unit=Das_unit,
        out_xlsx=out_xlsx,
        test_mode=test_mode,
    )



def cli() -> None:
    parser = argparse.ArgumentParser(description='Clustering_Logic')
    parser.add_argument('--date', required=True, type=str, help='YYYYMMDD')
    parser.add_argument('--cent', required=True, type=str, help='CENT_CODE')
    parser.add_argument('--CLASS_LV', type=str, required=True, help='온도구분(상온/냉장/냉동)')
    parser.add_argument('--group_min', type=int, required=True)
    parser.add_argument('--max_SKU', type=int, required=True)
    parser.add_argument('--Das_unit', type=int, required=True)
    parser.add_argument('--test_mode', action='store_true', help='DB 대신 더미 데이터를 사용합니다.')
    parser.add_argument('--db_config_path', type=str, default=None)
    parser.add_argument('--client_path', type=str, default=DEFAULT_DB_CLIENT_PATH)

    args = parser.parse_args()

    main(
        date=args.date,
        cent=args.cent,
        CLASS_LV=args.CLASS_LV,
        group_min=args.group_min,
        max_SKU=args.max_SKU,
        Das_unit=args.Das_unit,
        test_mode=args.test_mode,
        db_config_path=args.db_config_path,
        client_path=args.client_path,
    )


if __name__ == "__main__":
    cli()
