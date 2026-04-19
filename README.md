# DAS Cluster

DAS 작업 데이터를 매장 단위로 종합 후 매장을 모듈 단위로 클러스터링

---

## 알고리즘 목적

비슷한 주문 상품 구성을 가진 매장들을 묶어 모듈 단위로 작업 효율이 높은 그룹을 구성하는 것

---

## 처리 흐름

1. **데이터 로드**
   - DB 조회 또는 실행 입력값 기준으로 원천 데이터를 로드

2. **매장 단위 요약**
   - 주문 데이터를 매장 기준으로 종합
   - 매장별 주문상품 리스트와 작업 시간 범위를 정리

3. **기본 그룹 구성**
   - 매장을 `마감차수` + `권역` 기준으로 사전 분할

4. **소규모 그룹 병합**
   - 너무 수가 적은 사전 분할 그룹은 다른 그룹과 병합

5. **클러스터링**
   - 각 그룹별로 주문상품이 많이 겹치는 순으로 병합 진행
   - 제약조건을 만족하는 최소 그룹수를 만족할 떄까지 병합

6. **회전 배치**
   - 최종 그룹별 모듈 수를 계산
   - 마감차수와 권역 우선순위에 따라 1회전 / 2회전 분할

7. **엑셀 출력**
   - 매장별 그룹 배치 결과
   - 그룹 요약 결과

---

## 디렉토리 구조

```text
.
├── Das_Clustering_repair.py   # 실제 클러스터링 엔진
├── das_runtime.py             # 실행 설정 / 입력 데이터 준비 코드
├── run_test_mode.py           # 설정값 수정 후 바로 실행하는 스크립트
├── app.py                     # API 실행 코드
├── tests/
│   ├── test_db_config.py
│   └── test_test_mode.py
├── data/                      # 결과 엑셀 출력 폴더
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 실행 환경

- Python **3.10 ~ 3.11** 권장
- 가상환경 사용 권장

```bash
mamba create -n das python=3.10 -y
mamba activate das
pip install -r requirements.txt
```

---

## 실행 방법

### 1) 설정값 수정 후 실행

`run_test_mode.py` 상단 설정값을 수정한 뒤 실행

```python
TEST_MODE = TestModeConfig(
    date="20260115",
    cent="1000",
    class_lv="상온",
    group_min=30,
    max_sku=300,
    das_unit=45,
    out_dir=Path("data") / "test_mode",
)
```

실행:

```bash
python run_test_mode.py
```

---

### 2) CLI 인자 방식 실행

```bash
python Das_Clustering_repair.py \
  --date 20260115 \
  --cent 1000 \
  --CLASS_LV 상온 \
  --group_min 30 \
  --max_SKU 300 \
  --Das_unit 45 \
  --test_mode
```

주요 인자:

- `--date`: 작업일자 (`YYYYMMDD`)
- `--cent`: 센터 코드
- `--CLASS_LV`: 온도구분 (`상온`, `냉장`, `냉동`)
- `--group_min`: 최소 그룹 매장 수
- `--max_SKU`: 그룹 당 최대 고유 SKU 수
- `--Das_unit`: DAS 모듈 단위 매장 수
- `--test_mode`: 내부 실행 데이터 사용

---

## 출력 결과

실행이 완료되면 엑셀 파일이 생성

기본 시트 구성:

- `매장별그룹배치결과`
- `그룹요약`

주요 결과 내용:

- 매장별 그룹 배치
- 그룹별 매장 수
- 그룹별 고유 SKU 수
- 그룹별 모듈 수
- 회전 구분

