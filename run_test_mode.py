from pathlib import Path

from Das_Clustering_repair import main
from das_runtime import TestModeConfig

# 테스트 모드에서 이 변수들만 바꿔서 실행하면 됩니다.
TEST_MODE = TestModeConfig(
    date="20260115",
    cent="1000",
    class_lv="상온",
    group_min=30,
    max_sku=300,
    das_unit=45,
    out_dir=Path("data") / "test_mode",
)


if __name__ == "__main__":
    output_path = main(**TEST_MODE.to_main_kwargs())
    print(f"[TEST MODE] 결과 파일 생성: {output_path}")
