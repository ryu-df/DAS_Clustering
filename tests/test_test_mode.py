import unittest
from pathlib import Path

from das_runtime import (
    BLURRED_API_BUILD_CONFIG,
    DEFAULT_TEST_MODE_CONFIG,
    TEST_MODE_REQUIRED_COLUMNS,
    TestModeConfig,
    build_dummy_records,
)


class TestModeConfigTests(unittest.TestCase):
    def test_default_output_path_marks_test_mode(self):
        config = TestModeConfig(
            date="20260131",
            cent="2000",
            class_lv="냉장",
            out_dir=Path("tmp-output"),
        )

        out_path = config.default_output_path()

        self.assertEqual(
            out_path,
            Path("tmp-output") / "[TEST][냉장]클러스터링 결과_2000_20260131.xlsx",
        )

    def test_to_main_kwargs_enables_test_mode(self):
        kwargs = DEFAULT_TEST_MODE_CONFIG.to_main_kwargs()

        self.assertTrue(kwargs["test_mode"])
        self.assertEqual(kwargs["cent"], DEFAULT_TEST_MODE_CONFIG.cent)
        self.assertEqual(kwargs["out_xlsx"], DEFAULT_TEST_MODE_CONFIG.default_output_path())


class DummyRecordTests(unittest.TestCase):
    def test_dummy_records_follow_simplified_area_and_magm_rules(self):
        records = build_dummy_records()

        self.assertGreater(len(records), 0)
        self.assertFalse(set(TEST_MODE_REQUIRED_COLUMNS) - set(records[0].keys()))
        self.assertEqual({row["MAGM_SEQN"] for row in records}, {"1", "2", "3"})
        self.assertEqual({row["SHIPTO_AREA_NM"] for row in records}, {"수도권", "지방"})
        self.assertFalse(any("제주" in str(row["SHIPTO_AREA_NM"]) for row in records))
        self.assertFalse(any("문앞" in str(row["DESCR"]) for row in records))
        self.assertFalse(any(row["FRCU_CODE"] == "10494" for row in records))


class ApiBlurConfigTests(unittest.TestCase):
    def test_api_build_config_is_blurred(self):
        self.assertEqual(
            BLURRED_API_BUILD_CONFIG,
            {
                "entrypoint": "app:app",
                "host": "<blurred>",
                "port": "<blurred>",
                "reload": "<blurred>",
            },
        )


if __name__ == "__main__":
    unittest.main()
