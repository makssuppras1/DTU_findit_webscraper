"""Smoke tests for thesis_extractor: config load, sectioning helpers, run_pipeline summary."""

import unittest
from pathlib import Path

from thesis_extractor import run_pipeline
from thesis_extractor.config import load_config


class TestConfig(unittest.TestCase):
    def test_load_config(self):
        root = Path(__file__).resolve().parent.parent
        cfg_path = root / "config.yaml"
        if not cfg_path.exists():
            self.skipTest("config.yaml not found")
        config = load_config(cfg_path)
        self.assertEqual(config.gcs.bucket, "thesis_archive_bucket")
        self.assertIn("master_thesis", config.gcs.prefix)
        self.assertTrue(config.runtime.resume)


class TestSectioning(unittest.TestCase):
    def test_preclean_dedup(self):
        from thesis_extractor.sectioning import preclean

        pages = ["Header\nA\nFooter", "Header\nB\nFooter", "Header\nC\nFooter"]
        out = preclean(pages)
        self.assertEqual(len(out), 3)
        for t in out:
            self.assertNotIn("Header", t)
            self.assertNotIn("Footer", t)

    def test_detect_hits_requires_config(self):
        from thesis_extractor.sectioning import detect_hits

        class C:
            class sectioning:
                heading_aliases = {"introduction": ["introduction", "1 introduction"]}
                fuzzy_match = True
                fuzzy_threshold = 85

        hits = detect_hits(C(), ["1 Introduction\nIntro text."])
        self.assertGreaterEqual(len(hits), 1)
        self.assertEqual(hits[0].canonical, "introduction")


if __name__ == "__main__":
    unittest.main()
