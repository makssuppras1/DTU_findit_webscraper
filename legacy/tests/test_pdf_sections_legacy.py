"""Unit tests for legacy pdf_section_extractor (heading detection, normalization, assemble_sections)."""

import sys
import unittest
from pathlib import Path

_legacy = Path(__file__).resolve().parent.parent
if str(_legacy) not in sys.path:
    sys.path.insert(0, str(_legacy))

from pdf_section_extractor.extractor import PageText
from pdf_section_extractor.sections import (
    NORMALIZE_MAP,
    CONFIDENCE_THRESHOLD,
    remove_headers_footers,
    extract_sections,
    assemble_sections,
    _normalize_heading,
    normalize_section_name,
    _is_heading_line,
    _detect_repeated_lines,
    HeadingHit,
)


class TestNormalization(unittest.TestCase):
    def test_exact_match(self):
        self.assertEqual(_normalize_heading("abstract"), ("abstract", "abstract"))
        self.assertEqual(_normalize_heading("Introduction"), ("introduction", "Introduction"))
        self.assertEqual(_normalize_heading("METHODOLOGY"), ("methodology", "METHODOLOGY"))

    def test_confidence_returned(self):
        canonical, conf = normalize_section_name("abstract")
        self.assertEqual(canonical, "abstract")
        self.assertEqual(conf, 100.0)
        canonical, conf = normalize_section_name("Introduction")
        self.assertGreaterEqual(conf, CONFIDENCE_THRESHOLD)

    def test_unknown_preserved(self):
        norm, orig = _normalize_heading("Appendix")
        self.assertIsNone(norm)
        self.assertEqual(orig, "Appendix")

    def test_map_contains_expected(self):
        self.assertIn("abstract", NORMALIZE_MAP)
        self.assertIn("conclusion", NORMALIZE_MAP)
        self.assertEqual(NORMALIZE_MAP["related work"], "related_work")


class TestHeadingDetection(unittest.TestCase):
    def test_numbered(self):
        self.assertTrue(_is_heading_line("1 Introduction"))
        self.assertTrue(_is_heading_line("1.1 Background"))
        self.assertTrue(_is_heading_line("2. Methodology"))

    def test_chapter(self):
        self.assertTrue(_is_heading_line("Chapter 1"))
        self.assertTrue(_is_heading_line("Chapter 2: Methods"))

    def test_all_caps(self):
        self.assertTrue(_is_heading_line("INTRODUCTION"))
        self.assertFalse(_is_heading_line("AB"))
        self.assertFalse(_is_heading_line("Just a normal sentence."))


class TestHeaderFooterRemoval(unittest.TestCase):
    def test_repeated_detected(self):
        texts = ["Header\nPage one\nFooter", "Header\nPage two\nFooter", "Header\nPage three\nFooter"]
        repeated = _detect_repeated_lines(texts)
        self.assertIn("Header", repeated)
        self.assertIn("Footer", repeated)

    def test_removal(self):
        pages = [
            PageText(0, "Header\nContent A\nFooter", "pdf_text", 20, 0.9),
            PageText(1, "Header\nContent B\nFooter", "pdf_text", 20, 0.9),
        ]
        out = remove_headers_footers(pages)
        self.assertEqual(len(out), 2)
        self.assertNotIn("Header", out[0].text)
        self.assertIn("Content A", out[0].text)


class TestExtractSections(unittest.TestCase):
    def test_sections_and_unknown(self):
        pages = [
            PageText(0, "1 Introduction\nThis is intro.", "pdf_text", 50, 0.9),
            PageText(1, "2 Related Work\nSome related work.", "pdf_text", 50, 0.9),
            PageText(2, "3 Appendix\nSome appendix.", "pdf_text", 50, 0.9),
            PageText(3, "4 Conclusion\nDone.", "pdf_text", 30, 0.9),
        ]
        known, unknown = extract_sections(pages)
        canonicals = [s.canonical_name for s in known]
        self.assertIn("introduction", canonicals)
        self.assertIn("related_work", canonicals)
        self.assertIn("conclusion", canonicals)
        unknown_headings = [s.raw_headings[0] if s.raw_headings else "" for s in unknown]
        self.assertIn("3 Appendix", unknown_headings)

    def test_multi_page_section_concatenated(self):
        pages = [
            PageText(0, "1 Introduction\nIntro page one.", "pdf_text", 50, 0.9),
            PageText(1, "More intro on page two.", "pdf_text", 50, 0.9),
            PageText(2, "2 Method\nMethods here.", "pdf_text", 50, 0.9),
        ]
        known, unknown = extract_sections(pages)
        intro = next((s for s in known if s.canonical_name == "introduction"), None)
        self.assertIsNotNone(intro)
        self.assertEqual(intro.start_page, 0)
        self.assertEqual(intro.end_page, 1)
        self.assertIn("Intro page one", intro.text)
        self.assertIn("More intro on page two", intro.text)


class TestAssembleSections(unittest.TestCase):
    def test_multi_page_span(self):
        page_texts = [
            "1 Introduction\nFirst page of intro.",
            "Second page of intro content.",
            "2 Methods\nMethods section.",
        ]
        hits = [
            HeadingHit(0, "1 Introduction", "introduction", 100.0),
            HeadingHit(2, "2 Methods", "methodology", 100.0),
        ]
        known, unknown = assemble_sections(page_texts, hits, add_abstract_heuristic=False)
        self.assertEqual(len(known), 2)
        intro = next(s for s in known if s.canonical_name == "introduction")
        self.assertEqual(intro.start_page, 0)
        self.assertEqual(intro.end_page, 1)
        self.assertIn("First page of intro", intro.text)
        self.assertIn("Second page of intro", intro.text)

    def test_same_canonical_merged(self):
        page_texts = [
            "6 Conclusion\nFirst conclusion part.",
            "7 Concluding Remarks\nSecond part.",
            "8 References\nRefs.",
        ]
        hits = [
            HeadingHit(0, "6 Conclusion", "conclusion", 100.0),
            HeadingHit(1, "7 Concluding Remarks", "conclusion", 90.0),
            HeadingHit(2, "8 References", "references", 100.0),
        ]
        known, unknown = assemble_sections(page_texts, hits, add_abstract_heuristic=False)
        conclusion = next((s for s in known if s.canonical_name == "conclusion"), None)
        self.assertIsNotNone(conclusion)
        self.assertEqual(len(conclusion.raw_headings), 2)
        self.assertIn("6 Conclusion", conclusion.raw_headings)
        self.assertIn("7 Concluding Remarks", conclusion.raw_headings)
        self.assertEqual(conclusion.start_page, 0)
        self.assertEqual(conclusion.end_page, 1)


if __name__ == "__main__":
    unittest.main()
