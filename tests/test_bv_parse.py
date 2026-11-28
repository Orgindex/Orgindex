"""Regression cases for values observed in Bolagsverket's real filings."""
import base64
import gzip
import hashlib
import sys
import unittest
import zipfile
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from bv_parse import ParseError, parse_archive


def archive(facts, extra="", *, nested_layers=0, attachments=()):
    document = f'''<html xmlns="http://www.w3.org/1999/xhtml"
        xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
        xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:iso4217="urn:currency">
      <xbrli:context id="period"><xbrli:entity><xbrli:identifier scheme="urn:org">556976-5794</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate><xbrli:endDate>2025-12-31</xbrli:endDate></xbrli:period></xbrli:context>
      <xbrli:context id="balance"><xbrli:entity><xbrli:identifier scheme="urn:org">556976-5794</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period></xbrli:context>
      <xbrli:unit id="SEK"><xbrli:measure>iso4217:SEK</xbrli:measure></xbrli:unit>
      <ix:nonNumeric name="se-cd:ForetagetsNamn" contextRef="period">Reporting company AB</ix:nonNumeric>
      {facts}{extra}</html>'''
    inner = BytesIO()
    with zipfile.ZipFile(inner, "w", zipfile.ZIP_DEFLATED) as target:
        target.writestr("filing.xhtml", document)
        for name, content in attachments:
            target.writestr(name, content)
    for _ in range(nested_layers):
        wrapper = BytesIO()
        with zipfile.ZipFile(wrapper, "w", zipfile.ZIP_DEFLATED) as target:
            target.writestr("package.zip", inner.getvalue())
        inner = wrapper
    outer = BytesIO()
    with zipfile.ZipFile(outer, "w", zipfile.ZIP_DEFLATED) as target:
        target.writestr("5569765794_2025-12-31.zip", inner.getvalue())
    outer.seek(0)
    return outer


def fact(name, value, *, decimals="0", scale="0", context="period", **attrs):
    attrs = " ".join(f'{key}="{value}"' for key, value in attrs.items())
    return (f'<ix:nonFraction name="se-gen:{name}" contextRef="{context}" '
            f'unitRef="SEK" decimals="{decimals}" scale="{scale}" '
            f'format="ixt:numspacecomma" {attrs}>{value}</ix:nonFraction>')


class ParserRegressionTests(unittest.TestCase):
    def test_precise_sek_wins_over_rounded_summary_thousands(self):
        result = parse_archive(archive(
            fact("Nettoomsattning", "1 382", decimals="-3", scale="3")
            + fact("Nettoomsattning", "1 382 107")
            + fact("AretsResultat", "21 783")))
        self.assertEqual(result["rows"][0]["net_revenue"], Decimal("1382107"))
        self.assertEqual(result["report_count"], 1)
        self.assertEqual(result["skipped"], [])

    def test_employee_concept_and_decimal_comma(self):
        employee = fact("MedelantaletAnstallda", "1,5", decimals="2").replace("ixt:numspacecomma", "ixt:numcomma")
        result = parse_archive(archive(fact("Nettoomsattning", "100") + employee))
        self.assertEqual(result["rows"][0]["employees"], Decimal("1.5"))

    def test_signed_scaled_amount(self):
        result = parse_archive(archive(fact("AretsResultat", "123", scale="3", sign="-")))
        self.assertEqual(result["rows"][0]["net_profit"], Decimal("-123000"))

    def test_equal_precision_conflict_is_null_and_source_retained(self):
        result = parse_archive(archive(fact("Nettoomsattning", "100")
            + fact("EgetKapital", "1000", context="balance")
            + fact("EgetKapital", "1001", context="balance")))
        self.assertIsNone(result["rows"][0]["equity"])
        self.assertEqual(result["conflicts"][0]["values"], ["1000", "1001"])
        self.assertIn("filing.xhtml", result["skipped"][0]["document"])
        self.assertIn("EgetKapital", result["skipped"][0]["content"])

    def test_parent_company_tuple_does_not_replace_company_identity(self):
        result = parse_archive(archive(fact("Nettoomsattning", "100"),
            '<ix:nonNumeric name="se-cd:ForetagetsNamn" contextRef="period" tupleRef="ParentTuple">Another company AB</ix:nonNumeric>'))
        self.assertEqual(result["rows"][0]["company_name"], "Reporting company AB")

    def test_no_usable_facts_preserves_unpacked_document(self):
        result = parse_archive(archive(""))
        self.assertEqual(result["rows"], [])
        self.assertEqual(result["report_count"], 1)
        self.assertEqual(len(result["skipped"]), 1)
        self.assertIn("Reporting company AB", result["skipped"][0]["content"])
        self.assertNotIn("content_encoding", result["skipped"][0])

    def test_large_diagnostic_is_lossless_deterministic_and_keeps_partial_row(self):
        source = archive(fact("Nettoomsattning", "100")
            + fact("EgetKapital", "bad-value", context="balance"),
            extra="<!--" + "å" * 600000 + "-->")
        archive_bytes = source.getvalue()
        with zipfile.ZipFile(source) as outer:
            with zipfile.ZipFile(BytesIO(outer.read(outer.infolist()[0]))) as inner:
                original = inner.read("filing.xhtml")
        result = parse_archive(archive_bytes)
        self.assertEqual(result["report_count"], 1)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["net_revenue"], Decimal("100"))
        self.assertEqual(len(result["skipped"]), 1)
        entry = result["skipped"][0]
        self.assertEqual(entry["reason"], "unsupported_facts:equity:unsupported_number")
        self.assertEqual(entry["content_encoding"], "gzip+base64")
        compressed = base64.b64decode(entry["content"], validate=True)
        self.assertEqual(gzip.decompress(compressed), original)
        self.assertEqual(entry["content_bytes"], len(original))
        self.assertEqual(entry["content_sha256"], hashlib.sha256(original).hexdigest())
        self.assertLess(len(entry["content"]), len(original) // 10)
        self.assertEqual(parse_archive(archive_bytes)["skipped"][0]["content"], entry["content"])

    def test_large_invalid_utf8_diagnostic_preserves_every_original_byte(self):
        original = b"<html>" + b"\xff\xfe\x00" + b"x" * (1024 * 1024 - 9)
        self.assertEqual(len(original), 1024 * 1024)
        result = parse_archive(archive("", attachments=[("invalid.xhtml", original)]))
        self.assertEqual(result["rows"], [])
        self.assertEqual(result["report_count"], 1)
        self.assertEqual(len(result["skipped"]), 2)
        entry = next(item for item in result["skipped"] if item["document"] == "invalid.xhtml")
        self.assertEqual(entry["reason"], "invalid_xml")
        self.assertEqual(entry["content_encoding"], "gzip+base64")
        self.assertEqual(gzip.decompress(base64.b64decode(entry["content"], validate=True)), original)
        self.assertEqual(entry["content_bytes"], len(original))
        self.assertEqual(entry["content_sha256"], hashlib.sha256(original).hexdigest())

    def test_compression_does_not_relax_uncompressed_exception_limit(self):
        original = b"x" * (1024 * 1024)
        with patch("bv_parse.MAX_RETAINED_EXCEPTIONS", len(original) - 1):
            with self.assertRaisesRegex(ParseError, "too_much_exception_content"):
                parse_archive(archive("", attachments=[("large.xhtml", original)]))

    def test_corrupt_or_oversized_archive_cannot_be_completed(self):
        with self.assertRaises(ParseError):
            parse_archive(BytesIO(b"not a ZIP"))
        with patch("bv_parse.MAX_DOCUMENT_BYTES", 8):
            with self.assertRaises(ParseError):
                parse_archive(archive(fact("Nettoomsattning", "100")))

    def test_nested_esef_package_preserves_company_period_and_diagnostic_path(self):
        result = parse_archive(archive(fact("Nettoomsattning", "123 456")
            + fact("EgetKapital", "bad-value", context="balance"), nested_layers=2))
        self.assertEqual(result["report_count"], 1)
        row = result["rows"][0]
        self.assertEqual(row["orgnr"], "5569765794")
        self.assertEqual(row["period_start"], "2025-01-01")
        self.assertEqual(row["period_end"], "2025-12-31")
        self.assertEqual(row["net_revenue"], Decimal("123456"))
        self.assertEqual(row["source_document_id"], "filing")
        self.assertEqual(result["skipped"][0]["document"], "package.zip/package.zip/filing.xhtml")
        self.assertIn("bad-value", result["skipped"][0]["content"])

    def test_oversized_unsupported_attachment_is_not_read(self):
        original_read = zipfile.ZipFile.read
        read_names = []

        def tracked_read(source, item, *args, **kwargs):
            read_names.append(item.filename if isinstance(item, zipfile.ZipInfo) else item)
            return original_read(source, item, *args, **kwargs)

        source = archive(fact("Nettoomsattning", "100"), attachments=[("image.png", b"x" * 5000)])
        with patch("bv_parse.MAX_DOCUMENT_BYTES", 2000), patch.object(zipfile.ZipFile, "read", tracked_read):
            result = parse_archive(source)
        self.assertEqual(result["rows"][0]["net_revenue"], Decimal("100"))
        self.assertNotIn("image.png", read_names)
        self.assertEqual(result["ignored_attachments"], [dict(
            archive_member="5569765794_2025-12-31.zip", document="image.png",
            declared_bytes=5000, reason="unsupported_attachment_type")])

    def test_corrupt_nested_zip_is_not_silently_ignored(self):
        with self.assertRaisesRegex(ParseError, "invalid_zip:BadZipFile"):
            parse_archive(archive(fact("Nettoomsattning", "100"),
                                  attachments=[("broken.zip", b"not a zip")]))

    def test_nested_zip_depth_is_bounded(self):
        with patch("bv_parse.MAX_NESTED_ZIP_DEPTH", 1):
            with self.assertRaisesRegex(ParseError, "nested_archive_depth_limit"):
                parse_archive(archive(fact("Nettoomsattning", "100"), nested_layers=2))

    def test_nested_zip_expansion_counts_all_layers(self):
        source = archive(fact("Nettoomsattning", "100"), nested_layers=1)
        with zipfile.ZipFile(source) as outer:
            with zipfile.ZipFile(BytesIO(outer.read(outer.infolist()[0]))) as inner:
                wrapper_expansion = sum(item.file_size for item in inner.infolist())
        source.seek(0)
        with patch("bv_parse.MAX_TOTAL_EXPANSION", wrapper_expansion + 1):
            with self.assertRaisesRegex(ParseError, "archive_expansion_limit"):
                parse_archive(source)

    def test_nested_zip_entries_count_across_each_company_package(self):
        with patch("bv_parse.MAX_INNER_ENTRIES", 1):
            with self.assertRaisesRegex(ParseError, "too_many_inner_entries"):
                parse_archive(archive(fact("Nettoomsattning", "100"), nested_layers=1))

    def test_unmapped_nested_document_is_retained(self):
        result = parse_archive(archive("", extra="<!--" + "x" * 10000 + "-->", nested_layers=1))
        self.assertEqual(result["rows"], [])
        self.assertEqual(result["skipped"][0]["reason"], "no_unambiguous_revenue_or_profit")
        self.assertGreater(len(result["skipped"][0]["content"]), 10000)


if __name__ == "__main__":
    unittest.main()
