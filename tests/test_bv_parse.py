"""Regression cases for values observed in Bolagsverket's real filings."""
import sys
import unittest
import zipfile
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from bv_parse import ParseError, parse_archive


def archive(facts, extra=""):
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

    def test_corrupt_or_oversized_archive_cannot_be_completed(self):
        with self.assertRaises(ParseError):
            parse_archive(BytesIO(b"not a ZIP"))
        with patch("bv_parse.MAX_DOCUMENT_BYTES", 8):
            with self.assertRaises(ParseError):
                parse_archive(archive(fact("Nettoomsattning", "100")))


if __name__ == "__main__":
    unittest.main()
