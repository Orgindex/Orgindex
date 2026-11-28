"""Parse Bolagsverket bulk archives without writing archive bytes to disk.

Only unambiguous company facts for the filing period are published. Documents
which cannot be represented are returned with their unpacked source for review.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import PurePosixPath
import re
import zipfile

from lxml import etree

XBRL = "http://www.xbrl.org/2003/instance"
METRICS = ("net_revenue", "operating_profit", "profit_after_financial_items",
           "net_profit", "total_assets", "equity", "employees")
FIELDS = dict(zip(("Nettoomsattning", "Rorelseresultat",
                   "ResultatEfterFinansiellaPoster", "AretsResultat",
                   "Tillgangar", "EgetKapital", "MedelantaletAnstallda"), METRICS))
FIELDS["MedelantalAnstallda"] = "employees"
# Verified ESEF sources include ZIPs above 64 MiB and XHTML above 96 MiB.
MAX_INNER_BYTES = 128 * 1024 * 1024
MAX_DOCUMENT_BYTES = 128 * 1024 * 1024
MAX_OUTER_EXPANSION = 1024 * 1024 * 1024
MAX_TOTAL_EXPANSION = 2 * 1024 * 1024 * 1024
MAX_RETAINED_EXCEPTIONS = 256 * 1024 * 1024
MAX_REPORTS = 20000
MAX_INNER_ENTRIES = 1000
MAX_NESTED_ZIP_DEPTH = 3


class ParseError(ValueError):
    """A corrupt or unbounded archive must not be marked completed."""


def _local(tag):
    return etree.QName(tag).localname if isinstance(tag, str) else ""


def _number(fact):
    if fact.get("{http://www.w3.org/2001/XMLSchema-instance}nil") in ("true", "1"):
        return None
    raw = re.sub(r"\s+", "", "".join(fact.itertext())).replace("\u2212", "-")
    fmt = fact.get("format", "").split(":")[-1].lower()
    if fmt in ("numspacecomma", "numcomma", "numcommadecimal", "numdotcomma"):
        raw = raw.replace(".", "").replace(",", ".")
    elif fmt in ("numspacedot", "numdotdecimal", "numcommadot", "numdot"):
        raw = raw.replace(",", "")
    elif fmt in ("zerodash", "numdash", "fixed-zero"):
        if raw not in ("-", "–", "—", "0"):
            return None
        raw = "0"
    elif fmt:
        return None
    if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", raw):
        return None
    try:
        scale = int(fact.get("scale", "0"))
        if abs(scale) > 18 or len(raw) > 100:
            return None
        value = Decimal(raw).scaleb(scale)
        if fact.get("sign") == "-":
            value = -abs(value)
        decimals = fact.get("decimals", "")
        precision = float("inf") if decimals == "INF" else int(decimals) if decimals else -1000
        return value, precision
    except (InvalidOperation, ValueError, OverflowError):
        return None


def _document(content, orgnr, period_end, member, doc):
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
    try:
        root = etree.fromstring(content, parser)
    except etree.XMLSyntaxError:
        return None, [], "invalid_xml"
    contexts = {}
    for context in root.iter("{" + XBRL + "}context"):
        identifier = context.findtext(".//{" + XBRL + "}identifier", "")
        identifier = re.sub(r"[-\s]", "", identifier)
        if identifier != orgnr:
            continue
        if any(_local(e.tag) in ("segment", "scenario") for e in context.iter()):
            continue
        start = context.findtext(".//{" + XBRL + "}startDate")
        end = (context.findtext(".//{" + XBRL + "}endDate")
               or context.findtext(".//{" + XBRL + "}instant"))
        if end == period_end:
            contexts[context.get("id")] = start
    starts = {start for start in contexts.values() if start}
    if len(starts) != 1:
        return None, [], "missing_or_ambiguous_company_period"
    period_start = next(iter(starts))
    try:
        if date.fromisoformat(period_start) > date.fromisoformat(period_end):
            return None, [], "invalid_period"
    except ValueError:
        return None, [], "invalid_period"
    units = {}
    for unit in root.iter("{" + XBRL + "}unit"):
        measures = unit.findall("{" + XBRL + "}measure")
        if len(measures) == 1:
            units[unit.get("id")] = (measures[0].text or "").split(":")[-1]
    names = set()
    facts = {}
    unresolved = []
    for fact in root.iter():
        local = _local(fact.tag)
        if local not in ("nonNumeric", "nonFraction"):
            continue
        # A tuple can describe a parent/subsidiary despite sharing the same
        # reporting context; its name and balances are not this company's.
        if fact.get("tupleRef") or any(_local(parent.tag) == "tuple" for parent in fact.iterancestors()):
            continue
        if local == "nonNumeric" and fact.get("name", "").split(":")[-1] == "ForetagetsNamn":
            if fact.get("contextRef") in contexts:
                name = " ".join("".join(fact.itertext()).split())
                if name and len(name) <= 1000:
                    names.add(name)
        if local != "nonFraction":
            continue
        field = FIELDS.get(fact.get("name", "").split(":")[-1])
        ref = fact.get("contextRef")
        if not field or ref not in contexts:
            continue
        if (field in ("equity", "total_assets")) != (contexts[ref] is None):
            continue
        if field != "employees" and units.get(fact.get("unitRef")) != "SEK":
            unresolved.append(field + ":unsupported_currency")
            continue
        parsed = _number(fact)
        if parsed is None:
            if fact.get("{http://www.w3.org/2001/XMLSchema-instance}nil") not in ("true", "1"):
                unresolved.append(field + ":unsupported_number")
            continue
        value, precision = parsed
        current = facts.get(field)
        if current is None or precision > current[0]:
            facts[field] = (precision, {value})
        elif precision == current[0]:
            current[1].add(value)
    conflicts = []
    chosen = {}
    for field in METRICS:
        precision, values = facts.get(field, (-1000, set()))
        chosen[field] = next(iter(values)) if len(values) == 1 else None
        if len(values) > 1:
            conflicts.append(dict(archive_member=member, document=doc, field=field,
                                  precision=str(precision), values=sorted(str(v) for v in values)))
    if chosen["net_revenue"] is None and chosen["net_profit"] is None:
        return None, conflicts, "no_unambiguous_revenue_or_profit"
    row = dict(orgnr=orgnr, period_start=period_start, period_end=period_end,
               reporting_scope="company", currency="SEK", **chosen,
               source_document_id=PurePosixPath(doc).stem,
               company_name=next(iter(names)) if len(names) == 1 else None,
               _precision={key: val[0] for key, val in facts.items()},
               _values={key: set(val[1]) for key, val in facts.items()})
    reason = "conflicting_facts" if conflicts else "unsupported_facts:" + ",".join(sorted(set(unresolved))) if unresolved else None
    return row, conflicts, reason


def parse_archive(source):
    """Return rows, report_count, skipped documents, and fact conflicts.

    ``source`` may be BytesIO, another seekable binary stream, bytes, or a Path.
    Nothing is extracted onto the filesystem. ``skipped`` can include a source
    document with a partial row when some of its facts need manual review.
    """
    if isinstance(source, (bytes, bytearray)):
        source = BytesIO(source)
    rows = {}
    skipped = []
    conflicts = []
    ignored_attachments = []
    report_count = 0
    total_expansion = 0
    retained = 0
    report_entries = 0

    def documents(source, archive_member, prefix="", depth=0):
        """Walk bounded ZIP packages while preserving the enclosing report identity.

        Listed companies can put an ESEF taxonomy/report package inside the
        company's ZIP. Entry counts accumulate across that company's packages;
        decompressed sizes accumulate across the whole bulk archive.
        """
        nonlocal total_expansion, report_entries
        with zipfile.ZipFile(BytesIO(source)) as inner:
            items = [item for item in inner.infolist() if not item.is_dir()]
            report_entries += len(items)
            if report_entries > MAX_INNER_ENTRIES:
                raise ParseError("too_many_inner_entries")
            total_expansion += sum(item.file_size for item in items)
            if total_expansion > MAX_TOTAL_EXPANSION:
                raise ParseError("archive_expansion_limit")
            for item in items:
                name = prefix + item.filename
                if item.filename.lower().endswith(".zip"):
                    if depth >= MAX_NESTED_ZIP_DEPTH:
                        raise ParseError("nested_archive_depth_limit")
                    if item.file_size > MAX_INNER_BYTES:
                        raise ParseError("inner_archive_too_large")
                    yield from documents(inner.read(item), archive_member, name + "/", depth + 1)
                elif item.filename.lower().endswith((".html", ".xhtml", ".xml", ".xbrl")):
                    if item.file_size > MAX_DOCUMENT_BYTES:
                        raise ParseError("document_too_large")
                    yield name, inner.read(item)
                else:
                    ignored_attachments.append(dict(archive_member=archive_member,
                        document=name, declared_bytes=item.file_size,
                        reason="unsupported_attachment_type"))

    def retain(member, document, reason, content=None):
        nonlocal retained
        if isinstance(content, bytes):
            retained += len(content)
            content = content.decode("utf-8", errors="replace")
        if retained > MAX_RETAINED_EXCEPTIONS:
            raise ParseError("too_much_exception_content")
        skipped.append(dict(archive_member=member, document=document, reason=reason, content=content))

    try:
        with zipfile.ZipFile(source) as outer:
            members = [item for item in outer.infolist() if not item.is_dir()]
            if len(members) > MAX_REPORTS or sum(item.file_size for item in members) > MAX_OUTER_EXPANSION:
                raise ParseError("outer_archive_expansion_limit")
            for member in members:
                match = re.fullmatch(r"(\d{10})_(\d{4}-\d{2}-\d{2})\.zip", PurePosixPath(member.filename).name)
                if not match:
                    raise ParseError("unexpected_outer_member:" + member.filename[:200])
                if member.file_size > MAX_INNER_BYTES:
                    raise ParseError("inner_archive_too_large")
                orgnr, period_end = match.groups()
                report_count += 1
                document_results = []
                raw_documents = []
                report_entries = 0
                for document, content in documents(outer.read(member), member.filename):
                    row, issues, reason = _document(content, orgnr, period_end, member.filename, document)
                    conflicts.extend(issues)
                    raw_documents.append((document, reason, content))
                    if row:
                        document_results.append(row)
                        if reason:
                            retain(member.filename, document, reason, content)
                if not document_results:
                    for doc, reason, content in raw_documents:
                        retain(member.filename, doc, reason or "no_financial_document", content)
                    if not raw_documents:
                        retain(member.filename, None, "no_supported_document")
                    continue
                for row in document_results:
                    key = (row["orgnr"], row["period_start"], row["period_end"], row["reporting_scope"])
                    previous = rows.get(key)
                    if previous is None:
                        rows[key] = row
                        continue
                    for field in METRICS:
                        if row[field] is None:
                            continue
                        new_precision = row["_precision"].get(field, -1000)
                        old_precision = previous["_precision"].get(field, -1000)
                        if new_precision > old_precision or field not in previous["_values"]:
                            previous[field] = row[field]
                            previous["_precision"][field] = new_precision
                            previous["_values"][field] = set(row["_values"].get(field, {row[field]}))
                        elif new_precision == old_precision and row[field] not in previous["_values"].get(field, set()):
                            previous["_values"].setdefault(field, set()).add(row[field])
                            conflicts.append(dict(archive_member=member.filename, document=row["source_document_id"],
                                                  field=field, precision=str(new_precision),
                                                  values=sorted(str(value) for value in previous["_values"][field])))
                            previous[field] = None
                            for doc, _, content in raw_documents:
                                retain(member.filename, doc, "conflicting_documents", content)
                    if not previous["company_name"] and row["company_name"]:
                        previous["company_name"] = row["company_name"]
        for row in rows.values():
            row.pop("_precision", None)
            row.pop("_values", None)
        return dict(rows=list(rows.values()), report_count=report_count, skipped=skipped,
                    conflicts=conflicts, ignored_attachments=ignored_attachments)
    except (zipfile.BadZipFile, RuntimeError, EOFError, NotImplementedError) as exc:
        raise ParseError("invalid_zip:" + type(exc).__name__) from exc
