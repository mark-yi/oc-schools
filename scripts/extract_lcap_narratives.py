#!/usr/bin/env python3
"""Extract section-tagged narrative chunks from California LCAP PDFs."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import hashlib
import json
import re
import subprocess
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

try:  # Preferred: preserves page ordering better than pypdf for narrative text.
    import fitz  # type: ignore
except Exception:  # pragma: no cover - exercised only when PyMuPDF is absent.
    fitz = None

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ANALYTICS_DB = ROOT / "outputs" / "analytics" / "2025" / "analytics.sqlite"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "rag" / "2025"

TARGET_TOKENS = 720
MAX_TOKENS = 920
OVERLAP_TOKENS = 100
MIN_CHUNK_TOKENS = 45
OCR_DPI = 180
WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]{4,}")
OCR_SCORE_TERMS = (
    "local",
    "control",
    "accountability",
    "plan",
    "school",
    "district",
    "student",
    "students",
    "goal",
    "action",
    "parents",
    "educational",
)


SECTION_PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("plan_summary", "Plan Summary", re.compile(r"^Plan Summary(?:\s*\[[^\]]+\])?$", re.I)),
    ("plan_summary", "Resumen del Plan", re.compile(r"^Resumen del Plan", re.I)),
    ("plan_summary", "General Information", re.compile(r"^General Information$", re.I)),
    ("plan_summary", "Información General", re.compile(r"^Información General$", re.I)),
    (
        "plan_summary",
        "Reflections: Annual Performance",
        re.compile(r"^Reflections:\s*Annual Performance", re.I),
    ),
    (
        "plan_summary",
        "Reflexiones: Desempeño Anual",
        re.compile(r"^Reflexiones:\s*Desempeño Anual", re.I),
    ),
    (
        "plan_summary",
        "Reflections: Technical Assistance",
        re.compile(r"^Reflections:\s*Technical Assistance", re.I),
    ),
    (
        "plan_summary",
        "Reflexiones: Ayuda Técnica",
        re.compile(r"^Reflexiones:\s*Ayuda Técnica", re.I),
    ),
    (
        "support_for_identified_schools",
        "Support for Identified Schools",
        re.compile(r"^(Comprehensive Support and Improvement|Support for Identified Schools)", re.I),
    ),
    (
        "support_for_identified_schools",
        "Apoyo para Escuelas Identificadas",
        re.compile(r"^(Apoyo y Mejoramiento Integral|Apoyo para Escuelas Identificadas)", re.I),
    ),
    (
        "monitoring_effectiveness",
        "Monitoring and Evaluating Effectiveness",
        re.compile(r"^Monitoring and Evaluating Effectiveness", re.I),
    ),
    (
        "monitoring_effectiveness",
        "Supervisando y Evaluando Efectividad",
        re.compile(r"^Supervisando y Evaluando Efectividad", re.I),
    ),
    (
        "engaging_partners",
        "Engaging Educational Partners",
        re.compile(r"^Engaging Educational Partners", re.I),
    ),
    (
        "engaging_partners",
        "Participación de Compañeros Educativos",
        re.compile(r"^Participación de (?:Compañeros|Socios) Educativos", re.I),
    ),
    ("goals_actions", "Goals and Actions", re.compile(r"^Goals and Actions", re.I)),
    ("goals_actions", "Metas y Acciones", re.compile(r"^Metas y Acciones", re.I)),
    ("goal_analysis", "Goal Analysis", re.compile(r"^Goal Analysis(?:\s*\[[^\]]+\])?$", re.I)),
    ("goal_analysis", "Análisis de Meta", re.compile(r"^Análisis de (?:la )?Meta", re.I)),
    (
        "increased_improved_services",
        "Increased or Improved Services",
        re.compile(r"^Increased or Improved Services", re.I),
    ),
    (
        "increased_improved_services",
        "Aumento o Mejora de Servicios",
        re.compile(r"^Aumento o Mejora de Servicios", re.I),
    ),
    (
        "required_descriptions",
        "Required Descriptions",
        re.compile(r"^Required Descriptions", re.I),
    ),
    (
        "required_descriptions",
        "Descripciones Requeridas",
        re.compile(r"^Descripciones Requeridas", re.I),
    ),
    ("equity_multiplier", "Equity Multiplier", re.compile(r"^Equity Multiplier", re.I)),
    ("equity_multiplier", "Multiplicador de Equidad", re.compile(r"^Multiplicador de Equidad", re.I)),
]

PROMPT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("implementation", re.compile(r"^(Describe|A description of).{0,90}implementation", re.I)),
    ("implementation", re.compile(r"^(Describa|Una descripción).{0,120}implementación", re.I)),
    ("effectiveness", re.compile(r"^(Describe|A description of).{0,120}effectiveness|ineffectiveness", re.I)),
    ("effectiveness", re.compile(r"^(Describa|Una descripción).{0,140}efectiv", re.I)),
    ("material_differences", re.compile(r"material differences", re.I)),
    ("material_differences", re.compile(r"diferencias materiales", re.I)),
    ("changes", re.compile(r"changes made to the planned goal|changes made to the planned", re.I)),
    ("changes", re.compile(r"cambios (?:hechos|realizados).{0,120}(?:meta|planeada|planificada)", re.I)),
    ("partner_engagement", re.compile(r"educational partners|input from", re.I)),
    ("partner_engagement", re.compile(r"compañeros educativos|socios educativos|comentarios de", re.I)),
    ("increased_improved_services", re.compile(r"increased or improved services", re.I)),
    ("increased_improved_services", re.compile(r"aumento o mejora de servicios", re.I)),
    ("needs_conditions", re.compile(r"needs, conditions|performance gaps|identified needs", re.I)),
    ("needs_conditions", re.compile(r"necesidades, condiciones|brechas de rendimiento|necesidades identificadas", re.I)),
]

TEMPLATE_MARKERS = (
    "local control and accountability plan instructions",
    "lcap instructions",
    "instructions: introduction",
    "instructions: plan summary",
    "instrucciones para completar",
    "requisitos e instrucciones",
    "para preguntas adicionales o ayuda técnica",
    "for additional questions or technical assistance",
    "for additional questions or technical assistance related to the completion of the lcap template",
    "the lcap template must be completed by all leas",
    "the following tables are required to be included as part of the lcap adopted",
    "for each action, provide the following information",
    "a well-developed plan summary section provides",
    "school districts and county offices of education must",
    "leas must provide a justification",
    "california department of education, july",
    "california department of education november",
)

BUDGET_OVERVIEW_MARKERS = (
    "lcff budget overview for parents",
    "resumen presupuestario lcff para los padres",
    "budgeted expenditures in the lcap",
    "projected lcff supplemental and/or concentration grants",
    "ingreso proyecto por fuente financiera",
)

LCFF_SUMMARY_MARKERS = (
    "total projected lcff supplemental",
    "total proyectado de subvenciones suplementarias",
    "required percentage to increase or improve services",
    "porcentaje requerido para aumentar o mejorar servicios",
)

TABLE_PAGE_MARKERS = (
    "prior action/service title",
    "last year's planned expenditures",
    "estimated actual expenditures",
    "total planned expenditures table",
    "contributing actions table",
    "annual update table",
    "lcff carryover table",
    "projected lcff base grant",
    "planned percentage of improved services",
    "planned expenditures for contributing actions",
    "scope unduplicated student group",
)

TABLE_HEADER_RE = re.compile(
    r"(?i)\b("
    r"metric #|baseline|year 1 outcome|year 2 outcome|target for year 3|current difference"
    r"|action #|action title|total funds|planned expenditures|contributing to increased"
    r"|goal #\s+description\s+type of goal"
    r"|# de medida|resultado del año|objetivo para\s+resultado del año|fondos totales"
    r")\b"
)
GOAL_RE = re.compile(r"(?i)^(?:broad\s+|focus\s+|maintenance\s+)?(?:goal|meta)\s+(\d+(?:\.\d+)?[A-Za-z]?)\b")
ACTION_RE = re.compile(r"(?i)^(?:Action|Acción)\s+(\d+(?:\.\d+)?[A-Za-z]?)\b")
LCAP_HEADER_RE = re.compile(
    r"(?i)^(?:\d{4}-\d{2}\s+)?(?:Local Control and Accountability Plan|Plan de Rendición de Cuentas con Control Local).+"
    r"(?:Page|Página) \d+ (?:of|de) \d+$"
)
LOOSE_LCAP_HEADER_RE = re.compile(
    r"(?i)(?:\d{4}-\d{2}\s+)?(?:Local Control and Accountability Plan|Plan de Rendición de Cuentas con Control Local).+"
)

STRUCTURAL_LINE_RE = re.compile(
    r"(?i)^("
    r"goal|goal #|description|type of goal|state priorities addressed by this goal"
    r"|action #|title|total funds|contributing|metric #|metric|baseline"
    r"|year 1 outcome|year 2 outcome|target for year 3 outcome|desired outcome"
    r"|# de medida|medida|referente|resultado del año|objetivo para resultado del año"
    r"|goal and|goal and action #|identified need\\(s\\)|metric\\(s\\) to monitor"
    r"|# de meta y acción|necesidades identificadas|medidas para supervisar"
    r"|effectiveness|provided on an lea-wide or schoolwide basis"
    r"|how the action\\(s\\) address need\\(s\\).*"
    r"|scope:?|xlea-wide|xschoolwide|xlimited to.*|xall schools"
    r"|contributing actions table|annual update table|lcff carryover table"
    r")$"
)
TEMPLATE_TAIL_RE = re.compile(
    r"(?i)\b(?:"
    r"2024\s+LCAP\s+Annual Update for the 2023-24 LCAP"
    r"|2025-26\s+Local Control and Accountability"
    r"|2025-26\s+Plan de Rendición de Cuentas con Control Local"
    r"|Instructions\s+For additional questions or technical assistance"
    r"|For additional questions or technical assistance"
    r"|Requirements and Instructions"
    r"|Requisitos e Instrucciones"
    r"|Para preguntas adicionales o ayuda técnica"
    r"|Complete the prompts as instructed"
    r")\b"
)
REQUIRED_DESCRIPTIONS_PROMPT_RE = re.compile(
    r"(?i)^(?:with\s+)?(?:an?\s+)?(?:Improved Services|Planned Percentage of Improved Services)"
    r"\s+in the Contributing Summary Table rather than.*?as applicable\.\s*"
)
ADDITIONAL_CONCENTRATION_PROMPT_RE = re.compile(
    r"(?i)\bAdditional Concentration Grant Funding\s+A description of the plan for how the additional "
    r"concentration grant add-on funding.*?as applicable\.\s*"
)
INLINE_REQUIRED_TABLE_HEADER_RE = re.compile(
    r"(?i)\b(?:Metric #\s*[\w\s.,/-]{0,80})?Identified Need\(s\)\s+How the Action\(s\).*?"
    r"Metric\(s\) to Monitor\s*"
)
CHUNK_TABLE_TAIL_RE = re.compile(
    r"(?is)\n\s*(?:\d+\s+of\s+\d+\s+)?(?:Local Control and Accountability Plan\s*)?"
    r"Action\s*\n\s*#\s*(?:●\s*)?(?:Description\s*)?(?:\n|\s)+Total\s*(?:\n|\s)+Funds.*$"
)


@dataclass
class PageText:
    page_number: int
    text: str


@dataclass
class Segment:
    cds_code: str
    county: str
    district: str
    school_year: str
    source_path: str
    pdf_url: str
    document_match: int | None
    section_type: str
    section_path: str
    prompt_label: str
    goal_number: str
    action_number: str
    chunk_kind: str
    page_start: int
    page_end: int
    text: str


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    replacements = {
        "\xa0": " ",
        "\u200b": "",
        "\u200c": "",
        "\u200d": "",
        "\ufeff": "",
        "\u202a": "",
        "\u202b": "",
        "\u202c": "",
        "\u202d": "",
        "\u202e": "",
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\u2022": "*",
        "\u00ad": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", normalize_text(value)).strip()


def stable_hash(value: str, length: int = 16) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]


def estimate_tokens(text: str) -> int:
    # Good enough for chunking and stable without an optional tokenizer dependency.
    return max(1, int(len(text) / 4))


def load_documents(db_path: Path, cds_code: str | None, limit: int | None) -> list[dict[str, Any]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        where = ["source_path != ''", "coalesce(district_name_match, 1) != 0"]
        params: list[Any] = []
        if cds_code:
            where.append("cds_code = ?")
            params.append(cds_code)
        query = f"""
            select
              cds_code,
              county,
              district,
              school_year,
              source_path,
              pdf_url,
              district_name_match
            from lcap_documents
            where {' and '.join(where)}
            order by county, district, cds_code
        """
        if limit:
            query += " limit ?"
            params.append(limit)
        return [dict(row) for row in connection.execute(query, params)]
    finally:
        connection.close()


def extract_pages_with_pymupdf(path: Path) -> list[PageText]:
    assert fitz is not None
    pages: list[PageText] = []
    with fitz.open(path) as document:  # type: ignore[attr-defined]
        for page_index, page in enumerate(document, start=1):
            blocks = page.get_text("blocks") or []
            blocks = sorted(blocks, key=lambda block: (round(block[1], 1), round(block[0], 1)))
            lines: list[str] = []
            for block in blocks:
                text = normalize_text(block[4] if len(block) > 4 else "")
                if text:
                    lines.extend(line for line in text.split("\n") if normalize_text(line))
            pages.append(PageText(page_index, "\n".join(lines)))
    return pages


def extract_pages_with_pypdf(path: Path) -> list[PageText]:
    reader = PdfReader(str(path))
    pages: list[PageText] = []
    for page_index, page in enumerate(reader.pages, start=1):
        try:
            text = normalize_text(page.extract_text() or "")
        except Exception:
            text = ""
        pages.append(PageText(page_index, text))
    return pages


def needs_ocr_fallback(pages: list[PageText]) -> bool:
    if fitz is None or not pages:
        return False
    text = "\n".join(page.text for page in pages)
    compact = compact_text(text)
    word_count = len(WORD_RE.findall(compact))
    if len(pages) > 5 and len(compact) < 1000:
        return True
    if len(pages) > 20 and word_count < len(pages) * 8:
        return True
    return len(compact) > 10000 and word_count / max(len(compact), 1) < 0.01


def ocr_quality_score(text: str) -> float:
    lowered = compact_text(text).casefold()
    term_score = sum(lowered.count(term) for term in OCR_SCORE_TERMS)
    word_score = len(WORD_RE.findall(lowered)) / 40
    return term_score + word_score


def tesseract_image(image_path: Path) -> str:
    result = subprocess.run(
        ["tesseract", str(image_path), "stdout", "-l", "eng", "--psm", "3"],
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    return normalize_text(result.stdout if result.returncode == 0 else "")


def choose_ocr_rotation(document: Any, temp_dir: Path) -> int:
    if len(document) == 0:
        return 0
    page = document[min(4, len(document) - 1)]
    best_rotation = 0
    best_score = -1.0
    for rotation in (0, 90, 180, 270):
        image_path = temp_dir / f"rotation_probe_{rotation}.png"
        pixmap = page.get_pixmap(
            matrix=fitz.Matrix(OCR_DPI / 72, OCR_DPI / 72).prerotate(rotation),
            alpha=False,
        )
        pixmap.save(str(image_path))
        score = ocr_quality_score(tesseract_image(image_path))
        if score > best_score:
            best_score = score
            best_rotation = rotation
    return best_rotation


def extract_pages_with_ocr(path: Path) -> list[PageText]:
    assert fitz is not None
    pages: list[PageText] = []
    with tempfile.TemporaryDirectory(prefix="lcap_ocr_") as tmpdir:
        temp_dir = Path(tmpdir)
        with fitz.open(path) as document:  # type: ignore[attr-defined]
            rotation = choose_ocr_rotation(document, temp_dir)
            matrix = fitz.Matrix(OCR_DPI / 72, OCR_DPI / 72).prerotate(rotation)
            for page_index, page in enumerate(document, start=1):
                image_path = temp_dir / f"page_{page_index:04d}.png"
                pixmap = page.get_pixmap(matrix=matrix, alpha=False)
                pixmap.save(str(image_path))
                pages.append(PageText(page_index, tesseract_image(image_path)))
    return pages


def extract_pages(path: Path) -> list[PageText]:
    if fitz is not None:
        try:
            pages = extract_pages_with_pymupdf(path)
            if needs_ocr_fallback(pages):
                try:
                    ocr_pages = extract_pages_with_ocr(path)
                    raw_text = "\n".join(page.text for page in pages)
                    ocr_text = "\n".join(page.text for page in ocr_pages)
                    if (
                        sum(len(page.text) for page in ocr_pages) > sum(len(page.text) for page in pages)
                        or ocr_quality_score(ocr_text) > ocr_quality_score(raw_text) * 1.5
                    ):
                        return ocr_pages
                except Exception:
                    pass
            return pages
        except Exception:
            pass
    return extract_pages_with_pypdf(path)


def clean_line(line: str) -> str:
    line = compact_text(line)
    if not line:
        return ""
    line = re.sub(
        r"(?i)^.*?Local Control and Accountability Plan(?:\s*\(LCAP\))?\s*\|\s*[A-Z]+ \d{1,2}, \d{4}\s*",
        "",
        line,
    )
    line = re.sub(
        r"(?i)^(?:\d{4}-\d{2}\s+)?Local Control and Accountability Plan for .+? Page \d+ of \d+\s*",
        "",
        line,
    )
    line = re.sub(
        r"(?i)^(?:\d{4}-\d{2}\s+)?Plan de Rendición de Cuentas con Control Local .+? Página \d+ de \d+\s*",
        "",
        line,
    )
    line = re.sub(r"(?i)^Local Control and Accountability Plan Template\s*", "", line)
    line = re.sub(r"(?i)^Plan de Rendición de Cuentas con Control Local\s*", "", line)
    line = re.sub(r"(?i)^LCAP\s*-\s*Final\s+FCSS\s+Approved\s*", "", line)
    line = re.sub(r"(?i)^Goal\s*#\s*Description\s+(?:Type of Goal\s+)?(?:\d+(?:\.\d+)?[A-Za-z]?\s+)?", "", line)
    line = re.sub(r"(?i)^Goal\s+Goal\s*#\s*Description\s+(?:Type of Goal\s+)?(?:\d+(?:\.\d+)?[A-Za-z]?\s+)?", "", line)
    line = re.sub(r"(?i)\b(?:Focus|Broad|Maintenance)?\s*Goal\s+Goal\s*#\s*Description(?:\s+Type of Goal)?\s*", "", line)
    line = re.sub(r"(?i)\bGoal\s*#\s*Description\s+Type of Goal\s*", "", line)
    line = re.sub(r"(?i)^Goal and\s+Action #\s+Identified Need\(s\)\s+", "", line)
    line = re.sub(r"(?i)^How the Action\(s\).+?Metric\(s\) to Monitor\s+Effectiveness\s*", "", line)
    line = REQUIRED_DESCRIPTIONS_PROMPT_RE.sub("", line).strip()
    line = ADDITIONAL_CONCENTRATION_PROMPT_RE.sub("Additional Concentration Grant Funding: ", line).strip()
    line = INLINE_REQUIRED_TABLE_HEADER_RE.sub("", line).strip()
    line = re.split(r"(?i)\bPlease refer to the below table\b|\bAction #\s+Total Funds\b", line, maxsplit=1)[0].strip()
    tail_match = TEMPLATE_TAIL_RE.search(line)
    if tail_match:
        line = line[: tail_match.start()].strip()
    if not line:
        return ""
    if LCAP_HEADER_RE.match(line):
        return ""
    if LOOSE_LCAP_HEADER_RE.fullmatch(line):
        return ""
    if STRUCTURAL_LINE_RE.match(line):
        return ""
    if re.fullmatch(r"(?i)Improved Services and Estimated Actual Percentages of Improved Services\.?", line):
        return ""
    lowered = line.casefold()
    if any(marker in lowered for marker in LCFF_SUMMARY_MARKERS):
        return ""
    if "projected percentage to increase" in lowered or "lcff carryover" in lowered:
        return ""
    if re.fullmatch(r"Page \d+ of \d+", line, re.I):
        return ""
    if re.fullmatch(r"\d{4}-\d{2}", line):
        return ""
    if re.fullmatch(r"[\d\s.$,%()/+-]+", line):
        return ""
    return line


def is_template_page(text: str, page_number: int) -> bool:
    lowered = compact_text(text).casefold()
    if page_number < 20:
        return False
    hits = sum(1 for marker in TEMPLATE_MARKERS if marker in lowered)
    return hits >= 1


def is_budget_overview_page(text: str, page_number: int) -> bool:
    if page_number > 10:
        return False
    lowered = compact_text(text).casefold()
    return any(marker in lowered for marker in BUDGET_OVERVIEW_MARKERS)


def is_lcff_summary_page(text: str) -> bool:
    lowered = compact_text(text).casefold()
    return all(marker in lowered for marker in LCFF_SUMMARY_MARKERS)


def is_table_page(text: str) -> bool:
    lowered = compact_text(text).casefold()
    marker_hits = sum(1 for marker in TABLE_PAGE_MARKERS if marker in lowered)
    return marker_hits >= 2


def is_structured_table_start(line: str) -> bool:
    lowered = compact_text(line).casefold()
    return (
        ("action #" in lowered and "title" in lowered)
        or ("action #" in lowered and "total funds" in lowered)
        or ("metric #" in lowered and "baseline" in lowered)
        or ("goal #" in lowered and "description" in lowered and "type of goal" in lowered)
        or "measuring and reporting results" in lowered
        or "midiendo y reportando resultados" in lowered
        or ("# de meta" in lowered and "necesidades identificadas" in lowered)
        or ("# de medida" in lowered and "referente" in lowered)
        or ("year 1 outcome" in lowered and "target year 3" in lowered)
        or ("year 1 outcome" in lowered and "current difference outcome" in lowered)
        or ("resultado del año" in lowered and "diferencia actual" in lowered)
    )


def match_section(line: str) -> tuple[str, str] | None:
    if len(line) > 180:
        return None
    for section_type, label, pattern in SECTION_PATTERNS:
        if pattern.search(line):
            return section_type, label
    return None


def match_prompt(line: str) -> str | None:
    if len(line) > 360:
        return None
    for label, pattern in PROMPT_PATTERNS:
        if pattern.search(line):
            return label
    return None


def table_like_score(text: str) -> float:
    lines = [line for line in text.split("\n") if compact_text(line)]
    if not lines:
        return 1.0
    numeric_lines = 0
    header_lines = 0
    short_cells = 0
    for line in lines:
        if TABLE_HEADER_RE.search(line):
            header_lines += 1
        chars = re.sub(r"\s+", "", line)
        if chars:
            digit_ratio = sum(char.isdigit() for char in chars) / len(chars)
            if digit_ratio > 0.22 or "$" in line or "%" in line:
                numeric_lines += 1
        if len(line.split()) <= 4:
            short_cells += 1
    return max(header_lines / len(lines), numeric_lines / len(lines), short_cells / max(len(lines), 1) * 0.6)


def authored_confidence(text: str, document_match: int | None) -> float:
    score = 1.0
    score -= min(0.55, table_like_score(text) * 0.45)
    lowered = compact_text(text).casefold()
    if any(marker in lowered for marker in TEMPLATE_MARKERS):
        score -= 0.5
    if document_match == 0:
        score -= 0.25
    return round(max(0.0, min(1.0, score)), 3)


def useful_segment(text: str) -> bool:
    clean = compact_text(text)
    if estimate_tokens(clean) < MIN_CHUNK_TOKENS:
        return False
    if table_like_score(text) >= 0.45:
        return False
    lowered = clean.casefold()
    if any(marker in lowered for marker in TEMPLATE_MARKERS):
        return False
    if any(marker in lowered for marker in LCFF_SUMMARY_MARKERS):
        return False
    if (
        table_like_score(text) >= 0.32
        and any(marker in lowered for marker in ("action # total funds", "annual update table", "lcff carryover"))
    ):
        return False
    if "the budgeted expenditures for actions identified as contributing may be found" in lowered:
        return False
    return True


def clean_chunk_text(text: str) -> str:
    return normalize_text(CHUNK_TABLE_TAIL_RE.sub("", text))


def chunk_kind(section_type: str, prompt_label: str, action_number: str) -> str:
    if action_number:
        return "action_description"
    if prompt_label:
        return prompt_label
    return section_type


def split_text_units(text: str) -> list[str]:
    paragraphs = [compact_text(part) for part in re.split(r"\n\s*\n", text) if compact_text(part)]
    if len(paragraphs) > 1:
        units: list[str] = []
        for paragraph in paragraphs:
            if estimate_tokens(paragraph) > MAX_TOKENS:
                units.extend(sentence for sentence in re.split(r"(?<=[.!?])\s+", paragraph) if sentence)
            else:
                units.append(paragraph)
        return units

    lines = [compact_text(line) for line in text.split("\n") if compact_text(line)]
    if len(lines) > 1:
        return lines

    sentences = re.split(r"(?<=[.!?])\s+", compact_text(text))
    return [sentence for sentence in sentences if sentence]


def overlap_units(units: list[str]) -> list[str]:
    result: list[str] = []
    total = 0
    for unit in reversed(units):
        result.insert(0, unit)
        total += estimate_tokens(unit)
        if total >= OVERLAP_TOKENS:
            break
    return result


def split_segment(segment: Segment) -> list[dict[str, Any]]:
    units = split_text_units(segment.text)
    chunks: list[dict[str, Any]] = []
    current: list[str] = []
    current_tokens = 0

    def flush() -> None:
        nonlocal current, current_tokens
        body_text = clean_chunk_text("\n\n".join(current))
        if not body_text:
            return
        if not useful_segment(body_text):
            current = []
            current_tokens = 0
            return
        add_chunk(segment, body_text, len(chunks), chunks)
        current = overlap_units(current)
        current_tokens = sum(estimate_tokens(unit) for unit in current)

    for unit in units:
        unit_tokens = estimate_tokens(unit)
        if current and current_tokens + unit_tokens > MAX_TOKENS:
            flush()
        current.append(unit)
        current_tokens += unit_tokens
        if current_tokens >= TARGET_TOKENS:
            flush()

    if current:
        body_text = clean_chunk_text("\n\n".join(current))
        if useful_segment(body_text):
            add_chunk(segment, body_text, len(chunks), chunks)
    return chunks


def add_chunk(segment: Segment, body_text: str, chunk_index: int, chunks: list[dict[str, Any]]) -> None:
    text_hash = stable_hash(body_text, 24)
    section_id = stable_hash(
        "|".join(
            [
                segment.cds_code,
                segment.section_type,
                segment.section_path,
                segment.goal_number,
                segment.action_number,
            ]
        ),
        20,
    )
    chunk_id = stable_hash(
        "|".join(
            [
                segment.cds_code,
                section_id,
                str(segment.page_start),
                str(segment.page_end),
                str(chunk_index),
                text_hash,
            ]
        ),
        24,
    )
    breadcrumb = " | ".join(
        part
        for part in [
            f"District: {segment.district}",
            f"County: {segment.county}",
            f"Section: {segment.section_path}",
            f"Goal: {segment.goal_number}" if segment.goal_number else "",
            f"Action: {segment.action_number}" if segment.action_number else "",
            f"Prompt: {segment.prompt_label}" if segment.prompt_label else "",
        ]
        if part
    )
    search_text = f"{breadcrumb}\n\n{body_text}" if breadcrumb else body_text
    chunks.append(
        {
            "chunk_id": chunk_id,
            "section_id": section_id,
            "cds_code": segment.cds_code,
            "county": segment.county,
            "district": segment.district,
            "school_year": segment.school_year,
            "source_path": segment.source_path,
            "pdf_url": segment.pdf_url,
            "page_start": segment.page_start,
            "page_end": segment.page_end,
            "section_type": segment.section_type,
            "section_path": segment.section_path,
            "prompt_label": segment.prompt_label,
            "goal_number": segment.goal_number,
            "action_number": segment.action_number,
            "chunk_kind": segment.chunk_kind,
            "chunk_index": chunk_index,
            "token_count": estimate_tokens(search_text),
            "text_hash": text_hash,
            "authored_confidence": authored_confidence(body_text, segment.document_match),
            "body_text": body_text,
            "search_text": search_text,
        }
    )


def parse_document(record: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    path = Path(record["source_path"])
    if not path.exists():
        return [], [], {"cds_code": record.get("cds_code", ""), "status": "missing_pdf", "chunk_count": 0}

    pages = extract_pages(path)
    current_section_type = "unknown"
    current_section_path = "Unknown"
    current_prompt = ""
    current_goal = ""
    current_action = ""
    buffer: list[tuple[int, str]] = []
    segments: list[Segment] = []
    skipped_template_pages = 0
    template_started = False
    skipping_structured_table = False

    def flush() -> None:
        nonlocal buffer
        if not buffer:
            return
        text = normalize_text("\n".join(line for _, line in buffer))
        if current_section_type == "goals_actions" and not current_prompt and not current_action:
            buffer = []
            return
        page_start = min(page for page, _ in buffer)
        page_end = max(page for page, _ in buffer)
        if useful_segment(text):
            segments.append(
                Segment(
                    cds_code=compact_text(record.get("cds_code")),
                    county=compact_text(record.get("county")),
                    district=compact_text(record.get("district")),
                    school_year=compact_text(record.get("school_year")),
                    source_path=str(path),
                    pdf_url=compact_text(record.get("pdf_url")),
                    document_match=record.get("district_name_match"),
                    section_type=current_section_type,
                    section_path=current_section_path,
                    prompt_label=current_prompt,
                    goal_number=current_goal,
                    action_number=current_action,
                    chunk_kind=chunk_kind(current_section_type, current_prompt, current_action),
                    page_start=page_start,
                    page_end=page_end,
                    text=text,
                )
            )
        buffer = []

    for page in pages:
        if template_started:
            skipped_template_pages += 1
            continue
        if (
            is_template_page(page.text, page.page_number)
            or is_budget_overview_page(page.text, page.page_number)
            or is_table_page(page.text)
        ):
            if is_template_page(page.text, page.page_number):
                flush()
                template_started = True
            skipped_template_pages += 1
            continue

        raw_lines = [compact_text(line) for line in page.text.split("\n") if compact_text(line)]
        for line_index, raw_line in enumerate(raw_lines):
            if is_structured_table_start(raw_line):
                flush()
                skipping_structured_table = True
                continue

            line = clean_line(raw_line)
            if not line:
                continue

            section = match_section(line)
            if section and line_index > 8 and section[0] not in {"required_descriptions", "goal_analysis"}:
                section = None
            if section:
                flush()
                current_section_type, current_section_path = section
                current_prompt = ""
                current_action = ""
                skipping_structured_table = False
                continue

            if TABLE_HEADER_RE.search(line):
                continue

            goal_match = GOAL_RE.match(line)
            if goal_match and len(line) < 160 and "analysis" not in line.casefold():
                flush()
                current_goal = goal_match.group(1)
                current_action = ""
                current_prompt = ""
                if current_section_type == "unknown":
                    current_section_type = "goals_actions"
                    current_section_path = "Goals and Actions"
                continue

            action_match = ACTION_RE.match(line)
            if action_match and len(line) < 160:
                flush()
                current_action = ""
                current_prompt = ""
                continue

            prompt = match_prompt(line)
            if prompt and len(line.split()) >= 4:
                flush()
                current_prompt = prompt
                skipping_structured_table = False
                continue

            if skipping_structured_table:
                continue

            if current_section_type == "unknown":
                # Avoid indexing front-matter or accidental text before the first authored section.
                continue

            buffer.append((page.page_number, line))

    flush()

    chunks: list[dict[str, Any]] = []
    for segment in segments:
        chunks.extend(split_segment(segment))

    sections_by_id: dict[str, dict[str, Any]] = {}
    for chunk in chunks:
        section_id = chunk["section_id"]
        existing = sections_by_id.get(section_id)
        if not existing:
            sections_by_id[section_id] = {
                "section_id": section_id,
                "cds_code": chunk["cds_code"],
                "county": chunk["county"],
                "district": chunk["district"],
                "school_year": chunk["school_year"],
                "source_path": chunk["source_path"],
                "pdf_url": chunk["pdf_url"],
                "section_type": chunk["section_type"],
                "section_path": chunk["section_path"],
                "goal_number": chunk["goal_number"],
                "action_number": chunk["action_number"],
                "page_start": chunk["page_start"],
                "page_end": chunk["page_end"],
                "chunk_count": 1,
            }
            continue
        existing["page_start"] = min(existing["page_start"], chunk["page_start"])
        existing["page_end"] = max(existing["page_end"], chunk["page_end"])
        existing["chunk_count"] += 1

    summary = {
        "cds_code": record.get("cds_code", ""),
        "county": record.get("county", ""),
        "district": record.get("district", ""),
        "source_path": str(path),
        "status": "ok",
        "page_count": len(pages),
        "section_count": len(sections_by_id),
        "segment_count": len(segments),
        "chunk_count": len(chunks),
        "skipped_template_pages": skipped_template_pages,
    }
    return list(sections_by_id.values()), chunks, summary


def parse_source_pages(value: Any) -> tuple[int, int]:
    if not value:
        return 0, 0
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            numbers = [int(item) for item in re.findall(r"\d+", value)]
            return (min(numbers), max(numbers)) if numbers else (0, 0)
    if isinstance(value, list):
        numbers = [int(item) for item in value if str(item).isdigit()]
        return (min(numbers), max(numbers)) if numbers else (0, 0)
    return 0, 0


def structured_segments(db_path: Path, documents: list[dict[str, Any]]) -> list[Segment]:
    by_cds = {compact_text(record.get("cds_code")): record for record in documents}
    if not by_cds:
        return []
    placeholders = ", ".join("?" for _ in by_cds)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    segments: list[Segment] = []
    try:
        goals = connection.execute(
            f"""
            select *
            from lcap_goals
            where cds_code in ({placeholders})
              and trim(description) != ''
            """,
            list(by_cds),
        ).fetchall()
        for row in goals:
            record = by_cds.get(row["cds_code"], {})
            page_start, page_end = parse_source_pages(row["source_pages"])
            segments.append(
                Segment(
                    cds_code=row["cds_code"],
                    county=row["county"],
                    district=row["district"],
                    school_year=row["school_year"],
                    source_path=compact_text(record.get("source_path")),
                    pdf_url=compact_text(record.get("pdf_url")),
                    document_match=record.get("district_name_match"),
                    section_type="goals_actions",
                    section_path="Structured LCAP Goal",
                    prompt_label="goal_description",
                    goal_number=compact_text(row["goal_number"]),
                    action_number="",
                    chunk_kind="goal_description",
                    page_start=page_start,
                    page_end=page_end,
                    text=compact_text(row["description"]),
                )
            )

        actions = connection.execute(
            f"""
            select *
            from lcap_actions
            where cds_code in ({placeholders})
              and (trim(title) != '' or trim(description) != '')
            """,
            list(by_cds),
        ).fetchall()
        for row in actions:
            record = by_cds.get(row["cds_code"], {})
            page_start, page_end = parse_source_pages(row["source_pages"])
            title = compact_text(row["title"])
            description = compact_text(row["description"])
            text = f"{title}\n\n{description}" if title and description else title or description
            segments.append(
                Segment(
                    cds_code=row["cds_code"],
                    county=row["county"],
                    district=row["district"],
                    school_year=row["school_year"],
                    source_path=compact_text(record.get("source_path")),
                    pdf_url=compact_text(record.get("pdf_url")),
                    document_match=record.get("district_name_match"),
                    section_type="goals_actions",
                    section_path="Structured LCAP Action",
                    prompt_label="action_description",
                    goal_number=compact_text(row["goal_number"]),
                    action_number=compact_text(row["action_number"]),
                    chunk_kind="action_description",
                    page_start=page_start,
                    page_end=page_end,
                    text=text,
                )
            )
    finally:
        connection.close()
    return segments


def sections_from_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sections_by_id: dict[str, dict[str, Any]] = {}
    for chunk in chunks:
        section_id = chunk["section_id"]
        existing = sections_by_id.get(section_id)
        if not existing:
            sections_by_id[section_id] = {
                "section_id": section_id,
                "cds_code": chunk["cds_code"],
                "county": chunk["county"],
                "district": chunk["district"],
                "school_year": chunk["school_year"],
                "source_path": chunk["source_path"],
                "pdf_url": chunk["pdf_url"],
                "section_type": chunk["section_type"],
                "section_path": chunk["section_path"],
                "goal_number": chunk["goal_number"],
                "action_number": chunk["action_number"],
                "page_start": chunk["page_start"],
                "page_end": chunk["page_end"],
                "chunk_count": 1,
            }
            continue
        existing["page_start"] = min(existing["page_start"], chunk["page_start"])
        existing["page_end"] = max(existing["page_end"], chunk["page_end"])
        existing["chunk_count"] += 1
    return list(sections_by_id.values())


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({field for row in rows for field in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analytics-db", type=Path, default=DEFAULT_ANALYTICS_DB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--cds-code", help="Extract one district only.")
    parser.add_argument("--limit", type=int, help="Limit documents for smoke testing.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    documents = load_documents(args.analytics_db, args.cds_code, args.limit)
    all_sections: list[dict[str, Any]] = []
    all_chunks: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    for index, record in enumerate(documents, start=1):
        sections, chunks, summary = parse_document(record)
        all_sections.extend(sections)
        all_chunks.extend(chunks)
        summaries.append(summary)
        print(
            f"[{index}/{len(documents)}] {record.get('district', '')}: "
            f"{summary.get('chunk_count', 0)} chunks"
        )

    structured_chunks: list[dict[str, Any]] = []
    for segment in structured_segments(args.analytics_db, documents):
        structured_chunks.extend(split_segment(segment))
    all_chunks.extend(structured_chunks)
    all_sections = sections_from_chunks(all_chunks)

    write_jsonl(args.output_dir / "sections.jsonl", all_sections)
    write_jsonl(args.output_dir / "chunks.jsonl", all_chunks)
    write_summary_csv(args.output_dir / "extraction_summary.csv", summaries)
    summary = {
        "documents": len(documents),
        "sections": len(all_sections),
        "chunks": len(all_chunks),
        "tokens": sum(int(chunk.get("token_count") or 0) for chunk in all_chunks),
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
