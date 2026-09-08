from __future__ import annotations

import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "06_projectverslag.md"
OUTPUT = ROOT / "docs" / "06_projectverslag.pdf"


def register_fonts() -> tuple[str, str, str]:
    regular = Path(r"C:\Windows\Fonts\segoeui.ttf")
    bold = Path(r"C:\Windows\Fonts\segoeuib.ttf")
    mono = Path(r"C:\Windows\Fonts\consola.ttf")
    if regular.exists() and bold.exists() and mono.exists():
        pdfmetrics.registerFont(TTFont("SegoeUI", str(regular)))
        pdfmetrics.registerFont(TTFont("SegoeUI-Bold", str(bold)))
        pdfmetrics.registerFont(TTFont("Consolas", str(mono)))
        return "SegoeUI", "SegoeUI-Bold", "Consolas"
    return "Helvetica", "Helvetica-Bold", "Courier"


def inline_markup(value: str) -> str:
    value = html.escape(value, quote=False)
    value = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", value)
    value = re.sub(r"`([^`]+)`", r"<font name='Consolas'>\1</font>", value)
    value = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", value)
    value = re.sub(r"\*([^*]+)\*", r"<i>\1</i>", value)
    return value


def is_table_separator(line: str) -> bool:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells)


def table_rows(lines: list[str]) -> list[list[str]]:
    rows = []
    for line in lines:
        rows.append([cell.strip() for cell in line.strip().strip("|").split("|")])
    width = max(len(row) for row in rows)
    return [row + [""] * (width - len(row)) for row in rows]


def build_story(source: str, font_regular: str, font_bold: str, font_mono: str):
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="ReportTitle", parent=styles["Title"], fontName=font_bold,
        fontSize=22, leading=27, textColor=colors.HexColor("#17324D"),
        spaceAfter=12, alignment=TA_LEFT,
    ))
    styles.add(ParagraphStyle(
        name="H1Report", parent=styles["Heading1"], fontName=font_bold,
        fontSize=15, leading=19, textColor=colors.HexColor("#17324D"),
        spaceBefore=14, spaceAfter=7, keepWithNext=True,
    ))
    styles.add(ParagraphStyle(
        name="H2Report", parent=styles["Heading2"], fontName=font_bold,
        fontSize=11.5, leading=15, textColor=colors.HexColor("#2C5D7C"),
        spaceBefore=10, spaceAfter=5, keepWithNext=True,
    ))
    styles.add(ParagraphStyle(
        name="BodyReport", parent=styles["BodyText"], fontName=font_regular,
        fontSize=9.2, leading=13, spaceAfter=5,
    ))
    styles.add(ParagraphStyle(
        name="BulletReport", parent=styles["BodyText"], fontName=font_regular,
        fontSize=9.2, leading=12.5, leftIndent=13, firstLineIndent=-8, spaceAfter=3,
    ))
    styles.add(ParagraphStyle(
        name="SmallReport", parent=styles["BodyText"], fontName=font_regular,
        fontSize=7.5, leading=9.5,
    ))
    styles.add(ParagraphStyle(
        name="TableReport", parent=styles["BodyText"], fontName=font_regular,
        fontSize=7.2, leading=9,
    ))
    styles.add(ParagraphStyle(
        name="TableHeaderReport", parent=styles["BodyText"], fontName=font_bold,
        fontSize=7.2, leading=9, textColor=colors.white,
    ))
    styles.add(ParagraphStyle(
        name="QuoteReport", parent=styles["BodyText"], fontName=font_regular,
        fontSize=9, leading=12, leftIndent=14, borderPadding=7,
        borderColor=colors.HexColor("#B7C9D6"), borderWidth=0.5,
        backColor=colors.HexColor("#F0F5F8"), spaceAfter=7,
    ))

    story = []
    lines = source.splitlines()
    index = 0
    in_code = False
    code_lines: list[str] = []
    while index < len(lines):
        line = lines[index]
        if line.startswith("```"):
            if in_code:
                story.append(Preformatted("\n".join(code_lines), ParagraphStyle(
                    "Code", fontName=font_mono, fontSize=6.5, leading=8,
                    backColor=colors.HexColor("#F4F6F7"), borderColor=colors.HexColor("#D4DDE2"),
                    borderWidth=0.5, borderPadding=6, spaceBefore=4, spaceAfter=7,
                )))
                code_lines = []
                in_code = False
            else:
                in_code = True
            index += 1
            continue
        if in_code:
            code_lines.append(line)
            index += 1
            continue
        if not line.strip():
            index += 1
            continue
        if line.startswith("|") and index + 1 < len(lines) and is_table_separator(lines[index + 1]):
            raw_table = [line, lines[index + 1]]
            index += 2
            while index < len(lines) and lines[index].strip().startswith("|"):
                raw_table.append(lines[index])
                index += 1
            rows = table_rows(raw_table)
            table_data = []
            for row_number, row in enumerate(rows):
                style_name = "TableHeaderReport" if row_number == 0 else "TableReport"
                table_data.append([Paragraph(inline_markup(cell), styles[style_name]) for cell in row])
            table = Table(table_data, repeatRows=1, hAlign="LEFT", splitByRow=1)
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#285B78")),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#B8C6CE")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F3F7F9")]),
            ]))
            story.extend([Spacer(1, 3), table, Spacer(1, 6)])
            continue
        heading = re.match(r"^(#{1,3})\s+(.+)$", line)
        if heading:
            level = len(heading.group(1))
            text = inline_markup(heading.group(2))
            style = styles["ReportTitle"] if level == 1 and not story else styles["H1Report"] if level == 1 else styles["H2Report"]
            story.append(Paragraph(text, style))
            index += 1
            continue
        if line.startswith("> "):
            story.append(Paragraph(inline_markup(line[2:]), styles["QuoteReport"]))
        elif re.match(r"^\s*[-*]\s+", line):
            story.append(Paragraph("• " + inline_markup(re.sub(r"^\s*[-*]\s+", "", line)), styles["BulletReport"]))
        elif re.match(r"^\s*\d+\.\s+", line):
            story.append(Paragraph(inline_markup(line.strip()), styles["BulletReport"]))
        elif line.startswith("---"):
            story.append(Spacer(1, 4))
        else:
            story.append(Paragraph(inline_markup(line), styles["BodyReport"]))
        index += 1
    return story


def add_page(canvas, document):
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor("#D4DDE2"))
    canvas.line(18 * mm, 14 * mm, 192 * mm, 14 * mm)
    canvas.setFont("SegoeUI", 7)
    canvas.setFillColor(colors.HexColor("#63727C"))
    canvas.drawString(18 * mm, 9 * mm, "Contoso Lakehouse v2 | Projectverslag")
    canvas.drawRightString(192 * mm, 9 * mm, f"Pagina {document.page}")
    canvas.restoreState()


def main() -> None:
    regular, bold, mono = register_fonts()
    story = build_story(SOURCE.read_text(encoding="utf-8"), regular, bold, mono)
    document = SimpleDocTemplate(
        str(OUTPUT), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=16 * mm, bottomMargin=19 * mm, title="Contoso Lakehouse v2 - Projectverslag",
        author="Contoso Lakehouse projectteam",
    )
    document.build(story, onFirstPage=add_page, onLaterPages=add_page)
    print(f"Created {OUTPUT} ({OUTPUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()