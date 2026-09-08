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
    Image,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.graphics.shapes import Drawing, Line, Rect, String
from reportlab.lib.utils import ImageReader


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


def box(drawing: Drawing, x: float, y: float, width: float, label: str,
        fill: str, text_color: str = "#17324D") -> None:
    drawing.add(Rect(x, y, width, 31, rx=4, ry=4,
                     fillColor=colors.HexColor(fill),
                     strokeColor=colors.HexColor("#6C8798"), strokeWidth=0.8))
    drawing.add(String(x + width / 2, y + 12, label, textAnchor="middle",
                       fontName="Helvetica-Bold", fontSize=7.5,
                       fillColor=colors.HexColor(text_color)))


def arrow(drawing: Drawing, x1: float, y1: float, x2: float, y2: float) -> None:
    drawing.add(Line(x1, y1, x2, y2, strokeColor=colors.HexColor("#4E6878"),
                     strokeWidth=1.2, endArrow=True))


def architecture_diagram() -> Drawing:
    drawing = Drawing(480, 116)
    labels = [
        ("Landing", "#DCEBF2"), ("Bronze", "#DCEBF2"),
        ("Quality", "#E5F1E2"), ("Reject", "#F7E0DC"),
        ("Data Vault", "#E8E1F0"), ("Gold", "#F5EACB"),
    ]
    start_x = 5
    width = 70
    gap = 10
    for position, (label, fill) in enumerate(labels):
        x = start_x + position * (width + gap)
        box(drawing, x, 61, width, label, fill)
        if position < len(labels) - 1:
            arrow(drawing, x + width, 76, x + width + gap, 76)
    drawing.add(String(240, 104, "Contoso Lakehouse v2 - end-to-end architectuur",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    drawing.add(String(240, 41, "Metadata, audit en delivery-gate sturen alle lagen",
                       textAnchor="middle", fontName="Helvetica-Oblique", fontSize=8,
                       fillColor=colors.HexColor("#63727C")))
    box(drawing, 155, 4, 85, "Metadata + Audit", "#EAF0F3")
    arrow(drawing, 197, 35, 197, 60)
    box(drawing, 286, 4, 85, "Unity Catalog", "#EAF0F3")
    arrow(drawing, 328, 35, 328, 60)
    return drawing


def workflow_diagram() -> Drawing:
    drawing = Drawing(480, 118)
    labels = [
        ("Validate", "#DCEBF2"), ("Manifest", "#DCEBF2"),
        ("Bronze", "#DCEBF2"), ("Gate", "#F5EACB"),
        ("Quality", "#E5F1E2"), ("Vault", "#E8E1F0"),
        ("Gold", "#F5EACB"),
    ]
    start_x = 3
    width = 58
    gap = 11
    for position, (label, fill) in enumerate(labels):
        x = start_x + position * (width + gap)
        box(drawing, x, 62, width, label, fill)
        if position < len(labels) - 1:
            arrow(drawing, x + width, 77, x + width + gap, 77)
    drawing.add(String(240, 105, "Metadata-gedreven workflow en control plane",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 55, 8, 95, "Reject + Reconcile", "#F7E0DC")
    arrow(drawing, 82, 39, 82, 61)
    box(drawing, 190, 8, 95, "Audit + Work-items", "#EAF0F3")
    arrow(drawing, 237, 39, 237, 61)
    box(drawing, 325, 8, 95, "Monitoring + SLO", "#EAF0F3")
    arrow(drawing, 372, 39, 372, 61)
    return drawing


def metadata_diagram() -> Drawing:
    drawing = Drawing(480, 132)
    drawing.add(String(240, 119, "Metadata als besturingslaag", textAnchor="middle",
                       fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 182, 78, 116, "Metadata catalogus", "#E8E1F0")
    targets = [
        (18, 27, "Bronnen + objecten", "#DCEBF2"),
        (139, 27, "Mappings + DQ", "#E5F1E2"),
        (260, 27, "Vault + Gold", "#F5EACB"),
        (381, 27, "Audit + policies", "#EAF0F3"),
    ]
    for x, y, label, fill in targets:
        box(drawing, x, y, 100, label, fill)
        arrow(drawing, 240, 78, x + 50, 58)
    return drawing


def delivery_gate_diagram() -> Drawing:
    drawing = Drawing(480, 128)
    drawing.add(String(240, 116, "Delivery-gate: geen gedeeltelijke verwerking",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 12, 65, 92, "Datumfolder", "#DCEBF2")
    box(drawing, 124, 65, 92, "CLOSED manifest", "#DCEBF2")
    box(drawing, 236, 65, 92, "Readiness view", "#F5EACB")
    box(drawing, 348, 84, 112, "Gate open", "#E5F1E2")
    box(drawing, 348, 28, 112, "SKIPPED / wachten", "#F7E0DC")
    arrow(drawing, 104, 80, 124, 80)
    arrow(drawing, 216, 80, 236, 80)
    arrow(drawing, 328, 80, 348, 99)
    arrow(drawing, 328, 80, 348, 43)
    drawing.add(String(341, 108, "complete", fontName="Helvetica", fontSize=7,
                       fillColor=colors.HexColor("#376C42")))
    drawing.add(String(338, 19, "onvolledig", fontName="Helvetica", fontSize=7,
                       fillColor=colors.HexColor("#A34539")))
    return drawing


def vault_diagram() -> Drawing:
    drawing = Drawing(480, 130)
    drawing.add(String(240, 118, "Data Vault 2.0: historie zonder bronverlies",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 18, 70, 90, "Hubs / keys", "#E8E1F0")
    box(drawing, 128, 70, 90, "Links / relaties", "#E8E1F0")
    box(drawing, 238, 70, 90, "Satellites / historie", "#E8E1F0")
    box(drawing, 348, 70, 114, "Business Vault", "#F5EACB")
    arrow(drawing, 108, 85, 128, 85)
    arrow(drawing, 218, 85, 238, 85)
    arrow(drawing, 328, 85, 348, 85)
    box(drawing, 145, 20, 190, "SHA-256 + hashdiff + record source", "#EAF0F3")
    arrow(drawing, 193, 51, 193, 70)
    arrow(drawing, 287, 51, 287, 70)
    return drawing


def gold_publication_diagram() -> Drawing:
    drawing = Drawing(480, 130)
    drawing.add(String(240, 118, "Gold Actueel: atomische publicatie per groep",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 18, 69, 102, "Build _v2", "#DCEBF2")
    box(drawing, 138, 69, 102, "Validate group", "#E5F1E2")
    box(drawing, 258, 69, 102, "Releasepointer", "#F5EACB")
    box(drawing, 378, 69, 84, "Public views", "#F5EACB")
    arrow(drawing, 120, 84, 138, 84)
    arrow(drawing, 240, 84, 258, 84)
    arrow(drawing, 360, 84, 378, 84)
    box(drawing, 120, 18, 110, "Fout -> vorige versie", "#F7E0DC")
    arrow(drawing, 189, 69, 175, 49)
    return drawing


def source_routes_diagram() -> Drawing:
    drawing = Drawing(480, 132)
    drawing.add(String(240, 120, "Bronroutes naar dezelfde generieke keten",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 18, 76, 100, "Sales files", "#DCEBF2")
    box(drawing, 18, 28, 100, "API / JDBC pulls", "#DCEBF2")
    box(drawing, 190, 52, 105, "Landing Volume", "#DCEBF2")
    box(drawing, 365, 52, 100, "Bronze -> Gold", "#F5EACB")
    arrow(drawing, 118, 91, 190, 75)
    arrow(drawing, 118, 43, 190, 65)
    arrow(drawing, 295, 68, 365, 68)
    return drawing


def validation_diagram() -> Drawing:
    drawing = Drawing(480, 128)
    drawing.add(String(240, 116, "Validatieketen: van Git tot runtime",
                       textAnchor="middle", fontName="Helvetica-Bold", fontSize=9,
                       fillColor=colors.HexColor("#17324D")))
    box(drawing, 18, 65, 102, "Release-check", "#DCEBF2")
    box(drawing, 138, 65, 102, "Pytest 131", "#E5F1E2")
    box(drawing, 258, 65, 102, "Bundle + setup", "#DCEBF2")
    box(drawing, 378, 65, 84, "Dev runtime", "#F5EACB")
    arrow(drawing, 120, 80, 138, 80)
    arrow(drawing, 240, 80, 258, 80)
    arrow(drawing, 360, 80, 378, 80)
    box(drawing, 154, 18, 172, "Bewijs: logs, audit, counts", "#EAF0F3")
    arrow(drawing, 309, 65, 278, 49)
    return drawing


def diagrams_for_heading(heading_text: str) -> list[Drawing]:
    if heading_text.startswith("5. Architectuur"):
        return [metadata_diagram(), source_routes_diagram(), vault_diagram(), delivery_gate_diagram(), gold_publication_diagram()]
    if heading_text.startswith("7. Validatie"):
        return [validation_diagram()]
    return []


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
            if level == 1 and not any(isinstance(item, Drawing) for item in story):
                story.append(Paragraph("Architectuur in beeld", styles["H2Report"]))
                story.append(architecture_diagram())
                story.append(Spacer(1, 7))
                story.append(workflow_diagram())
                story.append(Spacer(1, 8))
            for diagram in diagrams_for_heading(heading.group(2)):
                story.append(Spacer(1, 5))
                story.append(diagram)
                story.append(Spacer(1, 7))
            index += 1
            continue
        image_match = re.match(r"^!\[[^]]*\]\(([^)]+)\)$", line.strip())
        if image_match:
            image_path = SOURCE.parent / image_match.group(1)
            if image_path.exists():
                image_width, image_height = ImageReader(str(image_path)).getSize()
                scale = min((174 * mm) / image_width, (42 * mm) / image_height)
                story.append(Image(
                    str(image_path),
                    width=image_width * scale,
                    height=image_height * scale,
                    hAlign="CENTER",
                ))
                story.append(Spacer(1, 8))
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

    screenshots = [
        ("Overzicht", "overzicht.png"),
    ]
    screenshot_dir = ROOT / "docs" / "app-screenshots"
    available_screenshots = [
        (label, screenshot_dir / filename)
        for label, filename in screenshots
        if (screenshot_dir / filename).exists()
    ]
    if available_screenshots:
        story.append(PageBreak())
        story.append(Paragraph("Bijlage A - Schermafdruk van de Streamlit-app", styles["H1Report"]))
        story.append(Paragraph(
            "Het onderstaande scherm toont het eerste overzicht van de Contoso Control Room.",
            styles["BodyReport"],
        ))
        for label, screenshot_path in available_screenshots:
            story.append(Paragraph(label, styles["H2Report"]))
            image_width, image_height = ImageReader(str(screenshot_path)).getSize()
            scale = min((174 * mm) / image_width, (235 * mm) / image_height)
            story.append(Image(
                str(screenshot_path),
                width=image_width * scale,
                height=image_height * scale,
                hAlign="CENTER",
            ))
            story.append(Spacer(1, 8))
    return story


def add_page(canvas, document):
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor("#D4DDE2"))
    canvas.line(18 * mm, 14 * mm, 192 * mm, 14 * mm)
    canvas.setFont("SegoeUI", 7)
    canvas.setFillColor(colors.HexColor("#63727C"))
    canvas.drawString(18 * mm, 9 * mm, "Contoso Lakehouse | Praktijkexperiment")
    canvas.drawRightString(192 * mm, 9 * mm, f"Pagina {document.page}")
    canvas.restoreState()


def main() -> None:
    regular, bold, mono = register_fonts()
    story = build_story(SOURCE.read_text(encoding="utf-8"), regular, bold, mono)
    document = SimpleDocTemplate(
        str(OUTPUT), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm,
        topMargin=16 * mm, bottomMargin=19 * mm, title="Contoso Lakehouse - Praktijkexperiment",
        author="Contoso Lakehouse projectteam",
    )
    document.build(story, onFirstPage=add_page, onLaterPages=add_page)
    print(f"Created {OUTPUT} ({OUTPUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()