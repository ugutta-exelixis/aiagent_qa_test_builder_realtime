"""Generate the management/client presentation deck.

Run:
    python tests/presentation/build_deck.py

Output:
    tests/presentation/AI_Test_Automation_Executive_Deck.pptx
"""

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

OUT = Path(__file__).with_name("AI_Test_Automation_Executive_Deck.pptx")

# ── Brand palette ─────────────────────────────────────────────────────────────
NAVY = RGBColor(0x0B, 0x25, 0x45)
NAVY_LT = RGBColor(0x1B, 0x3A, 0x5F)
TEAL = RGBColor(0x00, 0xA8, 0xA0)
TEAL_LT = RGBColor(0xD8, 0xF2, 0xF0)
AMBER = RGBColor(0xE8, 0xA3, 0x3D)
AMBER_LT = RGBColor(0xFD, 0xF1, 0xDE)
GREEN = RGBColor(0x2E, 0x9E, 0x5B)
GREEN_LT = RGBColor(0xE2, 0xF4, 0xE9)
RED = RGBColor(0xC0, 0x39, 0x3B)
RED_LT = RGBColor(0xFA, 0xE7, 0xE7)
GREY = RGBColor(0x5A, 0x66, 0x75)
GREY_LT = RGBColor(0xF2, 0xF4, 0xF7)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)

W, H = Inches(13.333), Inches(7.5)

prs = Presentation()
prs.slide_width = W
prs.slide_height = H
BLANK = prs.slide_layouts[6]


# ── Primitives ────────────────────────────────────────────────────────────────

def _txbox(slide, x, y, w, h, text, size=18, bold=False, color=NAVY,
           align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, font="Segoe UI",
           line_spacing=1.15, space_after=6):
    box = slide.shapes.add_textbox(x, y, w, h)
    tf = box.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Emu(0)
    tf.margin_top = tf.margin_bottom = Emu(0)

    lines = text if isinstance(text, list) else [text]
    for i, line in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        p.line_spacing = line_spacing
        p.space_after = Pt(space_after)
        if isinstance(line, tuple):
            content, opts = line
        else:
            content, opts = line, {}
        run = p.add_run()
        run.text = content
        f = run.font
        f.name = opts.get("font", font)
        f.size = Pt(opts.get("size", size))
        f.bold = opts.get("bold", bold)
        f.color.rgb = opts.get("color", color)
    return box


def _rect(slide, x, y, w, h, fill=WHITE, line=None, shape=MSO_SHAPE.ROUNDED_RECTANGLE,
          adj=0.06, shadow=False):
    sh = slide.shapes.add_shape(shape, x, y, w, h)
    sh.fill.solid()
    sh.fill.fore_color.rgb = fill
    if line is None:
        sh.line.fill.background()
    else:
        sh.line.color.rgb = line
        sh.line.width = Pt(1.25)
    sh.shadow.inherit = shadow
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE:
        try:
            sh.adjustments[0] = adj
        except Exception:
            pass
    sh.text_frame.text = ""
    return sh


def _slide(title=None, eyebrow=None, subtitle=None):
    s = prs.slides.add_slide(BLANK)
    # top accent bar
    bar = _rect(s, Inches(0), Inches(0), W, Inches(0.11), fill=TEAL, shape=MSO_SHAPE.RECTANGLE)
    bar.line.fill.background()
    y = Inches(0.42)
    if eyebrow:
        _txbox(s, Inches(0.75), y, Inches(11.8), Inches(0.28), eyebrow.upper(),
               size=11, bold=True, color=TEAL)
        y = Inches(0.75)
    if title:
        _txbox(s, Inches(0.75), y, Inches(11.8), Inches(0.62), title, size=30, bold=True)
        y = y + Inches(0.66)
    if subtitle:
        _txbox(s, Inches(0.75), y, Inches(11.8), Inches(0.42), subtitle, size=14.5, color=GREY)
    return s


def _footer(slide, n, note="AI Test Automation  ·  Clinical Data Platform"):
    _rect(slide, Inches(0), H - Inches(0.5), W, Inches(0.5), fill=GREY_LT,
          shape=MSO_SHAPE.RECTANGLE).line.fill.background()
    _txbox(slide, Inches(0.75), H - Inches(0.37), Inches(9), Inches(0.25),
           note, size=9.5, color=GREY)
    _txbox(slide, Inches(11.6), H - Inches(0.37), Inches(1), Inches(0.25),
           str(n), size=9.5, color=GREY, align=PP_ALIGN.RIGHT)


def _metric(slide, x, y, w, h, value, label, sub=None, accent=TEAL, fill=WHITE):
    card = _rect(slide, x, y, w, h, fill=fill, line=None, shadow=True)
    _rect(slide, x, y, Inches(0.07), h, fill=accent, shape=MSO_SHAPE.RECTANGLE)
    _txbox(slide, x + Inches(0.32), y + Inches(0.22), w - Inches(0.5), Inches(0.75),
           value, size=34, bold=True, color=accent)
    _txbox(slide, x + Inches(0.32), y + Inches(1.02), w - Inches(0.5), Inches(0.5),
           label, size=12.5, bold=True, color=NAVY)
    if sub:
        _txbox(slide, x + Inches(0.32), y + Inches(1.45), w - Inches(0.5), Inches(0.6),
               sub, size=10.5, color=GREY)
    return card


def _chip(slide, x, y, w, h, text, fill, text_color=WHITE, size=12.5, bold=True):
    sh = _rect(slide, x, y, w, h, fill=fill)
    tf = sh.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    tf.margin_left = tf.margin_right = Inches(0.08)
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = text
    r.font.size = Pt(size)
    r.font.bold = bold
    r.font.name = "Segoe UI"
    r.font.color.rgb = text_color
    return sh


def _arrow(slide, x, y, w=Inches(0.34), h=Inches(0.3), color=GREY):
    a = slide.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, x, y, w, h)
    a.fill.solid()
    a.fill.fore_color.rgb = color
    a.line.fill.background()
    a.shadow.inherit = False
    return a


def _bullets(slide, x, y, w, items, size=15, gap=0.52, marker_color=TEAL):
    for i, item in enumerate(items):
        yy = y + Inches(gap * i)
        dot = _rect(slide, x, yy + Inches(0.09), Inches(0.13), Inches(0.13),
                    fill=marker_color, shape=MSO_SHAPE.OVAL)
        dot.line.fill.background()
        if isinstance(item, tuple):
            head, tail = item
            _txbox(slide, x + Inches(0.32), yy, w, Inches(0.45),
                   [(head, {"bold": True, "size": size, "color": NAVY}),
                    (tail, {"size": size - 1.5, "color": GREY})],
                   line_spacing=1.1, space_after=0)
        else:
            _txbox(slide, x + Inches(0.32), yy, w, Inches(0.4), item, size=size, color=NAVY)


def _table(slide, x, y, w, rows, col_w, header_fill=NAVY, row_h=Inches(0.42),
           head_h=Inches(0.48), size=12, zebra=True):
    """rows[0] is the header. col_w is a list of Inches widths."""
    cy = y
    for r_i, row in enumerate(rows):
        cx = x
        is_head = r_i == 0
        h = head_h if is_head else row_h
        if is_head:
            fill = header_fill
        elif zebra and r_i % 2 == 0:
            fill = GREY_LT
        else:
            fill = WHITE
        band = _rect(slide, x, cy, w, h, fill=fill, shape=MSO_SHAPE.RECTANGLE)
        band.line.fill.background()
        for c_i, cell in enumerate(row):
            cw = col_w[c_i]
            if isinstance(cell, tuple):
                text, opts = cell
            else:
                text, opts = cell, {}
            _txbox(slide, cx + Inches(0.14), cy + Inches(0.11), cw - Inches(0.2),
                   h - Inches(0.14), text,
                   size=opts.get("size", size),
                   bold=opts.get("bold", is_head),
                   color=opts.get("color", WHITE if is_head else NAVY),
                   align=opts.get("align", PP_ALIGN.LEFT),
                   anchor=MSO_ANCHOR.MIDDLE, space_after=0)
            cx += cw
        cy += h
    return cy


N = 0


def num(s):
    global N
    N += 1
    _footer(s, N)
    return s


# ═══════════════════════════════════════════════════════════════════════════════
# 1 · TITLE
# ═══════════════════════════════════════════════════════════════════════════════
s = prs.slides.add_slide(BLANK)
_rect(s, Inches(0), Inches(0), W, H, fill=NAVY, shape=MSO_SHAPE.RECTANGLE)
_rect(s, Inches(0), Inches(0), Inches(0.22), H, fill=TEAL, shape=MSO_SHAPE.RECTANGLE)
# decorative blocks
for i, (dx, op) in enumerate([(9.6, TEAL), (10.9, NAVY_LT), (12.2, TEAL)]):
    b = _rect(s, Inches(dx), Inches(5.4), Inches(0.95), Inches(0.95), fill=op)
    b.line.fill.background()

_txbox(s, Inches(0.95), Inches(1.85), Inches(9.5), Inches(0.35),
       "EXECUTIVE BRIEFING  ·  SEPTEMBER 2026", size=13, bold=True, color=TEAL)
_txbox(s, Inches(0.95), Inches(2.35), Inches(10.5), Inches(1.9),
       ["Accelerating Software Quality with AI",
        ("Automated Test Engineering for the Clinical Data Platform",
         {"size": 21, "bold": False, "color": RGBColor(0xC7, 0xD3, 0xE0)})],
       size=42, bold=True, color=WHITE, line_spacing=1.05, space_after=14)

_rect(s, Inches(0.95), Inches(4.55), Inches(2.4), Inches(0.05), fill=TEAL,
      shape=MSO_SHAPE.RECTANGLE)

_txbox(s, Inches(0.95), Inches(4.95), Inches(9.5), Inches(1.2),
       ["20 days of engineering work delivered in 2 days.",
        ("2 live defects found — one affecting patient-data protection.",
         {"size": 16, "bold": False, "color": RGBColor(0xC7, 0xD3, 0xE0)})],
       size=19, bold=True, color=WHITE, space_after=8)

_txbox(s, Inches(0.95), Inches(6.6), Inches(9), Inches(0.3),
       "Prepared by Data Engineering  ·  DDDA Clinical Data Platform (Koios)",
       size=11, color=RGBColor(0x8F, 0xA3, 0xB8))


# ═══════════════════════════════════════════════════════════════════════════════
# 2 · THE ASK / AGENDA
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="What we will cover", eyebrow="Agenda",
               subtitle="A ten-minute walkthrough of the pilot, the savings, and the decision we are asking for"))

items = [
    ("01", "The problem", "Why quality assurance kept getting deferred"),
    ("02", "What we did", "An AI assistant that writes and proves its own tests"),
    ("03", "The results", "146 tests · 2 defects found · 9-second run time"),
    ("04", "The time saved", "90% reduction — and an honest view of the number"),
    ("05", "Safety & control", "Where your code goes, and where it does not"),
    ("06", "The recommendation", "What we are asking you to approve"),
]
x0, y0 = Inches(0.75), Inches(2.0)
for i, (n_, head, sub) in enumerate(items):
    col, row = i % 3, i // 3
    x = x0 + Inches(4.05 * col)
    y = y0 + Inches(2.05 * row)
    _rect(s, x, y, Inches(3.75), Inches(1.75), fill=WHITE, shadow=True)
    _rect(s, x, y, Inches(3.75), Inches(0.07), fill=TEAL, shape=MSO_SHAPE.RECTANGLE)
    _txbox(s, x + Inches(0.28), y + Inches(0.26), Inches(1), Inches(0.4), n_,
           size=15, bold=True, color=TEAL)
    _txbox(s, x + Inches(0.28), y + Inches(0.68), Inches(3.2), Inches(0.4), head,
           size=16, bold=True)
    _txbox(s, x + Inches(0.28), y + Inches(1.08), Inches(3.25), Inches(0.6), sub,
           size=11.5, color=GREY)


# ═══════════════════════════════════════════════════════════════════════════════
# 3 · HEADLINE RESULTS
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="The result in one slide", eyebrow="Executive summary",
               subtitle="A four-week manual undertaking, delivered in two supervised days"))

cards = [
    ("146", "Automated tests created", "Across all three data layers", TEAL),
    ("90%", "Reduction in effort", "20 days → 2 days", GREEN),
    ("2", "Live defects found", "One affecting patient-data protection", RED),
    ("$0", "Cloud cost to run tests", "Runs in 9 seconds, no data cluster", NAVY),
]
for i, (v, l, sub, c) in enumerate(cards):
    _metric(s, Inches(0.75 + 3.05 * i), Inches(2.1), Inches(2.85), Inches(2.15),
            v, l, sub, accent=c)

band = _rect(s, Inches(0.75), Inches(4.75), Inches(11.8), Inches(1.65),
             fill=NAVY, shadow=True)
_txbox(s, Inches(1.15), Inches(5.0), Inches(11.1), Inches(1.2),
       ["The pilot did not just save time — it found problems that manual review had missed.",
        ("Both defects were already live in our development environment. Neither had been caught by code review.",
         {"size": 14, "bold": False, "color": RGBColor(0xC7, 0xD3, 0xE0)})],
       size=19, bold=True, color=WHITE, space_after=10)


# ═══════════════════════════════════════════════════════════════════════════════
# 4 · THE PROBLEM
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="Why this needed solving", eyebrow="The problem",
               subtitle="The Clinical Data Platform processes regulated trial data — with effectively no automated safety net"))

rows = [
    ["Without automated tests", "Business consequence"],
    ["Defects surface only when a pipeline fails", "Delays, rework, expensive re-runs"],
    ["Engineers cannot safely modernise code", "Technical debt compounds; delivery slows"],
    ["Quality evidence is manual and inconsistent", "Weak position in audit and regulatory review"],
    ["Business rules live in people's heads", "Key-person dependency; painful onboarding"],
    ["Every change carries unquantified risk", "Teams become cautious; velocity drops"],
]
_table(s, Inches(0.75), Inches(2.05), Inches(7.4), rows,
       [Inches(4.0), Inches(3.4)], row_h=Inches(0.5), size=12.5)

_rect(s, Inches(8.55), Inches(2.05), Inches(4.0), Inches(3.5), fill=AMBER_LT, shadow=True)
_rect(s, Inches(8.55), Inches(2.05), Inches(4.0), Inches(0.08), fill=AMBER,
      shape=MSO_SHAPE.RECTANGLE)
_txbox(s, Inches(8.9), Inches(2.4), Inches(3.35), Inches(0.4),
       "Why wasn't it solved?", size=17, bold=True)
_txbox(s, Inches(8.9), Inches(2.95), Inches(3.35), Inches(2.4),
       ["Not neglect — economics.",
        ("Thorough testing of complex data pipelines takes a senior engineer 2–3 days per module. "
         "With competing deadlines, it is perpetually deferred.",
         {"size": 13, "bold": False, "color": GREY}),
        ("The constraint was never willingness. It was cost.",
         {"size": 14, "bold": True, "color": NAVY})],
       size=15, bold=True, space_after=12)

_rect(s, Inches(0.75), Inches(5.85), Inches(11.8), Inches(0.75), fill=NAVY)
_txbox(s, Inches(1.15), Inches(6.05), Inches(11.1), Inches(0.4),
       "AI changes that equation.", size=18, bold=True, color=WHITE)


# ═══════════════════════════════════════════════════════════════════════════════
# 5 · HOW IT WORKS (loop)
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="How the AI assistant works", eyebrow="What we did",
               subtitle="It works the way a good engineer works — and, critically, it proves its own work"))

steps = [("1", "Read\nthe code"), ("2", "Design the\ntest approach"),
         ("3", "Write\nthe tests"), ("4", "Run\nthe tests"),
         ("5", "Correct\nitself"), ("6", "Human\napproves")]
x = Inches(0.75)
y = Inches(2.35)
bw, bh = Inches(1.72), Inches(1.4)
for i, (n_, label) in enumerate(steps):
    is_human = i == 5
    fill = AMBER if is_human else TEAL
    _chip(s, x, y, bw, bh, label.replace("\n", "\n"), fill=fill, size=13.5)
    _txbox(s, x, y - Inches(0.34), bw, Inches(0.3), f"STEP {n_}", size=10,
           bold=True, color=GREY, align=PP_ALIGN.CENTER)
    if i < len(steps) - 1:
        _arrow(s, x + bw + Inches(0.11), y + Inches(0.55), Inches(0.32), Inches(0.3),
               color=GREY)
    x += bw + Inches(0.55)

# feedback loop annotation
_rect(s, Inches(0.75), Inches(4.15), Inches(9.0), Inches(0.06), fill=TEAL,
      shape=MSO_SHAPE.RECTANGLE)
_txbox(s, Inches(0.75), Inches(4.25), Inches(9.0), Inches(0.35),
       "◀ If a test fails, the assistant reads the real error and corrects itself — then runs again",
       size=12, bold=True, color=TEAL)

_rect(s, Inches(0.75), Inches(4.95), Inches(11.8), Inches(1.5), fill=GREEN_LT, shadow=True)
_rect(s, Inches(0.75), Inches(4.95), Inches(0.08), Inches(1.5), fill=GREEN,
      shape=MSO_SHAPE.RECTANGLE)
_txbox(s, Inches(1.15), Inches(5.2), Inches(11.1), Inches(1.1),
       ["The critical difference from ordinary AI tools",
        ("This assistant runs the tests and sees real results. It is not guessing at plausible-looking "
         "code — it proves its work, corrects itself when wrong, and only then brings the outcome to a human.",
         {"size": 13.5, "bold": False, "color": GREY})],
       size=16, bold=True, space_after=8)


# ═══════════════════════════════════════════════════════════════════════════════
# 6 · ARCHITECTURE
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="Where everything happens", eyebrow="Architecture",
               subtitle="Three things to take from this picture — and the third is the most important"))

# Secure boundary
_rect(s, Inches(0.75), Inches(1.95), Inches(7.55), Inches(3.55), fill=TEAL_LT)
_txbox(s, Inches(1.0), Inches(2.12), Inches(7.0), Inches(0.3),
       "OUR SECURE ENVIRONMENT", size=11.5, bold=True, color=TEAL)

inner = [("Our code", "never leaves"), ("AI assistant", "reads · writes · runs"),
         ("146 tests", "9 seconds"), ("Human engineer", "reviews & approves")]
bx = Inches(1.05)
for i, (t, sub) in enumerate(inner):
    fill = AMBER if i == 3 else (NAVY if i == 0 else TEAL)
    sh = _rect(s, bx, Inches(2.6), Inches(1.6), Inches(1.5), fill=fill)
    _txbox(s, bx + Inches(0.1), Inches(2.9), Inches(1.4), Inches(0.5), t,
           size=13, bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    _txbox(s, bx + Inches(0.1), Inches(3.4), Inches(1.4), Inches(0.5), sub,
           size=10, color=RGBColor(0xDD, 0xEE, 0xEE), align=PP_ALIGN.CENTER)
    if i < 3:
        _arrow(s, bx + Inches(1.68), Inches(3.2), Inches(0.28), Inches(0.28), color=TEAL)
    bx += Inches(1.85)

_txbox(s, Inches(1.05), Inches(4.35), Inches(7.0), Inches(0.9),
       ["Enterprise agreement: our code is never used to train public AI models.",
        ("Analysis happens inside our own controlled boundary.",
         {"size": 11.5, "bold": False, "color": GREY})],
       size=12.5, bold=True, space_after=4)

# Blocked zone
_rect(s, Inches(8.7), Inches(1.95), Inches(3.85), Inches(3.55), fill=RED_LT)
_txbox(s, Inches(8.95), Inches(2.12), Inches(3.4), Inches(0.3),
       "DELIBERATELY UNREACHABLE", size=11.5, bold=True, color=RED)
blocked = ["Clinical patient data", "Production systems",
           "Cloud data warehouse", "Live credentials"]
for i, b in enumerate(blocked):
    _chip(s, Inches(8.95), Inches(2.55 + 0.68 * i), Inches(3.35), Inches(0.52),
          b, fill=WHITE, text_color=RED, size=12, bold=True)

_txbox(s, Inches(8.35), Inches(3.05), Inches(0.5), Inches(0.4), "⛔",
       size=22, align=PP_ALIGN.CENTER)

_rect(s, Inches(0.75), Inches(5.7), Inches(11.8), Inches(0.85), fill=NAVY)
_txbox(s, Inches(1.1), Inches(5.86), Inches(11.2), Inches(0.55),
       "1 · Code stays inside our boundary   ●   2 · The AI cannot reach patient data   ●   "
       "3 · A qualified human always approves",
       size=14.5, bold=True, color=WHITE)


# ═══════════════════════════════════════════════════════════════════════════════
# 7 · DELIVERY WORKFLOW
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="From request to release", eyebrow="Workflow",
               subtitle="Blue is automated. Amber is a human decision. Green is the business outcome."))

flow = [("Engineer raises\na request", GREY),
        ("AI builds\nthe tests", TEAL),
        ("Tests run\nautomatically", TEAL),
        ("Results presented\nfor review", TEAL),
        ("Engineer reviews\n& approves", AMBER),
        ("Automated safety\nchecks pass", TEAL),
        ("Quality Lead\nauthorises", AMBER),
        ("Promoted\nto UAT", GREEN)]

x = Inches(0.62)
bw = Inches(1.35)
for i, (label, c) in enumerate(flow):
    _chip(s, x, Inches(2.55), bw, Inches(1.45), label, fill=c, size=11.5)
    if i < len(flow) - 1:
        _arrow(s, x + bw + Inches(0.06), Inches(3.13), Inches(0.26), Inches(0.28),
               color=RGBColor(0xB6, 0xC0, 0xCC))
    x += bw + Inches(0.44)

legend = [("Automated", TEAL), ("Human decision", AMBER), ("Business outcome", GREEN)]
lx = Inches(0.75)
for label, c in legend:
    d = _rect(s, lx, Inches(4.42), Inches(0.22), Inches(0.22), fill=c,
              shape=MSO_SHAPE.OVAL)
    d.line.fill.background()
    _txbox(s, lx + Inches(0.34), Inches(4.4), Inches(2.2), Inches(0.3), label,
           size=12, color=GREY)
    lx += Inches(2.5)

_rect(s, Inches(0.75), Inches(5.05), Inches(11.8), Inches(1.45), fill=AMBER_LT, shadow=True)
_rect(s, Inches(0.75), Inches(5.05), Inches(0.08), Inches(1.45), fill=AMBER,
      shape=MSO_SHAPE.RECTANGLE)
_txbox(s, Inches(1.15), Inches(5.3), Inches(11.1), Inches(1.0),
       ["The governing principle: the AI qualifies the work — a human authorises it.",
        ("We have deliberately not automated the release decision. For regulated clinical data, a passing "
         "test suite earns a candidate the right to be considered. It never authorises the release itself.",
         {"size": 13, "bold": False, "color": GREY})],
       size=16, bold=True, space_after=8)


# ═══════════════════════════════════════════════════════════════════════════════
# 8 · TIME SAVED — TABLE
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="Where the time is saved", eyebrow="The numbers",
               subtitle="Effort comparison for the pilot — one senior engineer, three data layers"))

rows = [
    ["Activity", ("Manual", {"align": PP_ALIGN.CENTER}), ("AI-assisted", {"align": PP_ALIGN.CENTER}),
     ("Saved", {"align": PP_ALIGN.CENTER})],
    ["Understanding the existing code", ("1 day", {"align": PP_ALIGN.CENTER}),
     ("0.1 day", {"align": PP_ALIGN.CENTER}), ("90%", {"align": PP_ALIGN.CENTER, "bold": True, "color": GREEN})],
    ["Designing the test approach", ("3.5 days", {"align": PP_ALIGN.CENTER}),
     ("0.5 day", {"align": PP_ALIGN.CENTER}), ("86%", {"align": PP_ALIGN.CENTER, "bold": True, "color": GREEN})],
    ["Tests — data ingestion layer", ("2 days", {"align": PP_ALIGN.CENTER}),
     ("0.2 day", {"align": PP_ALIGN.CENTER}), ("90%", {"align": PP_ALIGN.CENTER, "bold": True, "color": GREEN})],
    ["Tests — data quality layer", ("5 days", {"align": PP_ALIGN.CENTER}),
     ("0.4 day", {"align": PP_ALIGN.CENTER}), ("92%", {"align": PP_ALIGN.CENTER, "bold": True, "color": GREEN})],
    ["Tests — reporting layer", ("5 days", {"align": PP_ALIGN.CENTER}),
     ("0.4 day", {"align": PP_ALIGN.CENTER}), ("92%", {"align": PP_ALIGN.CENTER, "bold": True, "color": GREEN})],
    ["Debugging, fixing, documenting", ("3 days", {"align": PP_ALIGN.CENTER}),
     ("0.3 day", {"align": PP_ALIGN.CENTER}), ("90%", {"align": PP_ALIGN.CENTER, "bold": True, "color": GREEN})],
    [("Human review (does not compress)", {"color": AMBER}), ("—", {"align": PP_ALIGN.CENTER}),
     ("1 day", {"align": PP_ALIGN.CENTER, "bold": True, "color": AMBER}),
     ("—", {"align": PP_ALIGN.CENTER})],
]
_table(s, Inches(0.75), Inches(2.0), Inches(8.6), rows,
       [Inches(4.1), Inches(1.5), Inches(1.5), Inches(1.5)], row_h=Inches(0.44), size=12)

# total band
ty = Inches(2.0) + Inches(0.48) + Inches(0.44 * 7)
_rect(s, Inches(0.75), ty, Inches(8.6), Inches(0.6), fill=NAVY, shape=MSO_SHAPE.RECTANGLE)
for txt, cx, cw, al in [("TOTAL", Inches(0.89), Inches(4.0), PP_ALIGN.LEFT),
                        ("≈ 20 days", Inches(4.85), Inches(1.5), PP_ALIGN.CENTER),
                        ("≈ 2 days", Inches(6.35), Inches(1.5), PP_ALIGN.CENTER),
                        ("≈ 90%", Inches(7.85), Inches(1.5), PP_ALIGN.CENTER)]:
    _txbox(s, cx, ty + Inches(0.15), cw, Inches(0.35), txt, size=14, bold=True,
           color=WHITE, align=al)

_metric(s, Inches(9.75), Inches(2.0), Inches(2.8), Inches(1.9), "18",
        "Engineering days saved", "On this single pilot", accent=GREEN)
_metric(s, Inches(9.75), Inches(4.05), Inches(2.8), Inches(1.9), "40–60",
        "Days released per year", "Projected, team of six", accent=TEAL)


# ═══════════════════════════════════════════════════════════════════════════════
# 9 · VISUAL BAR COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="Twenty days, or two", eyebrow="The compression",
               subtitle="Elapsed working days to a complete, verified and reviewed test suite"))

_txbox(s, Inches(0.75), Inches(2.35), Inches(2.3), Inches(0.4), "MANUAL APPROACH",
       size=13, bold=True, color=GREY)
bar = _rect(s, Inches(3.2), Inches(2.28), Inches(9.0), Inches(0.85), fill=RGBColor(0xD9, 0xE0, 0xE8),
            shape=MSO_SHAPE.RECTANGLE)
segs = [("Understand", 0.45, GREY), ("Design approach", 1.8, NAVY_LT),
        ("Write tests", 5.4, NAVY), ("Debug & document", 1.35, GREY)]
sx = Inches(3.2)
for label, w_in, c in segs:
    seg = _rect(s, sx, Inches(2.28), Inches(w_in), Inches(0.85), fill=c,
                shape=MSO_SHAPE.RECTANGLE)
    seg.line.color.rgb = WHITE
    seg.line.width = Pt(1)
    if w_in > 1.2:
        _txbox(s, sx, Inches(2.58), Inches(w_in), Inches(0.3), label, size=11,
               bold=True, color=WHITE, align=PP_ALIGN.CENTER)
    sx += Inches(w_in)
_txbox(s, Inches(3.2), Inches(3.25), Inches(9.0), Inches(0.35), "20 working days",
       size=13, bold=True, color=NAVY, align=PP_ALIGN.RIGHT)

_txbox(s, Inches(0.75), Inches(4.35), Inches(2.3), Inches(0.4), "AI-ASSISTED",
       size=13, bold=True, color=TEAL)
sx = Inches(3.2)
segs2 = [("AI work", 0.45, TEAL), ("Human review", 0.45, AMBER)]
for label, w_in, c in segs2:
    seg = _rect(s, sx, Inches(4.28), Inches(w_in), Inches(0.85), fill=c,
                shape=MSO_SHAPE.RECTANGLE)
    seg.line.color.rgb = WHITE
    seg.line.width = Pt(1)
    sx += Inches(w_in)
_txbox(s, Inches(4.25), Inches(4.53), Inches(4.0), Inches(0.35),
       "2 working days  ◀ AI work + human review", size=13, bold=True, color=TEAL)

_rect(s, Inches(0.75), Inches(5.5), Inches(11.8), Inches(1.15), fill=AMBER_LT)
_rect(s, Inches(0.75), Inches(5.5), Inches(0.08), Inches(1.15), fill=AMBER,
      shape=MSO_SHAPE.RECTANGLE)
_txbox(s, Inches(1.15), Inches(5.68), Inches(11.1), Inches(0.85),
       ["An honest note on the number",
        ("90% applies to building a large volume of tests from scratch — exactly where AI excels. "
         "For ongoing smaller tasks, expect 60–70%. We recommend planning against the conservative figure.",
         {"size": 13, "bold": False, "color": GREY})],
       size=15, bold=True, space_after=6)


# ═══════════════════════════════════════════════════════════════════════════════
# 10 · THE DEFECTS
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="What it found", eyebrow="Beyond the time saving",
               subtitle="Two defects, already live in our development environment, missed by code review"))

d1 = _rect(s, Inches(0.75), Inches(2.05), Inches(5.85), Inches(2.75), fill=WHITE, shadow=True)
_rect(s, Inches(0.75), Inches(2.05), Inches(5.85), Inches(0.09), fill=RED,
      shape=MSO_SHAPE.RECTANGLE)
_chip(s, Inches(1.1), Inches(2.35), Inches(1.75), Inches(0.36), "DEFECT 1", fill=RED, size=11)
_txbox(s, Inches(1.1), Inches(2.9), Inches(5.15), Inches(1.7),
       ["Patient-data protection could fail",
        ("The system that masks confidential patient information contained a fault that would fail "
         "on certain data types.", {"size": 13, "bold": False, "color": GREY}),
        ("Risk avoided: a compliance and privacy incident on regulated clinical data.",
         {"size": 13, "bold": True, "color": RED})],
       size=17, bold=True, space_after=10)

d2 = _rect(s, Inches(6.9), Inches(2.05), Inches(5.65), Inches(2.75), fill=WHITE, shadow=True)
_rect(s, Inches(6.9), Inches(2.05), Inches(5.65), Inches(0.09), fill=AMBER,
      shape=MSO_SHAPE.RECTANGLE)
_chip(s, Inches(7.25), Inches(2.35), Inches(1.75), Inches(0.36), "DEFECT 2", fill=AMBER, size=11)
_txbox(s, Inches(7.25), Inches(2.9), Inches(4.95), Inches(1.7),
       ["Silently invalid data keys",
        ("Configuration processing generated invalid keys from ordinary formatting — producing wrong "
         "results without producing an error.", {"size": 13, "bold": False, "color": GREY}),
        ("Risk avoided: silent data-correctness errors in downstream reporting.",
         {"size": 13, "bold": True, "color": AMBER})],
       size=17, bold=True, space_after=10)

_rect(s, Inches(0.75), Inches(5.1), Inches(11.8), Inches(1.35), fill=NAVY)
_txbox(s, Inches(1.15), Inches(5.35), Inches(11.1), Inches(0.9),
       ["This is the strongest argument for the investment.",
        ("The pilot paid for itself on the first defect alone.",
         {"size": 15, "bold": False, "color": RGBColor(0xC7, 0xD3, 0xE0)})],
       size=20, bold=True, color=WHITE, space_after=8)


# ═══════════════════════════════════════════════════════════════════════════════
# 11 · BUSINESS VALUE
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="Value beyond the hours saved", eyebrow="Business case",
               subtitle="Time is the easiest benefit to measure — it is not the most valuable one"))

vals = [
    ("Defects found early", "5–10× lower cost to fix than in UAT or production", TEAL),
    ("Confidence to modernise", "Turns 'don't touch it' into 'improve it safely'", GREEN),
    ("Faster, safer delivery", "Problems caught at the point of change", TEAL),
    ("Audit readiness", "Timestamped, reproducible evidence — automatically", NAVY),
    ("Reduced key-person risk", "Business rules captured as executable tests", GREEN),
    ("Faster onboarding", "The test suite explains how the platform behaves", TEAL),
]
for i, (head, sub, c) in enumerate(vals):
    col, row = i % 3, i // 3
    x = Inches(0.75) + Inches(4.05 * col)
    y = Inches(2.1) + Inches(2.1 * row)
    _rect(s, x, y, Inches(3.75), Inches(1.8), fill=WHITE, shadow=True)
    _rect(s, x, y, Inches(0.07), Inches(1.8), fill=c, shape=MSO_SHAPE.RECTANGLE)
    _txbox(s, x + Inches(0.3), y + Inches(0.35), Inches(3.2), Inches(0.5), head,
           size=16, bold=True)
    _txbox(s, x + Inches(0.3), y + Inches(0.9), Inches(3.2), Inches(0.75), sub,
           size=12, color=GREY)


# ═══════════════════════════════════════════════════════════════════════════════
# 12 · SAFETY Q&A
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="The questions you should be asking", eyebrow="Safety & confidentiality",
               subtitle="Direct answers to the concerns that AI-assisted engineering legitimately raises"))

qa = [
    ("Does our code leave our environment?",
     "No. Work happens inside our enterprise-controlled boundary. Commercial terms prohibit using our code to train public AI."),
    ("Can the AI see patient data?",
     "No. It never connects to clinical systems. Tests use invented data — fictional sites, fictional identifiers."),
    ("Can the AI change production systems?",
     "No. It writes test files only. Any change to real system code requires individually recorded human approval."),
    ("Can the AI release code by itself?",
     "No. Release requires a named Quality Lead and Technical Lead to approve."),
    ("What if it makes a mistake?",
     "Every output is reviewed by a qualified engineer. Independent automated checks run in parallel. Nothing merges unreviewed."),
    ("Is this auditable?",
     "Fully. Every session, change, approval and test result is logged and retained."),
]
y = Inches(2.0)
for i, (q, a) in enumerate(qa):
    fill = WHITE if i % 2 else GREY_LT
    band = _rect(s, Inches(0.75), y, Inches(11.8), Inches(0.72), fill=fill,
                 shape=MSO_SHAPE.RECTANGLE)
    band.line.fill.background()
    _chip(s, Inches(0.95), y + Inches(0.16), Inches(0.4), Inches(0.4), "?",
          fill=TEAL, size=13)
    _txbox(s, Inches(1.5), y + Inches(0.08), Inches(4.0), Inches(0.6), q,
           size=13, bold=True, anchor=MSO_ANCHOR.MIDDLE)
    _txbox(s, Inches(5.7), y + Inches(0.08), Inches(6.7), Inches(0.6), a,
           size=11.5, color=GREY, anchor=MSO_ANCHOR.MIDDLE)
    y += Inches(0.76)


# ═══════════════════════════════════════════════════════════════════════════════
# 13 · WHERE HUMANS REMAIN ESSENTIAL
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="What the AI does — and what only people can do", eyebrow="Human expertise",
               subtitle="Being clear about the limits is what makes the claims credible"))

_rect(s, Inches(0.75), Inches(2.05), Inches(5.85), Inches(3.9), fill=TEAL_LT)
_txbox(s, Inches(1.1), Inches(2.3), Inches(5.1), Inches(0.4),
       "THE AI HANDLES WELL", size=12.5, bold=True, color=TEAL)
_bullets(s, Inches(1.1), Inches(2.85), Inches(4.9), [
    "Writing large volumes of repetitive test cases",
    "Covering edge cases systematically",
    "Correcting its own technical errors",
    "Producing consistent documentation",
    "Working tirelessly through mechanical detail",
], size=13.5, gap=0.56, marker_color=TEAL)

_rect(s, Inches(6.9), Inches(2.05), Inches(5.65), Inches(3.9), fill=AMBER_LT)
_txbox(s, Inches(7.25), Inches(2.3), Inches(5.0), Inches(0.4),
       "A HUMAN IS ESSENTIAL", size=12.5, bold=True, color=AMBER)
_bullets(s, Inches(7.25), Inches(2.85), Inches(4.8), [
    "Judging whether a clinical business rule is correct",
    "Deciding risk trade-offs on live systems",
    "Approving anything touching patient-data protection",
    "Regulatory interpretation and validation strategy",
    "Confirming the tests check the right things",
], size=13.5, gap=0.56, marker_color=AMBER)

_rect(s, Inches(0.75), Inches(6.2), Inches(11.8), Inches(0.65), fill=NAVY)
_txbox(s, Inches(1.15), Inches(6.36), Inches(11.1), Inches(0.4),
       "In short: the AI knows what the code does. Only our people know what it should do.",
       size=15, bold=True, color=WHITE)


# ═══════════════════════════════════════════════════════════════════════════════
# 14 · WHAT IS REQUIRED
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="What the investment looks like", eyebrow="Requirements",
               subtitle="An incremental step, not a transformation programme — the foundations are already in place"))

rows = [
    ["Item", "Purpose", "Note"],
    ["AI assistant licences (Enterprise tier)", "Secure operation, policy controls, audit",
     ("Enterprise tier is required", {"bold": True})],
    ["Security scanning add-on", "Independent verification of AI output", "Recommended for regulated code"],
    ["Automation platform usage", "Running tests and safety checks", "Minimal — tests take 9 seconds"],
    ["Engineer review time", "The essential human control", "≈ 1 day per major module"],
    ["Cloud testing compute", "—", ("ELIMINATED — no data cluster needed", {"bold": True, "color": GREEN})],
]
_table(s, Inches(0.75), Inches(2.05), Inches(11.8), rows,
       [Inches(4.0), Inches(4.3), Inches(3.5)], row_h=Inches(0.52), size=12.5)

_rect(s, Inches(0.75), Inches(5.35), Inches(11.8), Inches(1.2), fill=GREEN_LT, shadow=True)
_rect(s, Inches(0.75), Inches(5.35), Inches(0.08), Inches(1.2), fill=GREEN,
      shape=MSO_SHAPE.RECTANGLE)
_txbox(s, Inches(1.15), Inches(5.55), Inches(11.1), Inches(0.9),
       ["We already have the foundations.",
        ("The platform code, the operating instructions for the AI, our release pipeline, and — most "
         "importantly — the engineering judgement to supervise it.",
         {"size": 13, "bold": False, "color": GREY})],
       size=16, bold=True, space_after=6)


# ═══════════════════════════════════════════════════════════════════════════════
# 15 · ROADMAP
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="The path forward", eyebrow="Roadmap",
               subtitle="Four waves — the first is complete and evidenced"))

waves = [
    ("WAVE 1", "Pilot", "COMPLETE", "146 tests delivered\n2 defects found\nZero security incidents", GREEN),
    ("WAVE 2", "Automate", "4–6 weeks", "AI works from tickets\nAll approval controls retained", TEAL),
    ("WAVE 3", "Extend", "6–8 weeks", "Apply to further systems\nProve portability", NAVY_LT),
    ("WAVE 4", "Standardise", "Ongoing", "Organisational default\nQuality-approved procedure", AMBER),
]
x = Inches(0.75)
for i, (w, name, when, detail, c) in enumerate(waves):
    _rect(s, x, Inches(2.15), Inches(2.75), Inches(3.3), fill=WHITE, shadow=True)
    _rect(s, x, Inches(2.15), Inches(2.75), Inches(0.75), fill=c, shape=MSO_SHAPE.RECTANGLE)
    _txbox(s, x + Inches(0.22), Inches(2.3), Inches(2.3), Inches(0.25), w,
           size=10.5, bold=True, color=WHITE)
    _txbox(s, x + Inches(0.22), Inches(2.55), Inches(2.3), Inches(0.3), name,
           size=17, bold=True, color=WHITE)
    _chip(s, x + Inches(0.22), Inches(3.05), Inches(1.6), Inches(0.35), when,
          fill=GREY_LT, text_color=NAVY, size=11)
    _txbox(s, x + Inches(0.22), Inches(3.58), Inches(2.35), Inches(1.6), detail,
           size=12, color=GREY, line_spacing=1.35)
    if i < 3:
        _arrow(s, x + Inches(2.82), Inches(3.65), Inches(0.28), Inches(0.3), color=GREY)
    x += Inches(3.05)

_txbox(s, Inches(0.75), Inches(5.75), Inches(11.8), Inches(0.4),
       "RECOMMENDED NEXT STEPS", size=12, bold=True, color=TEAL)
steps = ["1  Approve Wave 2", "2  Confirm enterprise agreement in writing",
         "3  Nominate accountable owners", "4  Set the measurement baseline"]
sx = Inches(0.75)
for st in steps:
    _chip(s, sx, Inches(6.15), Inches(2.85), Inches(0.55), st, fill=NAVY, size=12)
    sx += Inches(2.98)


# ═══════════════════════════════════════════════════════════════════════════════
# 16 · RECOMMENDATION
# ═══════════════════════════════════════════════════════════════════════════════
s = prs.slides.add_slide(BLANK)
N += 1
_rect(s, Inches(0), Inches(0), W, H, fill=NAVY, shape=MSO_SHAPE.RECTANGLE)
_rect(s, Inches(0), Inches(0), Inches(0.22), H, fill=TEAL, shape=MSO_SHAPE.RECTANGLE)

_txbox(s, Inches(0.95), Inches(0.95), Inches(11), Inches(0.35), "THE RECOMMENDATION",
       size=13, bold=True, color=TEAL)
_txbox(s, Inches(0.95), Inches(1.45), Inches(11.2), Inches(0.9),
       "We recommend proceeding to Wave 2.", size=36, bold=True, color=WHITE)

_txbox(s, Inches(0.95), Inches(2.6), Inches(11.2), Inches(1.3),
       ["The pilot delivered a 90% reduction in effort on work the organisation has repeatedly deferred "
        "on cost grounds — and found two live defects, one with direct privacy and compliance implications.",
        ("The controls are proportionate and already proven: our code stayed inside our boundary, the AI "
         "never touched patient data, and every output passed human review.",
         {"size": 15, "color": RGBColor(0xC7, 0xD3, 0xE0)})],
       size=15.5, color=WHITE, space_after=12, line_spacing=1.35)

_rect(s, Inches(0.95), Inches(4.25), Inches(11.2), Inches(0.05), fill=TEAL,
      shape=MSO_SHAPE.RECTANGLE)

_txbox(s, Inches(0.95), Inches(4.65), Inches(11.2), Inches(1.3),
       ["The remaining decision is not technical. It is a business judgement:",
        ("We can continue to defer quality assurance because it is expensive — or we can adopt an "
         "approach that makes it affordable, and start closing the gap now.",
         {"size": 20, "bold": True, "color": TEAL})],
       size=15, color=RGBColor(0xC7, 0xD3, 0xE0), space_after=14, line_spacing=1.3)

# mini metrics strip
mini = [("146", "tests"), ("90%", "less effort"), ("2", "defects found"), ("$0", "cloud cost")]
mx = Inches(0.95)
for v, l in mini:
    _txbox(s, mx, Inches(6.3), Inches(2.6), Inches(0.45), v, size=26, bold=True, color=WHITE)
    _txbox(s, mx, Inches(6.78), Inches(2.6), Inches(0.3), l.upper(), size=10.5,
           bold=True, color=RGBColor(0x8F, 0xA3, 0xB8))
    mx += Inches(2.8)


# ═══════════════════════════════════════════════════════════════════════════════
# 17 · BOARD SUMMARY / APPENDIX
# ═══════════════════════════════════════════════════════════════════════════════
s = num(_slide(title="One page for the board", eyebrow="Summary",
               subtitle="Every question, with its answer"))

rows = [
    ["Question", "Answer"],
    ["What did we do?", "Used an AI assistant to build a complete automated test suite for the Clinical Data Platform"],
    ["How long did it take?", ("2 days, versus 20 days manually", {"bold": True})],
    ["What did it save?", ("≈ 18 engineering days — a 90% reduction", {"bold": True, "color": GREEN})],
    ["What did it find?", ("2 live defects, one affecting patient-data protection", {"bold": True, "color": RED})],
    ["What did it cost to run?", "$0 in cloud compute — the suite runs in 9 seconds"],
    ["Is our code safe?", "Yes — it never leaves our environment; enterprise terms prohibit training use"],
    ["Is patient data safe?", "Yes — the tests cannot connect to clinical data by design"],
    ["Who approves changes?", "A qualified engineer, plus a Quality Lead for any release"],
    ["What is the ongoing saving?", "60–85% on future testing work; 40–60 engineering days per year"],
    ["What do we recommend?", ("Approve Wave 2 — automate the workflow, retain every control",
                               {"bold": True, "color": TEAL})],
]
_table(s, Inches(0.75), Inches(1.95), Inches(11.8), rows,
       [Inches(3.6), Inches(8.2)], row_h=Inches(0.4), head_h=Inches(0.44), size=12)


prs.save(OUT)
print(f"Deck written: {OUT}")
print(f"Slides: {len(prs.slides.__iter__.__self__._sldIdLst)}")
