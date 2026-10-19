"""Tests for :mod:`core.formatting` — entity conversion, fully offline."""

from __future__ import annotations

import pytest

from core.formatting import (
    RichText,
    normalize_entities,
    render_bale_markdown,
    render_relay_text,
    utf16_length,
    utf16_offset_to_index,
)
from core.models import Platform

# ------------------------------------------------------------------ normalizing


def test_plain_text_without_entities_is_untouched() -> None:
    rich = normalize_entities("سلام دنیا", ())
    assert rich.text == "سلام دنیا"
    assert rich.spans == ()


def test_bold_italic_and_code_become_spans() -> None:
    rich = normalize_entities(
        "ab cd ef",
        (
            {"type": "bold", "offset": 0, "length": 2},
            {"type": "italic", "offset": 3, "length": 2},
            {"type": "code", "offset": 6, "length": 2},
        ),
    )
    kinds = [span.kind.value for span in rich.spans]
    assert kinds == ["bold", "italic", "code"]
    assert [(span.start, span.end) for span in rich.spans] == [(0, 2), (3, 5), (6, 8)]


def test_pre_and_text_link_become_spans() -> None:
    rich = normalize_entities(
        "code link",
        (
            {"type": "pre", "offset": 0, "length": 4},
            {"type": "text_link", "offset": 5, "length": 4, "url": "https://example.com"},
        ),
    )
    assert rich.spans[0].kind.value == "pre"
    assert rich.spans[1].kind.value == "text_link"
    assert rich.spans[1].url == "https://example.com"


@pytest.mark.parametrize(
    "entity",
    [
        {"type": "mention", "offset": 0, "length": 3},
        {"type": "hashtag", "offset": 0, "length": 3},
        {"type": "bot_command", "offset": 0, "length": 3},
        {"type": "unknown_future", "offset": 0, "length": 3},
        {"offset": 0, "length": 3},  # no type at all
        "not a mapping",
    ],
)
def test_unsupported_entity_types_are_dropped_but_text_kept(entity: object) -> None:
    rich = normalize_entities("abc", (entity,))  # type: ignore[list-item]
    assert rich.text == "abc"
    assert rich.spans == ()


def test_text_link_without_valid_url_is_dropped() -> None:
    rich = normalize_entities(
        "click here",
        (
            {"type": "text_link", "offset": 0, "length": 5, "url": "javascript:alert(1)"},
            {"type": "text_link", "offset": 6, "length": 4, "url": "not-a-url"},
            {"type": "text_link", "offset": 0, "length": 5},
        ),
    )
    assert rich.spans == ()


def test_out_of_range_and_empty_spans_are_dropped() -> None:
    rich = normalize_entities(
        "abc",
        (
            {"type": "bold", "offset": 2, "length": 99},  # extends past the end
            {"type": "bold", "offset": 1, "length": 0},  # empty
            {"type": "bold", "offset": 1, "length": -3},  # negative
            {"type": "bold", "offset": 50, "length": 3},  # fully outside
            {"type": "italic"},  # no offsets
            {"type": "code", "offset": "0", "length": "1"},  # not ints
        ),
    )
    # The (2, 99) span is clamped to (2, 3); everything else is dropped.
    assert [(span.start, span.end) for span in rich.spans] == [(2, 3)]


def test_utf16_offsets_are_converted_to_codepoints() -> None:
    text = "a😀b"  # 4 UTF-16 units, 3 codepoints
    rich = normalize_entities(
        text,
        (
            {"type": "bold", "offset": 0, "length": 4},  # the whole text
            {"type": "italic", "offset": 1, "length": 2},  # the emoji only
        ),
    )
    assert [(span.start, span.end) for span in rich.spans] == [(0, 3), (1, 2)]


def test_utf16_offset_to_index_identity_on_bmp_only_text() -> None:
    assert utf16_offset_to_index("abc", 2) == 2
    assert utf16_offset_to_index("abc", 99) == 3  # clamped
    assert utf16_offset_to_index("abc", 0) == 0


def test_utf16_offset_to_index_over_astral_characters() -> None:
    text = "😀😀x"
    # 2 units into the string lands after the first emoji (index 1).
    assert utf16_offset_to_index(text, 2) == 1
    assert utf16_offset_to_index(text, 4) == 2
    assert utf16_offset_to_index(text, 5) == 3


# ------------------------------------------------------------------- rendering


def render(text: str, entities: tuple[dict, ...]) -> str:
    return render_bale_markdown(normalize_entities(text, entities))


def test_render_bold_italic_code_and_pre() -> None:
    assert render("سلام", ({"type": "bold", "offset": 0, "length": 4},)) == "**سلام**"
    assert render("سلام", ({"type": "italic", "offset": 0, "length": 4},)) == "*سلام*"
    assert render("x=1", ({"type": "code", "offset": 0, "length": 3},)) == "`x=1`"
    assert render(
        "print(1)", ({"type": "pre", "offset": 0, "length": 8},)
    ) == "```\nprint(1)\n```"


def test_render_text_link() -> None:
    rendered = render(
        "وب‌سایت ما",
        (
            {
                "type": "text_link",
                "offset": 0,
                "length": 7,
                "url": "https://example.com",
            },
        ),
    )
    assert rendered == "[وب‌سایت](https://example.com) ما"


def test_render_multiple_disjoint_spans() -> None:
    rendered = render(
        "one two",
        (
            {"type": "bold", "offset": 0, "length": 3},
            {"type": "italic", "offset": 4, "length": 3},
        ),
    )
    assert rendered == "**one** *two*"


def test_render_overlapping_spans_falls_back_to_plain_text() -> None:
    rendered = render(
        "abcdef",
        (
            {"type": "bold", "offset": 0, "length": 4},
            {"type": "italic", "offset": 2, "length": 4},
        ),
    )
    assert rendered == "abcdef"


def test_render_text_with_markdown_characters_falls_back_to_plain_text() -> None:
    entities = ({"type": "bold", "offset": 0, "length": 3},)
    for text in ("a*b", "a`b", "a[b", "a]b", "**"):
        assert render(text, entities) == text


def test_render_fallback_keeps_text_byte_identical() -> None:
    text = "متن *امن* با **نشانه**"
    entities = ({"type": "bold", "offset": 0, "length": 20},)
    assert render(text, entities) == text


def test_rich_text_without_spans_renders_unchanged() -> None:
    assert render_bale_markdown(RichText(text="متن", spans=())) == "متن"


# --------------------------------------------------------------- relay dispatch


def test_relay_text_to_bale_applies_markdown() -> None:
    entities = ({"type": "bold", "offset": 0, "length": 4},)
    assert render_relay_text("سلام", entities, Platform.BALE) == "**سلام**"


def test_relay_text_to_telegram_stays_plain() -> None:
    entities = ({"type": "bold", "offset": 0, "length": 4},)
    assert render_relay_text("سلام", entities, Platform.TELEGRAM) == "سلام"


def test_relay_text_empty_body_is_returned_as_is() -> None:
    assert render_relay_text("", (), Platform.BALE) == ""


def test_entities_never_change_the_underlying_text() -> None:
    text = "متن ترکیبی 😀 با https://example.com"
    entities = (
        {"type": "bold", "offset": 0, "length": 3},
        {
            "type": "text_link",
            "offset": utf16_length(text) - len("https://example.com"),
            "length": len("https://example.com"),
            "url": "https://example.com",
        },
    )
    rich = normalize_entities(text, entities)
    assert rich.text == text, "normalization must not touch the plain text"
    assert len(rich.spans) == 2
    assert rich.spans[1].kind.value == "text_link"
