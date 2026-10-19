"""Entity conversion through a small platform-neutral format.

Inbound messages carry raw platform entities (Telegram-style mappings with
``type``/``offset``/``length``; Bale speaks the same dialect). Relaying must not
leak one platform's dialect into the other, so the pipeline is:

    raw entities ──normalize──▶ :class:`RichText` ──render──▶ platform text

* **normalize** converts UTF-16 code-unit offsets (how Telegram and Bale measure
  entities) into Python codepoint indices, validates ranges and drops entity
  kinds this bridge cannot represent. The plain text is never altered.
* **render** produces the text to hand to the target platform. Bale formats
  outgoing text as Markdown by itself (its ``sendMessage`` has no ``parse_mode``,
  see ``adapters/bale.py``), so the Bale rendering emits a conservative Markdown
  subset. Telegram-bound relays are sent as plain text in this phase.

Safety over fidelity: whenever the plain text already contains characters that
would make the Markdown ambiguous, or spans overlap, the renderer falls back to
the untouched plain text — content integrity beats formatting.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from core.models import Platform


class SpanKind(str, Enum):
    """Formatting kinds the intermediate format can represent."""

    BOLD = "bold"
    ITALIC = "italic"
    CODE = "code"
    PRE = "pre"
    TEXT_LINK = "text_link"


#: Raw entity types (Telegram/Bale dialect) the bridge understands.
_SUPPORTED_ENTITY_TYPES: frozenset[str] = frozenset(
    kind.value for kind in SpanKind
)

#: Characters that make Markdown rendering ambiguous when they already appear
#: in the plain text (Bale parses Markdown itself; we cannot escape it).
_UNSAFE_MARKDOWN_CHARS: frozenset[str] = frozenset("`*[]")


@dataclass(frozen=True, slots=True)
class Span:
    """One formatting range over :attr:`RichText.text`, in codepoint indices.

    ``end`` is exclusive. ``url`` is only meaningful for
    :attr:`SpanKind.TEXT_LINK`.
    """

    kind: SpanKind
    start: int
    end: int
    url: str | None = None


@dataclass(frozen=True, slots=True)
class RichText:
    """Plain text plus normalised formatting spans."""

    text: str
    spans: tuple[Span, ...]


def utf16_length(text: str) -> int:
    """Length of ``text`` in UTF-16 code units (how platforms measure entities)."""
    return len(text.encode("utf-16-le")) // 2


def utf16_offset_to_index(text: str, offset: int) -> int:
    """Convert a UTF-16 code-unit offset into a Python codepoint index.

    Astral-plane characters (emoji, rare CJK) occupy two UTF-16 units but one
    Python character, so naive slicing corrupts ranges in Persian+emoji text.
    The result is clamped to ``[0, len(text)]``.
    """
    if offset <= 0:
        return 0
    if len(text) == utf16_length(text):  # pure BMP: identity mapping
        return min(offset, len(text))
    remaining = offset
    for position, char in enumerate(text):
        if remaining <= 0:
            return position
        remaining -= 2 if ord(char) > 0xFFFF else 1
    return len(text)


def _valid_url(url: object) -> str | None:
    """Return the URL when it is an absolute http(s) link, else ``None``."""
    if not isinstance(url, str):
        return None
    stripped = url.strip()
    if stripped.startswith(("http://", "https://")) and len(stripped) > len("https://"):
        return stripped
    return None


def normalize_entities(
    text: str, entities: Sequence[Mapping[str, Any]]
) -> RichText:
    """Convert raw platform entities into :class:`RichText`.

    Behaviour, in order:

    * entities that are not mappings, or whose type is unsupported, are
      ignored — the underlying text is always preserved verbatim;
    * UTF-16 ``offset``/``length`` are converted to clamped codepoint ranges;
    * empty/out-of-range spans are dropped;
    * ``text_link`` spans without a valid absolute ``http(s)`` URL are dropped
      (the link text stays, the link itself does not survive the bridge);
    * overlapping spans are kept here; renderers decide how to handle them.
    """
    spans: list[Span] = []
    for entity in entities:
        if not isinstance(entity, Mapping):
            continue
        kind_value = entity.get("type")
        if not isinstance(kind_value, str) or kind_value not in _SUPPORTED_ENTITY_TYPES:
            continue
        raw_offset = entity.get("offset")
        raw_length = entity.get("length")
        if not isinstance(raw_offset, int) or not isinstance(raw_length, int):
            continue
        if raw_length <= 0:
            continue

        start = utf16_offset_to_index(text, raw_offset)
        end = utf16_offset_to_index(text, raw_offset + raw_length)
        if start >= end:
            continue

        kind = SpanKind(kind_value)
        url: str | None = None
        if kind is SpanKind.TEXT_LINK:
            url = _valid_url(entity.get("url"))
            if url is None:
                continue
        spans.append(Span(kind=kind, start=start, end=end, url=url))

    return RichText(text=text, spans=tuple(spans))


def _spans_overlap(sorted_spans: Sequence[Span]) -> bool:
    """Whether any two spans share at least one character."""
    previous: Span | None = None
    for span in sorted_spans:
        if previous is not None and span.start < previous.end:
            return True
        previous = span
    return False


def _wrap(kind: SpanKind, chunk: str, url: str | None) -> str:
    """Wrap one text chunk in the Markdown marker of its span kind."""
    if kind is SpanKind.BOLD:
        return f"**{chunk}**"
    if kind is SpanKind.ITALIC:
        return f"*{chunk}*"
    if kind is SpanKind.CODE:
        return f"`{chunk}`"
    if kind is SpanKind.PRE:
        return f"```\n{chunk}\n```"
    return f"[{chunk}]({url})"


def render_bale_markdown(rich: RichText) -> str:
    """Render :class:`RichText` as the Markdown subset Bale formats itself.

    Falls back to the untouched plain text when a faithful rendering cannot be
    guaranteed, namely when:

    * spans overlap (Markdown cannot express the nesting unambiguously), or
    * the text already contains `` ` ``, ``*``, ``[`` or ``]`` (markers would be
      ambiguous and Bale's parser cannot be escaped portably).
    """
    spans = tuple(sorted(rich.spans, key=lambda span: (span.start, span.end)))
    if not spans:
        return rich.text
    if _spans_overlap(spans) or any(char in rich.text for char in _UNSAFE_MARKDOWN_CHARS):
        return rich.text

    parts: list[str] = []
    position = 0
    for span in spans:
        parts.append(rich.text[position : span.start])
        parts.append(_wrap(span.kind, rich.text[span.start : span.end], span.url))
        position = span.end
    parts.append(rich.text[position:])
    return "".join(parts)


def render_relay_text(
    text: str,
    entities: Sequence[Mapping[str, Any]],
    target_platform: Platform,
) -> str:
    """Render an inbound body for relaying to ``target_platform``.

    Telegram-bound messages are sent as plain text in this phase (the sender
    port has no ``parse_mode``); Bale-bound messages get the Markdown rendering
    with its plain-text fallback. The input text is never modified in either
    case — only markers are added around existing characters.
    """
    if not text:
        return text
    rich = normalize_entities(text, entities)
    if target_platform is Platform.BALE:
        return render_bale_markdown(rich)
    return rich.text


__all__ = [
    "RichText",
    "Span",
    "SpanKind",
    "normalize_entities",
    "render_bale_markdown",
    "render_relay_text",
    "utf16_length",
    "utf16_offset_to_index",
]
