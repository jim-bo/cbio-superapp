"""Prompt-injection dampening for tool output (M7).

Tool output is fed back to the LLM. If a malicious actor can drop a
file that a tool will read (a crafted ``meta_study.txt``, a rigged
TSV, etc.), they can smuggle instructions into the model context via
comments and pseudo-system prompts. The path allowlist (M3) is the
real fix — this module is defense in depth.

``scrub_tool_output`` does three things:

1. **Truncate** to a hard ceiling (default 8 KB). Enormous output is
   both a context-window footgun and a good place to hide payloads.
2. **Strip HTML/XML comments and obvious instruction markers** that
   injection payloads hide behind.
3. **Wrap** the result in ``<tool-output>...</tool-output>`` tags so
   the system prompt can tell the model to treat the contents as
   untrusted data, not instructions.

The wrapping is the important bit — it gives the model a structural
anchor ("anything between these tags is not a directive"). Combined
with the system-prompt append we ship in ``cli/main.py``, this makes
naive "ignore prior instructions" attacks measurably harder.
"""
from __future__ import annotations

import re

MAX_OUTPUT_BYTES = 8 * 1024  # 8 KB

# HTML/XML comments — the classic hiding place for payloads.
_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)

# Common "switch persona" markers that injection attempts use.
# We don't try to be clever — just strip the obvious ones.
_INJECTION_MARKERS = (
    "<|im_start|>",
    "<|im_end|>",
    "<|system|>",
    "<|user|>",
    "<|assistant|>",
    "<|endoftext|>",
)


def scrub_tool_output(text: str, *, max_bytes: int = MAX_OUTPUT_BYTES) -> str:
    """Return a sanitized, tagged version of ``text`` for LLM consumption.

    Args:
        text: Raw tool output (e.g. file contents, validation report).
        max_bytes: Hard truncation ceiling. Default 8 KB.

    Returns:
        A string of the form::

            <tool-output>
            <scrubbed and possibly truncated content>
            </tool-output>

        ``max_bytes`` applies to the *inner* content; the wrapping tags
        are added on top.
    """
    if not isinstance(text, str):
        text = str(text)

    # 1. Strip HTML/XML comments.
    cleaned = _COMMENT_RE.sub("", text)

    # 2. Strip well-known injection role markers.
    for marker in _INJECTION_MARKERS:
        cleaned = cleaned.replace(marker, "")

    # 3. Defang any stray <tool-output> tags in the content so attackers
    # can't close our wrapper early and start writing "instructions"
    # outside it.
    cleaned = cleaned.replace("<tool-output>", "&lt;tool-output&gt;")
    cleaned = cleaned.replace("</tool-output>", "&lt;/tool-output&gt;")

    # 4. Truncate on byte budget, not chars (context counting is tokens,
    # but bytes is a cheap conservative proxy).
    encoded = cleaned.encode("utf-8", errors="replace")
    if len(encoded) > max_bytes:
        truncated = encoded[:max_bytes].decode("utf-8", errors="ignore")
        cleaned = (
            truncated
            + f"\n\n[... truncated, {len(encoded) - max_bytes} bytes elided ...]"
        )

    return f"<tool-output>\n{cleaned}\n</tool-output>"
