"""Platform-agnostic domain layer of the Telegram/Bale bridge.

Modules here never perform I/O against a messenger API. They depend only on
small repository protocols (dependency inversion), which keeps business rules
testable without network access.
"""

__all__: list[str] = []
