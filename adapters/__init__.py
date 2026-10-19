"""Network adapters: they turn platform updates into domain models and back.

Adapters are the only place that performs I/O against a messenger. They depend
on ``core`` (models and errors), never the other way round; the domain talks to
them through ``core.ports.MessageSenderPort``.
"""

__all__: list[str] = []
