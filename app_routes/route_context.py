"""Explicit dependency context for route modules."""

from typing import Dict, Iterable, Mapping, Any


class RouteContext:
    """Typed wrapper around shared runtime dependencies."""

    def __init__(self, deps: Mapping[str, Any]):
        self._deps: Dict[str, Any] = dict(deps)

    @classmethod
    def from_deps(cls, deps: Any) -> "RouteContext":
        if deps is None:
            mapping: Mapping[str, Any] = {}
        elif isinstance(deps, dict):
            mapping = deps
        else:
            mapping = vars(deps)
        return cls(mapping)

    def require(self, names: Iterable[str]) -> Dict[str, Any]:
        missing = [name for name in names if name not in self._deps]
        if missing:
            missing_csv = ", ".join(sorted(missing))
            raise RuntimeError(f"Missing route dependencies: {missing_csv}")
        return {name: self._deps[name] for name in names}
