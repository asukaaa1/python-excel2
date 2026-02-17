"""Dependency binding helpers for route modules."""


def bind_dependencies(module_globals, deps, required_names):
    """Bind explicitly allowed dependencies from dashboardserver globals."""
    if deps is None:
        deps_map = {}
    elif isinstance(deps, dict):
        deps_map = deps
    else:
        deps_map = vars(deps)

    missing = []
    for name in required_names:
        if name in module_globals:
            continue
        if name in deps_map:
            module_globals[name] = deps_map[name]
        else:
            missing.append(name)

    if missing:
        missing_csv = ", ".join(sorted(missing))
        raise RuntimeError(f"Missing route dependencies: {missing_csv}")
