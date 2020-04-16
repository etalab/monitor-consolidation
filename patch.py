from typing import List
import sys


def uncache(exclude: List[str]):
    """
    Remove package modules from cache except excluded ones.
    On next import they will be reloaded.

    Args: exclude: Sequence of module paths.
    """
    pkgs = []
    for mod in exclude:
        pkg = mod.split(".", 1)[0]
        pkgs.append(pkg)

    to_uncache = []
    for mod in sys.modules:
        if mod in exclude:
            continue

        if mod in pkgs:
            to_uncache.append(mod)
            continue

        for pkg in pkgs:
            if mod.startswith(pkg + "."):
                to_uncache.append(mod)
                break

    for mod in to_uncache:
        del sys.modules[mod]


import goodtables

goodtables.config.DEFAULT_ERROR_LIMIT = -1
uncache(["goodtables.config"])
