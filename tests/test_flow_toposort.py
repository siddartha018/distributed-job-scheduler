from __future__ import annotations

import pytest

from worker.executor import FlowDefinitionError, topo_sort_step_deps


def test_toposort_orders_dependencies_first() -> None:
    deps = {
        "signoff": {"dft", "dfp"},
        "dfp": {"compile"},
        "dft": {"compile"},
        "compile": set(),
    }
    order = topo_sort_step_deps(deps)
    assert order.index("compile") < order.index("dft")
    assert order.index("compile") < order.index("dfp")
    assert order.index("dft") < order.index("signoff")
    assert order.index("dfp") < order.index("signoff")


def test_toposort_cycle_raises() -> None:
    deps = {"a": {"b"}, "b": {"a"}}
    with pytest.raises(FlowDefinitionError):
        topo_sort_step_deps(deps)

