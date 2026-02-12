"""Static tests for csrf.js correctness.

We don't run a browser in unit tests, but we can still guard important
code paths by inspecting the JS source.

Spec:
- fetch() patch must consider Request inputs (method lives on input.method)
  when init.method is absent.
"""

from pathlib import Path


def test_csrf_js_handles_request_input_method():
    js_path = (
        Path(__file__).resolve().parents[3] / "services/webapp/app/static/js/csrf.js"
    )
    content = js_path.read_text(encoding="utf-8")

    # Ensure we look at input.method when input is a Request.
    assert "input instanceof Request" in content
    assert ".method" in content
