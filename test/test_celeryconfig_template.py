"""configs/celery/celeryconfig.py.tmpl as sdscli renders it: Jinja with the
default Undefined, and a context loaded from ~/.sds/config (YAML)."""

import ast
import os
import unittest

import jinja2

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "configs", "celery")
TEMPLATE = "celeryconfig.py.tmpl"


def render(**sds_config):
    """Render with only the ES hosts set (the template calls string methods on
    them); everything else is undefined, as when ~/.sds/config omits it."""
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
    context = {
        "MOZART_ES_PVT_IP": "127.0.0.1",
        "GRQ_ES_PVT_IP": "127.0.0.1",
        "METRICS_ES_PVT_IP": "127.0.0.1",
        **sds_config,
    }
    return env.get_template(TEMPLATE).render(**context)


def assigned(rendered, name):
    for node in ast.parse(rendered).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not assigned in the rendered celeryconfig")


class TestUserRulesSettings(unittest.TestCase):
    def test_defaults_when_unset(self):
        rendered = render()
        self.assertIs(assigned(rendered, "USER_RULES_SEND_INDEX"), False)
        self.assertEqual(assigned(rendered, "DATASET_INDEX_PREFIX"), "grq")

    def test_send_index_only_for_a_yaml_true(self):
        self.assertIs(assigned(render(USER_RULES_SEND_INDEX=True),
                               "USER_RULES_SEND_INDEX"), True)
        # anything else renders False, never an invalid Python name
        for value in (False, None, "true", "yes", 1):
            rendered = render(USER_RULES_SEND_INDEX=value)
            self.assertIs(assigned(rendered, "USER_RULES_SEND_INDEX"), False, value)

    def test_dataset_index_prefix_override(self):
        rendered = render(DATASET_INDEX_PREFIX="mygrq")
        self.assertEqual(assigned(rendered, "DATASET_INDEX_PREFIX"), "mygrq")


if __name__ == "__main__":
    unittest.main()
