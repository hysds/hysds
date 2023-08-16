import sys
import json
import warnings
from packaging import version

from hysds.es_util import get_mozart_es


MIN_ES_OSS_ISM_VERSION = version.parse("7.10.2")
POLICY_NAME = "ilm_policy_mozart"

warnings.simplefilter('always', UserWarning)


# https://opendistro.github.io/for-elasticsearch-docs/version-history/
# https://opendistro.github.io/for-elasticsearch-docs/docs/im/ism/#step-1-set-up-policies
def install_mozart_template(name, template_file):
    mozart_es = get_mozart_es()
    info = mozart_es.es.info()

    version_info = info["version"]
    version_number = version.parse(version_info["number"])
    build_flavor = version_info.get("build_flavor", None)
    distribution = version_info.get("distribution", "elasticsearch")

    with open(template_file) as f:
        template = json.load(f)

        # elasticsearch-oss
        if build_flavor == "oss" and distribution != "opensearch":
            if version_number <= MIN_ES_OSS_ISM_VERSION:
                warnings.warn("ISM in Open Distro < 1.13.0 requires the policy to added to the template")
                template["template"]["settings"]["opendistro.index_state_management.policy_id": "policy_id"] = POLICY_NAME
        elif distribution == "opensearch":
            # the policy should populate the ISM if the index patterns match
            warnings.warn("Opensearch 1.13+ ISM does not require the policy to added to the template")
        else:
            ilm_lifecycle = {
                "name": "ilm_policy_mozart"
            }
            template["template"]["settings"]["index"]["lifecycle"] = ilm_lifecycle
        print(json.dumps(template, indent=2))

        res = mozart_es.es.indices.put_index_template(name=name, body=template, ignore=[400])
        print(json.dumps(res, indent=2))


if __name__ == "__main__":
    _name = sys.argv[1]
    _template_file = sys.argv[2]

    install_mozart_template(_name, _template_file)
