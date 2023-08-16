import argparse
import json
import warnings

from hysds.es_util import get_mozart_es


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy-name", type=str, default="ilm_policy_mozart", help="ISM/ILM policy name")
    parser.add_argument("--ilm-policy", type=str, help="location of the ILM policy file")
    parser.add_argument("--ism-policy", type=str, help="location of the ISM policy file")

    args = parser.parse_args()
    policy_name = args.policy_name
    _ilm_policy_file = args.ilm_policy
    _ism_policy_file = args.ism_policy

    if not _ism_policy_file and not _ilm_policy_file:
        raise RuntimeError("--ilm-policy or --ism-policy must be provided")

    mozart_es = get_mozart_es()
    info = mozart_es.es.info()

    version_info = info["version"]
    build_flavor = version_info.get("build_flavor", None)
    distribution = version_info.get("distribution", "elasticsearch")

    if build_flavor == "oss" and distribution != "opensearch":
        # Elasticsearch OSS
        with open(_ism_policy_file) as f:
            ism_template = json.load(f)
            exists = mozart_es.es.transport.perform_request("GET", f"/_opendistro/_ism/policies/{policy_name}",
                                                            body=ism_template, ignore=[404])
            if exists.get("status") == 404:
                warnings.simplefilter('always', UserWarning)
                warnings.warn(f"{policy_name} already exists, skipping...")
            else:
                res = mozart_es.es.transport.perform_request("PUT", f"/_opendistro/_ism/policies/{policy_name}",
                                                             body=ism_template, ignore=[400])
    elif distribution == "opensearch":
        with open(_ism_policy_file) as f:
            ism_template = json.load(f)
            if hasattr(mozart_es.es, "index_management"):
                res = mozart_es.es.plugins.index_management.put_policy(policy_name, body=ism_template)
            else:
                res = mozart_es.es.transport.perform_request("PUT", f"/_plugins/_ism/policies/{policy_name}",
                                                             body=ism_template, ignore=[400])
    else:
        # regular Elasticsearch
        with open(_ilm_policy_file) as f:
            ilm_template = json.load(f)
            res = mozart_es.es.ilm.put_lifecycle(policy=policy_name, body=ilm_template, ignore=[400])

    print(json.dumps(res, indent=2))
