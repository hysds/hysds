from hysds.es_util import get_grq_es, get_mozart_es

if __name__ == "__main__":
    mozart_es = get_mozart_es()
    grq_es = get_grq_es()

    # MOZART
    aliases = mozart_es.es.indices.get_alias()
    for k, _ in aliases.items():
        if (
            k.startswith("job_status-")
            or k.startswith("worker_status-")
            or k.startswith("event_status-")
            or k.startswith("task_status-")
            or k.startswith("hysds_ios-")
            or k.startswith("user_rules-")
            or k == "job_specs"
            or k == "containers"
        ):
            print(f"deleted {k} index")
            mozart_es.es.indices.delete(index=k)

    mozart_es.es.indices.delete_index_template(name="job_status", ignore=404)
    print("deleted job_status template")
    mozart_es.es.indices.delete_index_template(name="task_status", ignore=404)
    print("deleted task_status template")
    mozart_es.es.indices.delete_index_template(name="worker_status", ignore=404)
    print("deleted worker_status template")
    mozart_es.es.indices.delete_index_template(name="event_status", ignore=404)
    print("deleted event_status template")

    mozart_es.es.indices.delete_template(name="containers", ignore=404)
    print("deleted containers template")
    mozart_es.es.indices.delete_template(name="job_specs", ignore=404)
    print("deleted job_specs template")
    mozart_es.es.indices.delete_template(name="hysds_ios", ignore=404)
    print("deleted hysds_ios template")

    mozart_es.es.indices.delete_template(name="logstash", ignore=404)
    mozart_es.es.indices.delete_template(name="ecs-logstash", ignore=404)
    print("deleted logstash template")

    info = mozart_es.es.info()
    version_info = info["version"]
    build_flavor = version_info.get("build_flavor", None)
    distribution = version_info.get("distribution", "elasticsearch")

    policy_name = "ilm_policy_mozart"
    if build_flavor == "oss" and distribution != "opensearch":
        # Elasticsearch OSS
        mozart_es.es.transport.perform_request(
            "DELETE",
            f"/_opendistro/_ism/policies/{policy_name}",
            params={"ignore": 404},
        )
    elif distribution == "opensearch":
        if hasattr(mozart_es.es, "index_management"):
            mozart_es.es.plugins.index_management.delete_policy(policy_name, ignore=404)
        else:
            mozart_es.es.transport.perform_request(
                "DELETE",
                f"/_plugins/_ism/policies/{policy_name}",
                params={"ignore": 404},
            )
    else:  # regular Elasticsearch
        mozart_es.es.ilm.delete_lifecycle(policy=policy_name, ignore=404)
    print("deleted ILM/ISM policy")

    # GRQ
    aliases = grq_es.es.indices.get_alias()
    for k, _ in aliases.items():
        if k.startswith("grq_"):
            print(f"deleted {k} index")
            grq_es.es.indices.delete(index=k)

    grq_es.es.indices.delete_template(name="grq", ignore=404)
    print("deleted grq template")
