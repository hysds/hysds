import time
from future import standard_library

standard_library.install_aliases()

import os
import re
import json
import shutil
import traceback

from glob import glob
from datetime import datetime, UTC

import hysds
from hysds.utils import makedirs
from hysds.log_utils import logger
from hysds.dataset_ingest import publish_dataset
from hysds.celery import app


def get_triage_partition_format():
    return app.conf.get("TRIAGE_PARTITION_FORMAT", None)


def triage(job, ctx):
    """Triage failed job's context and job json as well as _run.sh."""

    # set time_start if not defined (job failed prior to setting it)
    if "time_start" not in job["job_info"]:
        job["job_info"]["time_start"] = "{}Z".format(datetime.now(UTC).isoformat("T"))

    # default triage id
    default_triage_id_format = "triaged_job-{job_id}_task-{job[task_id]}"
    default_triage_id_regex = "triaged_job-(?P<job_id>.+)_task-(?P<task_id>[-\\w])"

    # if exit code of job command is zero, don't triage anything
    exit_code = job["job_info"]["status"]
    if exit_code == 0:
        logger.info("Job exited with exit code %s. No need to triage." % exit_code)
        return True

    # disable triage
    if ctx.get("_triage_disabled", False):
        logger.info("Flag _triage_disabled set to True. Not performing triage.")
        return True

    # Check if custom triage id format was provided
    if "_triage_id_format" in ctx:
        triage_id_format = ctx["_triage_id_format"]
    else:
        triage_id_format = default_triage_id_format

    # get job info
    job_dir = job["job_info"]["job_dir"]
    job_id = job["job_info"]["id"]
    logger.info(f"job id: {job_id}")

    # Check if the job_id is a triaged dataset. If so, let's parse out the job_id
    logger.info(f"Checking to see if the job_id matches the regex: {default_triage_id_regex}")
    match = re.search(default_triage_id_regex, job_id)
    if match:
        logger.info("job_id matches the triage dataset regex. Parsing out job_id")
        parsed_job_id = match.groupdict()["job_id"]
        logger.info(f"extracted job_id: {parsed_job_id}")
    else:
        logger.info(f"job_id does not match the triage dataset regex: {default_triage_id_regex}")
        parsed_job_id = job_id

    # create triage dataset
    # Attempt to first use triage id format from user, but if there is any problem use the default id format instead
    try:
        triage_id = triage_id_format.format(job_id=parsed_job_id, job=job, job_context=ctx)
    except Exception as e:
        logger.warning(
            "Failed to apply custom triage id format because of {}: {}. Falling back to default triage id".format(
                e.__class__.__name__, e
            )
        )
        triage_id = default_triage_id_format.format(job_id=parsed_job_id, job=job, job_context=ctx)
    triage_dir = os.path.join(job_dir, triage_id)
    makedirs(triage_dir)

    # create dataset json
    ds_file = os.path.join(triage_dir, f"{triage_id}.dataset.json")
    ds = {
        "version": f"v{hysds.__version__}",
        "label": f"triage for job {parsed_job_id}",
    }
    triage_partition_format = get_triage_partition_format()
    if triage_partition_format:
        index_met = {
            "index": {
                "suffix": f"{ds['version']}_{datetime.now(UTC).strftime(triage_partition_format)}_triaged_job"
            }
        }
        ds.update(index_met)
    logger.info(f"dataset info:\n{json.dumps(ds, indent=2)}")
    if "cmd_start" in job["job_info"]:
        ds["starttime"] = job["job_info"]["cmd_start"]
    if "cmd_end" in job["job_info"]:
        ds["endtime"] = job["job_info"]["cmd_end"]
    with open(ds_file, "w") as f:
        json.dump(ds, f, sort_keys=True, indent=2)

    # create met json
    met_file = os.path.join(triage_dir, f"{triage_id}.met.json")
    with open(met_file, "w") as f:
        json.dump(job["job_info"], f, sort_keys=True, indent=2)

    # triage job-related files
    for f in glob(os.path.join(job_dir, "_*")):
        if os.path.isdir(f):
            shutil.copytree(f, os.path.join(triage_dir, os.path.basename(f)))
        else:
            shutil.copy(f, triage_dir)

    # triage log files
    for f in glob(os.path.join(job_dir, "*.log")):
        if os.path.isdir(f):
            shutil.copytree(f, os.path.join(triage_dir, os.path.basename(f)))
        else:
            shutil.copy(f, triage_dir)

    # triage additional globs
    for g in ctx.get("_triage_additional_globs", []):
        for f in glob(os.path.join(job_dir, g)):
            f = os.path.normpath(f)
            dst = os.path.join(triage_dir, os.path.basename(f))
            if os.path.exists(dst):
                dst = "{}.{}Z".format(dst, datetime.now(UTC).isoformat("T"))
            try:
                if os.path.islink(f):
                    # Skip broken symlinks
                    if not os.path.exists(f):
                        logger.warning(f"Skipping broken symlink: {f}")
                        continue
                    # For valid symlinks, copy the symlink itself, not the target
                    linkto = os.readlink(f)
                    os.symlink(linkto, dst)
                elif os.path.isdir(f):
                    try:
                        shutil.copytree(f, dst)
                    except (OSError, shutil.Error) as e:
                        # Create empty directory if we can't copy contents
                        logger.warning(f"Could not copy contents of {f} due to error: {str(e)}. Creating empty directory.")
                        os.makedirs(dst, exist_ok=True)
                else:
                    shutil.copy(f, dst)
            except Exception as e:
                tb = traceback.format_exc()
                logger.error(
                    "Skipping copying of {}. Got exception: {}\n{}".format(
                        f, str(e), tb
                    )
                )
                continue

    # publish
    # HC-502: It's ok to clobber triage
    ctx["_force_ingest"] = True
    prod_json = publish_dataset(triage_dir, ds_file, job, ctx)

    # write published triage to file
    pub_triage_file = os.path.join(job_dir, "_triaged.json")
    with open(pub_triage_file, "w") as f:
        json.dump(prod_json, f, indent=2, sort_keys=True)

    # signal run_job() to continue
    return True
