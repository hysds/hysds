#!/usr/bin/env python
import os, sys, argparse

import hysds.orchestrator
from hysds.celery import app
from hysds.dataset_ingest import ingest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest dataset into HySDS.")
    parser.add_argument('ds_dir', help="dataset directory")
    parser.add_argument('datasets_cfg', help="datasets config JSON")
    args = parser.parse_args()
    ingest(os.path.basename(os.path.normpath(args.ds_dir)), args.datasets_cfg,
           app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE,
           os.path.abspath(args.ds_dir), None)
