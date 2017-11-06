#!/usr/bin/env python
import os, sys, argparse, logging

import hysds.orchestrator
from hysds.celery import app
from hysds.dataset_ingest import ingest


logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest dataset into HySDS.")
    parser.add_argument('ds_dir', help="dataset directory")
    parser.add_argument('datasets_cfg', help="datasets config JSON")
    parser.add_argument('-d', '--dry-run', action='store_true',
                        help="Don't upload dataset or ingest into GRQ")
    parser.add_argument('-f', '--force', action='store_true',
                        help="Force publish of dataset even if it clobbers")
    args = parser.parse_args()
    ingest(os.path.basename(os.path.normpath(args.ds_dir)), args.datasets_cfg,
           app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE,
           os.path.abspath(args.ds_dir), None, args.dry_run, args.force)
