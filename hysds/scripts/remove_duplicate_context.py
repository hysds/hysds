#!/usr/bin/env python3
"""
Remove duplicate top-level 'context' field from job_status documents in OpenSearch.

This script is used to clean up existing job_status documents after restoring
a snapshot from a cluster that has the duplicate context field (pre-HC-598).

Usage:
    python remove_duplicate_context.py --host <opensearch_host> [options]

Examples:
    # Dry run (count documents only)
    python remove_duplicate_context.py --host localhost:9201 --dry-run

    # Remove context field from all job_status indices
    python remove_duplicate_context.py --host localhost:9201

    # Remove from specific index pattern
    python remove_duplicate_context.py --host localhost:9201 --index "job_status-2024*"

    # With authentication
    python remove_duplicate_context.py --host localhost:9201 --user admin --password <pass>
"""

import argparse
import json
import sys
import time
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth


def get_auth(args):
    """Get authentication if credentials provided."""
    if args.user and args.password:
        return HTTPBasicAuth(args.user, args.password)
    return None


def make_request(method, url, auth=None, json_data=None, verify=True):
    """Make HTTP request with error handling."""
    try:
        response = requests.request(
            method,
            url,
            auth=auth,
            json=json_data,
            verify=verify,
            timeout=300
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None


def count_documents_with_context(base_url, index_pattern, auth, verify):
    """Count documents that have the top-level context field using exists query."""
    url = f"{base_url}/{index_pattern}/_count"
    query = {
        "query": {
            "exists": {
                "field": "context"
            }
        }
    }
    result = make_request("POST", url, auth=auth, json_data=query, verify=verify)
    if result:
        return result.get("count", 0)
    return 0


def sample_documents_for_context(base_url, index_pattern, auth, verify, sample_size=100):
    """
    Sample documents to check if context field exists in _source.
    This handles the case where context is mapped with enabled:false (not indexed).
    Returns (count_with_context, total_sampled).
    """
    url = f"{base_url}/{index_pattern}/_search"
    query = {
        "size": sample_size,
        "_source": ["context"],
        "query": {"match_all": {}}
    }
    result = make_request("POST", url, auth=auth, json_data=query, verify=verify)
    if not result:
        return 0, 0

    hits = result.get("hits", {}).get("hits", [])
    count_with_context = sum(1 for hit in hits if "context" in hit.get("_source", {}))
    return count_with_context, len(hits)


def count_total_documents(base_url, index_pattern, auth, verify):
    """Count total documents in the index pattern."""
    url = f"{base_url}/{index_pattern}/_count"
    result = make_request("POST", url, auth=auth, verify=verify)
    if result:
        return result.get("count", 0)
    return 0


def start_update_by_query(base_url, index_pattern, auth, verify, scroll_size=1000, slices="auto", use_match_all=False):
    """Start async update_by_query to remove context field.

    Args:
        use_match_all: If True, use match_all query and check for context in script.
                      This is needed when context field is mapped with enabled:false.
    """
    url = f"{base_url}/{index_pattern}/_update_by_query"
    params = f"?scroll_size={scroll_size}&slices={slices}&wait_for_completion=false&conflicts=proceed"

    if use_match_all:
        # For fields mapped with enabled:false, we can't use exists query
        # Instead, use match_all and check/remove in the painless script
        query = {
            "script": {
                "source": "if (ctx._source.containsKey('context')) { ctx._source.remove('context') } else { ctx.op = 'noop' }",
                "lang": "painless"
            },
            "query": {
                "match_all": {}
            }
        }
    else:
        query = {
            "script": {
                "source": "ctx._source.remove('context')",
                "lang": "painless"
            },
            "query": {
                "exists": {
                    "field": "context"
                }
            }
        }

    result = make_request("POST", url + params, auth=auth, json_data=query, verify=verify)
    if result:
        return result.get("task")
    return None


def get_task_status(base_url, task_id, auth, verify):
    """Get the status of an async task."""
    url = f"{base_url}/_tasks/{task_id}"
    return make_request("GET", url, auth=auth, verify=verify)


def wait_for_task(base_url, task_id, auth, verify, poll_interval=5):
    """Wait for async task to complete, showing progress."""
    print(f"\nTask ID: {task_id}")
    print("Monitoring progress...")

    last_updated = 0
    start_time = time.time()

    while True:
        status = get_task_status(base_url, task_id, auth, verify)
        if not status:
            print("Failed to get task status")
            return None

        task = status.get("task", {})
        task_status = task.get("status", {})

        total = task_status.get("total", 0)
        updated = task_status.get("updated", 0)
        deleted = task_status.get("deleted", 0)
        noops = task_status.get("noops", 0)

        if total > 0:
            progress = (updated + noops) / total * 100
            elapsed = time.time() - start_time

            if updated > last_updated:
                rate = updated / elapsed if elapsed > 0 else 0
                eta = (total - updated) / rate if rate > 0 else 0
                print(f"\rProgress: {progress:.1f}% ({updated:,}/{total:,}) - "
                      f"Rate: {rate:.0f} docs/sec - ETA: {eta:.0f}s", end="", flush=True)
                last_updated = updated

        if status.get("completed"):
            print(f"\n\nTask completed!")
            return status

        time.sleep(poll_interval)


def force_merge(base_url, index_pattern, auth, verify, max_segments=1):
    """Force merge indices to reclaim disk space."""
    print(f"\nForce merging {index_pattern} (max_segments={max_segments})...")
    url = f"{base_url}/{index_pattern}/_forcemerge?max_num_segments={max_segments}"
    result = make_request("POST", url, auth=auth, verify=verify)
    if result:
        print("Force merge completed")
        return True
    return False


def get_index_stats(base_url, index_pattern, auth, verify):
    """Get index statistics including size."""
    url = f"{base_url}/{index_pattern}/_stats/store"
    return make_request("GET", url, auth=auth, verify=verify)


def main():
    parser = argparse.ArgumentParser(
        description="Remove duplicate top-level 'context' field from job_status documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--host",
        required=True,
        help="OpenSearch host (e.g., localhost:9200 or https://opensearch.example.com)"
    )
    parser.add_argument(
        "--index",
        default="job_status-*",
        help="Index pattern to update (default: job_status-*)"
    )
    parser.add_argument(
        "--user",
        help="Username for authentication"
    )
    parser.add_argument(
        "--password",
        help="Password for authentication"
    )
    parser.add_argument(
        "--scroll-size",
        type=int,
        default=1000,
        help="Scroll size for update_by_query (default: 1000)"
    )
    parser.add_argument(
        "--slices",
        default="auto",
        help="Number of slices for parallel processing (default: auto)"
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable SSL certificate verification"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only count documents, don't make changes"
    )
    parser.add_argument(
        "--skip-force-merge",
        action="store_true",
        help="Skip force merge after update"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Polling interval in seconds for task status (default: 5)"
    )

    args = parser.parse_args()

    # Build base URL
    if not args.host.startswith("http"):
        base_url = f"http://{args.host}"
    else:
        base_url = args.host
    base_url = base_url.rstrip("/")

    auth = get_auth(args)
    verify = not args.no_verify_ssl

    print("=" * 60)
    print("Remove Duplicate Context Field from Job Status Documents")
    print("=" * 60)
    print(f"Host: {base_url}")
    print(f"Index pattern: {args.index}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Count documents
    print("Counting documents...")
    total_docs = count_total_documents(base_url, args.index, auth, verify)
    docs_with_context = count_documents_with_context(base_url, args.index, auth, verify)

    print(f"Total documents in {args.index}: {total_docs:,}")
    print(f"Documents with top-level 'context' field (indexed): {docs_with_context:,}")

    # If exists query returns 0, the context field might be mapped with enabled:false
    # In that case, sample documents to check _source directly
    use_match_all = False
    if docs_with_context == 0 and total_docs > 0:
        print("\nExists query found 0 documents. Checking if 'context' exists in _source...")
        print("(This happens when the field is mapped with 'enabled: false')")
        sampled_with_context, total_sampled = sample_documents_for_context(
            base_url, args.index, auth, verify, sample_size=100
        )
        print(f"Sample check: {sampled_with_context}/{total_sampled} documents have 'context' in _source")

        if sampled_with_context > 0:
            # Context exists in _source but not indexed - need to use match_all approach
            use_match_all = True
            estimated_pct = (sampled_with_context / total_sampled * 100) if total_sampled > 0 else 0
            docs_with_context = int(total_docs * estimated_pct / 100)  # Estimate
            print(f"Estimated documents with context: ~{docs_with_context:,} ({estimated_pct:.1f}%)")
            print("Will use match_all query with script-based filtering.")
        else:
            print("\nNo documents have the top-level 'context' field. Nothing to do.")
            return 0
    elif docs_with_context == 0:
        print("\nNo documents have the top-level 'context' field. Nothing to do.")
        return 0

    percentage = (docs_with_context / total_docs * 100) if total_docs > 0 else 0
    print(f"Percentage to update: {percentage:.1f}%")

    # Get initial index size
    print("\nGetting initial index size...")
    initial_stats = get_index_stats(base_url, args.index, auth, verify)
    if initial_stats:
        initial_size = initial_stats.get("_all", {}).get("total", {}).get("store", {}).get("size_in_bytes", 0)
        print(f"Initial total size: {initial_size / (1024**3):.2f} GB")
    else:
        initial_size = 0

    if args.dry_run:
        print("\n[DRY RUN] No changes made. Remove --dry-run to proceed.")
        return 0

    # Confirm before proceeding
    if use_match_all:
        print(f"\nThis will process ALL {total_docs:,} documents and remove 'context' where present.")
        print("(Using match_all because field is not indexed)")
    else:
        print(f"\nThis will remove the 'context' field from {docs_with_context:,} documents.")
    response = input("Do you want to proceed? [y/N]: ")
    if response.lower() != 'y':
        print("Aborted.")
        return 1

    # Start update_by_query
    print(f"\nStarting update_by_query (scroll_size={args.scroll_size}, slices={args.slices}, use_match_all={use_match_all})...")
    task_id = start_update_by_query(
        base_url, args.index, auth, verify,
        scroll_size=args.scroll_size,
        slices=args.slices,
        use_match_all=use_match_all
    )

    if not task_id:
        print("Failed to start update_by_query task")
        return 1

    # Wait for completion
    result = wait_for_task(base_url, task_id, auth, verify, poll_interval=args.poll_interval)

    if result:
        response = result.get("response", {})
        print(f"\nResults:")
        print(f"  Updated: {response.get('updated', 0):,}")
        print(f"  Deleted: {response.get('deleted', 0):,}")
        print(f"  Noops: {response.get('noops', 0):,}")
        print(f"  Failures: {len(response.get('failures', []))}")

        if response.get('failures'):
            print("\nFailures:")
            for failure in response.get('failures', [])[:5]:
                print(f"  - {failure}")
            if len(response.get('failures', [])) > 5:
                print(f"  ... and {len(response.get('failures', [])) - 5} more")

    # Verify removal
    print("\nVerifying removal...")
    if use_match_all:
        # Use sampling to verify since exists query won't work
        sampled_with_context, total_sampled = sample_documents_for_context(
            base_url, args.index, auth, verify, sample_size=100
        )
        print(f"Sample verification: {sampled_with_context}/{total_sampled} documents still have 'context'")
    else:
        remaining = count_documents_with_context(base_url, args.index, auth, verify)
        print(f"Documents still with 'context' field: {remaining:,}")

    # Force merge
    if not args.skip_force_merge:
        print("\nForce merge will reclaim disk space but may take a while.")
        response = input("Do you want to force merge? [y/N]: ")
        if response.lower() == 'y':
            force_merge(base_url, args.index, auth, verify)

            # Get final size
            print("\nGetting final index size...")
            final_stats = get_index_stats(base_url, args.index, auth, verify)
            if final_stats and initial_size > 0:
                final_size = final_stats.get("_all", {}).get("total", {}).get("store", {}).get("size_in_bytes", 0)
                print(f"Final total size: {final_size / (1024**3):.2f} GB")
                reduction = (1 - final_size / initial_size) * 100
                print(f"Size reduction: {reduction:.1f}%")

    print("\nDone!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
