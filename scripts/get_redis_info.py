#!/usr/bin/env python
"""
Helper script to extract Redis connection information from Celery config.
This helps you connect to Redis using redis-cli with TLS.
"""
import sys
import re
from urllib.parse import urlparse

try:
    from hysds.celery import app
except ImportError:
    print("Error: Could not import hysds.celery. Make sure you're in the correct environment.")
    sys.exit(1)

def parse_redis_url(url):
    """Parse a Redis URL and extract connection details."""
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 6379,
        'password': parsed.password or '',
        'scheme': parsed.scheme,
        'uses_tls': parsed.scheme == 'rediss'
    }

def main():
    redis_url = app.conf.get('REDIS_JOB_STATUS_URL')
    if not redis_url:
        print("Error: REDIS_JOB_STATUS_URL not found in Celery config")
        sys.exit(1)
    
    print("=" * 60)
    print("Redis Connection Information")
    print("=" * 60)
    print(f"URL: {redis_url}")
    print()
    
    conn_info = parse_redis_url(redis_url)
    
    print("Connection Details:")
    print(f"  Host: {conn_info['host']}")
    print(f"  Port: {conn_info['port']}")
    print(f"  Uses TLS: {conn_info['uses_tls']}")
    print(f"  Has Password: {'Yes' if conn_info['password'] else 'No'}")
    print()
    
    if conn_info['uses_tls']:
        print("redis-cli Command (with TLS):")
        if conn_info['password']:
            print(f"  redis-cli --tls --insecure -h {conn_info['host']} -p {conn_info['port']} -a '{conn_info['password']}'")
        else:
            print(f"  redis-cli --tls --insecure -h {conn_info['host']} -p {conn_info['port']}")
        print()
        print("Note: --insecure flag is used because ssl_cert_reqs='none' in the config")
        print("      (certificate verification is disabled)")
    else:
        print("redis-cli Command (without TLS):")
        if conn_info['password']:
            print(f"  redis-cli -h {conn_info['host']} -p {conn_info['port']} -a '{conn_info['password']}'")
        else:
            print(f"  redis-cli -h {conn_info['host']} -p {conn_info['port']}")
    
    print()
    print("Example query:")
    print('  redis-cli --tls --insecure -h <HOST> -p <PORT> -a "<PASSWORD>" KEYS "job-lock-*"')

if __name__ == '__main__':
    main()











