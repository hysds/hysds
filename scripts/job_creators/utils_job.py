import json, types
from pprint import pprint, pformat


def notify_job_by_email(info):
    """
    Create job json for email notification.
    
    Example:

    job = {
            'type': 'notify_job_by_email',
            'name': 'action-notify_job_by_email-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'Email CSK ingest',
            'username': 'ariaops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'emails': 'test@test.com,test2@test.com',
                'url': 'http://path_to_job',
                'rule_name': 'Email CSK ingest'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['id'] = info['job_id']
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']
    kwargs = json.loads(info['rule']['kwargs'])
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    params['url'] = rule_hit['_source']['job']['job_info']['job_url']
    job = {
            'type': 'notify_job_by_email',
            'name': 'action-notify_job_by_email-%s' % info['job_id'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    return job


def notify_job_by_tweet(info):
    """
    Create job json for tweet notification.
    
    Example:

    job = {
            'type': 'notify_job_by_tweet',
            'name': 'action-notify_job_by_tweet-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'tweet CSK ingest',
            'username': 'ariaops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'url': 'http://path_to_job',
                'rule_name': 'tweet CSK ingest',
                'hash_tags': '#thisrocks #datageek'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['id'] = info['objectid']
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']
    kwargs = json.loads(info['rule']['kwargs'])
    params['hash_tags'] = kwargs['hash_tags']
    rule_hit = info['rule_hit']
    params['url'] = rule_hit['_source']['job']['job_info']['job_url']
    job = {
            'type': 'notify_job_by_tweet',
            'name': 'action-notify_job_by_tweet-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    return job


def retry_job(info):
    """
    Create job json for job retry.
    """

    # build params
    params = {}
    params['id'] = info['job_id']
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']
    kwargs = json.loads(info['rule']['kwargs'])
    rule_hit = info['rule_hit']
    params['job'] = rule_hit['_source']['job']
    job_id = params['job'].get('job_id', None)
    if isinstance(job_id, types.StringTypes) and job_id.startswith('action-retry_job'):
        name = job_id
    else:
        name = 'action-retry_job-%s' % job_id
    job = {
            'type': 'retry_job',
            'name': name,
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    return job
