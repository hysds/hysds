import json
from pprint import pprint, pformat


def get_job_json(info, job_type):
    """
    Basic passthru of job info.
    """

    return {
        'type': job_type,
        'params': info,
        'localize_urls': info.get('localize_urls', []),
    }


def ingest_dataset(info):
    """
    Create job json for dataset ingest.
    
    Example:

    job = {
            'type': 'ingest_dataset',
            'name': 'ingest_dataset-AIRS.2010.01.01.222',
            'params': {
              'dataset': 'AIRS.2010.01.01.222'
            },
            'localize_urls': [
              {
                'url': 'https://msas-dav-pub.jpl.nasa.gov/sciflo/work/sciflowuid-3234234/AIRS.2010.01.01.222/AIRS.2010.01.01.222.nc',
                'local_path': 'AIRS.2010.01.01.222/'
              },
              {
                'url': 'https://msas-dav-pub.jpl.nasa.gov/sciflo/work/sciflowuid-3234234/AIRS.2010.01.01.222/test.png',
                'local_path': 'AIRS.2010.01.01.222/'
              },
              {
                'url': 'https://msas-dav-pub.jpl.nasa.gov/sciflo/work/sciflowuid-3234234/AIRS.2010.01.01.222/AIRS.2010.01.01.222.met.json',
                'local_path': 'AIRS.2010.01.01.222/'
              }
            ]
          }
    """

    # build params
    params = {}
    params['dataset'] = info['dataset']
    job = {
            'type': 'ingest_dataset',
            'name': 'ingest_dataset-%s' % params['dataset'],
            'params': params,
            'localize_urls': info['dataset_urls']
          }

    #print "Job:"
    #pprint(job, indent=2)
    return job


def notify_by_email(info):
    """
    Create job json for email notification.
    
    Example:

    job = {
            'type': 'notify_by_email',
            'name': 'action-notify_by_email-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'Email CSK ingest',
            'username': 'ops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'emails': 'test@test.com,test2@test.com',
                'url': 'http://path_to_repo',
                'rule_name': 'Email CSK ingest'
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
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0: params['url'] = urls[0]
    else: params['url'] = None
    job = {
            'type': 'notify_by_email',
            'name': 'action-notify_by_email-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def notify_by_tweet(info):
    """
    Create job json for tweet notification.
    
    Example:

    job = {
            'type': 'notify_by_tweet',
            'name': 'action-notify_by_tweet-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'tweet CSK ingest',
            'username': 'ops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'url': 'http://path_to_repo',
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
    urls = rule_hit['_source']['urls']
    if len(urls) > 0: params['url'] = urls[0]
    else: params['url'] = None
    job = {
            'type': 'notify_by_tweet',
            'name': 'action-notify_by_tweet-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def ftp_push(info):
    """
    Create job json for FTP push.
    
    Example:

    job = {
            'type': 'ftp_push',
            'name': 'action-ftp_push-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'FTP Push CSK Calimap',
            'username': 'ops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'url': 'http://path_to_repo'
                'ftp_url': 'ftp://username:password@test.ftp.com/public/data_drop_off',
                'emails': 'test@test.com,test2@test.com',
                'rule_name': 'FTP Push CSK Calimap'
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
    params['ftp_url'] = kwargs['ftp_url']
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0: params['url'] = urls[0]
    else: params['url'] = None
    job = {
            'type': 'ftp_push',
            'name': 'action-ftp_push-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def sftp_push(info):
    """
    Create job json for FTP push.
    
    Example:

    job = {
            'type': 'sftp_push',
            'name': 'action-sftp_push-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'SFTP Push CSK Calimap',
            'username': 'ops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'url': 'http://path_to_repo'
                'sftp_url': 'sftp://username@test.ftp.com/public/data_drop_off',
                'emails': 'test@test.com,test2@test.com',
                'rule_name': 'SFTP Push CSK Calimap'
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
    params['sftp_url'] = kwargs['sftp_url']
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0: params['url'] = urls[0]
    else: params['url'] = None
    job = {
            'type': 'sftp_push',
            'name': 'action-sftp_push-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def rsync_push(info):
    """
    Create job json for rsync push.
    
    Example:

    job = {
            'type': 'rsync_push',
            'name': 'action-rsync_push-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'Rsync Push CSK Calimap',
            'username': 'ops', 
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'url': 'http://path_to_repo'
                'rsync_url': 'file://username@test.ftp.com/home/username/data_drop_off',
                'emails': 'test@test.com,test2@test.com',
                'rule_name': 'Rsync Push CSK Calimap'
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
    params['rsync_url'] = kwargs['rsync_url']
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0: params['url'] = urls[0]
    else: params['url'] = None
    job = {
            'type': 'rsync_push',
            'name': 'action-rsync_push-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def purge_dataset(info):
    """
    Create job json for dataset purge.
    
    Example:

    job = {
            'type': 'purge_dataset',
            'name': 'action-purge_dataset-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'tag': 'v0.3 purge',
            'username': 'ops', 
            'params': {
                'index': 'grq_dev_v03_csk',
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
                'url': 'http://path_to_repo'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    rule_hit = info['rule_hit']
    params['index'] = rule_hit['_index']
    params['doctype'] = rule_hit['_type']
    params['id'] = info['objectid']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0: params['url'] = urls[0]
    else: params['url'] = None
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']
    job = {
            'type': 'purge_dataset',
            'name': 'action-purge_dataset-%s' % info['objectid'],
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def purge_datasets(info):
    """
    Create job json for purge of datasets by query.
    
    Example:

    job = {
            'type': 'purge_datasets',
            'name': 'action-purge_datasets',
            'tag': 'v0.3 purge',
            'username': 'ops', 
            'params': {
                'index': 'grq_dev_v03_csk',
                'query': '<ES query>'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['index'] = info['index']
    params['query'] = info['query']
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']
    job = {
            'type': 'purge_datasets',
            'name': 'action-purge_datasets',
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def custom_script(info):
    """
    Create job json for custom script.
    
    Example:

    job = {
            'type': 'custom_script',
            'name': 'action-custom_script',
            'tag': 'v0.3 custom_script',
            'username': 'ops', 
            'params': {
                'index': 'grq_dev_v03_csk',
                'query': '<ES query>'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['index'] = info['index']
    params['query'] = info['query']
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']

    # update params with kwargs
    kwargs = json.loads(info['rule']['kwargs'])
    params.update(kwargs)

    job = {
            'type': 'custom_script',
            'name': 'action-custom_script',
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job


def import_prov_es(info):
    """
    Create job json for importing of PROV-ES JSON.
    
    Example:

    job = {
            'type': 'import_prov_es',
            'name': 'action-import_prov_es',
            'tag': 'v0.3_import',
            'username': 'ops', 
            'params': {
                'prod_url': '<dataset url>'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    rule_hit = info['rule_hit']
    params['index'] = rule_hit['_index']
    params['doctype'] = rule_hit['_type']
    params['id'] = info['objectid']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0:
        params['prod_url'] = urls[0]
        for url in urls:
            if url.startswith('s3'):
                params['prod_url'] = url
                break
    else: params['prod_url'] = None
    params['rule_name'] = info['rule']['rule_name']
    params['username'] = info['rule']['username']
    job = {
            'type': 'import_prov_es',
            'name': 'action-import_prov_es',
            'tag': params['rule_name'],
            'username': params['username'],
            'params': params,
            'localize_urls': []
          }

    #pprint(job, indent=2)
    return job
