from __future__ import absolute_import

import sys, os, re, socket, types, pwd, json
from string import Template
from pprint import pprint


def isLocalUrl(url):
    """If URL is file: URL on this host, or a local path, return the simple path, else return None."""
    localHost = socket.getfqdn()
    if url.startswith('/'):
        path = url
    elif url.startswith('file://'+localHost) or url.startswith('file:///'):
        url = url[7:]
        path =  url[url.index('/'):]
    elif os.path.exists(url):
        path = os.path.abspath(url)
    else:
        path = None
    return path

    
class RecognizerError(Exception): pass


class Recognizer:
    def __init__(self, dataset_file, path, id, version):
        self.dataset_file = dataset_file
        self.id = id
        self.version = version

        # save fqdn hostname
        self.hostname = socket.getfqdn()

        # parse json
        self.dataset_info = json.load(open(self.dataset_file))    

        # compile match patterns
        for ds in self.dataset_info['datasets']:
            if 'match_pattern' not in ds:
                raise RecognizerError("No 'match_pattern' defined:\n%s" % pprint(ds))
            ds['match_pattern'] = re.compile(ds['match_pattern'])
            if 'alt_match_pattern' not in ds: ds['alt_match_pattern'] = None
            if ds['alt_match_pattern'] is not None:
                ds['alt_match_pattern'] = re.compile(ds['alt_match_pattern'])

        # recognized dataset
        self.recognized = None
        self.group_dict = None
        self.currentIpath = None

        # recognize
        self._recognize(path)

    def _recognize(self, path):
        """Recognize and return ipath. Otherwise raise RecognizerError."""

        for ds in self.dataset_info['datasets']: 
            match = ds['match_pattern'].search(path)
            if match: 
                self.group_dict = match.groupdict()
                if 'version' not in self.group_dict:
                    self.group_dict['version'] = self.version
                if 'level' not in self.group_dict:
                    self.group_dict['level'] = ds['level']
                if 'type' not in self.group_dict:
                    self.group_dict['type'] = ds['type']
                if ds['alt_match_pattern'] is not None:
                    alt_match = ds['alt_match_pattern'].search(path)
                    if alt_match:
                        self.group_dict.update(alt_match.groupdict())
                    else:
                        alt_matched = False
                        for i in os.listdir(path):
                            alt_match2 = ds['alt_match_pattern'].search(os.path.join(path, i))
                            if alt_match2:
                                self.group_dict.update(alt_match2.groupdict())
                                alt_matched = True
                                break
                        if not alt_matched: continue
                self.recognized = ds
                self.currentIpath = ds['ipath']
                return self.currentIpath
        raise RecognizerError("No dataset configured for %s. Check %s." % (path, self.dataset_file))

    def getId(self):
        """Generate and return the id."""
        
        if self.recognized is None: return None
        else: return self.id
        
    def getVersion(self):
        """Get the version."""
        
        if self.recognized is None: return None
        else:
            return Template(self.recognized['version']).substitute(self.group_dict)
        
    def getLevel(self):
        """Get the level."""
        
        if self.recognized is None: return None
        else:
            return Template(self.recognized['level']).substitute(self.group_dict)
        
    def getType(self):
        """Get the type."""
        
        if self.recognized is None: return None
        else:
            return Template(self.recognized['type']).substitute(self.group_dict)
        
    def getPublishPath(self):
        """Generate and return the publish path."""
        
        if self.recognized is None: return None
        else:
            return Template(self.recognized['publish']['location']).substitute(
                self.group_dict, hostname=self.hostname)
        
    def getBrowsePath(self):
        """Generate and return the browse path."""
        
        if self.recognized is None: return None
        else:
            return Template(self.recognized['browse']['location']).substitute(
                self.group_dict, hostname=self.hostname)
        
    def getPriority(self):
        """Return processing priority."""
        
        if self.recognized is None: return None
        if 'priority' not in self.recognized: return None
        return self.recognized['priority']
        
    def getMetadataExtractor(self):
        """Return the metadata extractor."""
        
        if self.recognized is None: return None
        else:
            if 'extractor' not in self.recognized: return None
            extractor = self.recognized['extractor']
            if isinstance(extractor, types.StringTypes):
                return os.path.expandvars(extractor)
            else: return extractor

    def getPublishUrls(self):
        """Generate and return the publish urls."""
        
        if self.recognized is None: return None
        else:
            pub_urls = []
            for pub_url in self.recognized['publish']['urls']:
                pub_urls.append(Template(pub_url).substitute(self.group_dict,
                                                             hostname=self.hostname))
            return pub_urls

    def getBrowseUrls(self):
        """Generate and return the browse urls."""
        
        if self.recognized is None: return None
        else:
            brs_urls = []
            for brs_url in self.recognized['browse']['urls']:
                brs_urls.append(Template(brs_url).substitute(self.group_dict,
                                                             hostname=self.hostname))
            return brs_urls

    def getBrowseSortOrder(self):
        """Return the browse sort order info."""
        
        if self.recognized is None: return None
        else:
            if 'sort_order' not in self.recognized['browse']: return None
            sort_order = self.recognized['browse']['sort_order']
            if sort_order is None: sort_order = []
            return sort_order

    def getS3Keys(self, pub_type="publish"):
        """Return a tuple of s3 keys"""
        if self.recognized is None: return (None, None)
        else:
            return (self.recognized[pub_type].get("s3-secret-key", None),
                    self.recognized[pub_type].get("s3-api-key", None))

    def getS3Acl(self, pub_type="publish"):
        """Return a s3 acl"""
        if self.recognized is None: return "bucket-owner-read"
        else:
            return self.recognized[pub_type].get("s3-acl", "bucket-owner-read")

    def getS3Profile(self, pub_type="publish"):
        """Return a s3 profile to use"""
        if self.recognized is None: return None
        else:
            return self.recognized[pub_type].get("s3-profile-name", None)
