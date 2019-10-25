import os
import logging
import ckan.plugins as p

from ckan.model.types import make_uuid
from ckan.lib.celery_app import celery
from ckan.lib import jobs as job

from ckanext.archiver.tasks import _update_resource as test_update_resource
from ckanext.archiver.tasks import _update_package as test_update_package

log = logging.getLogger(__name__)


class Test:
    def __init__(self, a):
        self.a = a

    def error(self, message, whatever='', a=None, b=None, c=None, d=None, e=None):
        if whatever:
            pass
        print(message)

    def info(self, message, whatever='', a=None, b=None, c=None, d=None, e=None):
        if whatever:
            pass
        print(message)

def create_archiver_resource_task(resource, queue):
    from pylons import config
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        # earlier CKANs had ResourceGroup
        package = resource.resource_group.package
    else:
        package = resource.package
    task_id = '%s/%s/%s' % (package.name, resource.id[:4], make_uuid()[:4])
    ckan_ini_filepath = os.path.abspath(config['__file__'])
    job.enqueue(test_update_resource, [package.id], {'queue': queue, 'log': Test('a')})
    log.debug('Archival of resource put into celery queue %s: %s/%s url=%r',
              queue, package.name, resource.id, resource.url)


def create_archiver_package_task(package, queue):
    from pylons import config
    task_id = '%s/%s' % (package.name, make_uuid()[:4])
    ckan_ini_filepath = os.path.abspath(config['__file__'])
    job.enqueue(test_update_package, [package.id], {'queue': queue, 'log': Test('a')})
    log.debug('Archival of package put into celery queue %s: %s',
              queue, package.name)


def get_extra_from_pkg_dict(pkg_dict, key, default=None):
    for extra in pkg_dict.get('extras', []):
        if extra['key'] == key:
            return extra['value']
    return default
