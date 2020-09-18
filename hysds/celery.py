from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division

from future import standard_library
standard_library.install_aliases()
from celery import Celery
import os


celery_cfg_module = os.environ.get('HYSDS_CELERY_CFG_MODULE', None)
if celery_cfg_module is None:
  print('env variable HYSDS_CELERY_CFG_MODULE must be set.')

app = Celery('hysds')
app.config_from_envvar('HYSDS_CELERY_CFG_MODULE')
### app.config_from_object('celeryconfig')


if __name__ == '__main__':
    app.start()
