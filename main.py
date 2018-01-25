from __future__ import print_function

import os
import uuid
from multiprocessing import Process
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
from stylelens_product.crawls import Crawls
import redis
import pickle

from bluelens_log import Logging

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_PRODUCT_HOST = os.environ['DB_PRODUCT_HOST']
DB_PRODUCT_PORT = os.environ['DB_PRODUCT_PORT']
DB_PRODUCT_USER = os.environ['DB_PRODUCT_USER']
DB_PRODUCT_PASSWORD = os.environ['DB_PRODUCT_PASSWORD']
DB_PRODUCT_NAME = os.environ['DB_PRODUCT_NAME']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

REDIS_PRODUCT_TEXT_MODEL_PROCESS_QUEUE = 'bl:product:text:model:process:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

SPAWNING_CRITERIA = 50
PROCESSING_TERM = 60

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-model')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

def get_latest_crawl_version(rconn):
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  if value is None:
    return None

  log.debug(value)
  try:
    version_id = value.decode("utf-8")
  except Exception as e:
    log.error(str(e))
    version_id = None
  return version_id

def cleanup_products(host_code, version_id):
  global product_api
  try:
    res = product_api.delete_products_by_hostcode_and_version_id(host_code, version_id,
                                                                 except_version=True)
    log.debug(res)
  except Exception as e:
    log.error(e)

def clear_product_queue(rconn):
  rconn.delete(REDIS_PRODUCT_TEXT_MODEL_PROCESS_QUEUE)

def push_product_to_queue(product):
  rconn.lpush(REDIS_PRODUCT_TEXT_MODEL_PROCESS_QUEUE, pickle.dumps(product))

def query(host_code, version_id):
  global product_api
  log.info('start query: ' + host_code)

  spawn_counter = 0

  q_offset = 0
  q_limit = 500

  try:
    while True:
      res = product_api.get_products_by_hostcode_and_version_id(host_code, version_id,
                                                                is_processed=False,
                                                                offset=q_offset, limit=q_limit)
      for p in res:
        push_product_to_queue(p)

      if len(res) == 0:
        break
      else:
        q_offset = q_offset + q_limit

  except Exception as e:
    log.error(str(e) + ':' + host_code)


def spawn(uuid):
  log.debug('RELEASE_MODE:' + RELEASE_MODE)

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-text-classification-modeler-' + uuid
  log.debug('spawn bl-text-classification-modeler: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-text-classification-modeler')
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'DB_PRODUCT_HOST', DB_PRODUCT_HOST)
  pool.addContainerEnv(container, 'DB_PRODUCT_PORT', DB_PRODUCT_PORT)
  pool.addContainerEnv(container, 'DB_PRODUCT_USER', DB_PRODUCT_USER)
  pool.addContainerEnv(container, 'DB_PRODUCT_PASSWORD', DB_PRODUCT_PASSWORD)
  pool.addContainerEnv(container, 'DB_PRODUCT_NAME', DB_PRODUCT_NAME)
  pool.setContainerImage(container, 'bluelens/bl-text-classification-modeler' + RELEASE_MODE)
  pool.setContainerImagePullPolicy(container, 'Always')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def dispatch(rconn, version_id):
  spawn(str(uuid.uuid4()))

def remove_prev_pods():
  pool = spawning_pool.SpawningPool()
  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setMetadataNamespace(RELEASE_MODE)
  data = {}
  data['key'] = 'group'
  data['value'] = 'bl-text-classification-modeler'
  pool.delete(data)
  time.sleep(60)

def prepare_products(rconn, version_id):
  global product_api
  offset = 0
  limit = 200

  clear_product_queue(rconn)
  remove_prev_pods()
  try:
    log.info('prepare_products')
    while True:
      res = product_api.get_products_by_version_id(version_id=version_id,
                                                   is_processed_for_text_class_model=False,
                                                   offset=offset,
                                                   limit=limit)

      log.debug("Got " + str(len(res)) + ' products')
      for product in res:
        push_product_to_queue(product)

      if len(res) == 0:
        break
      else:
        offset = offset + limit

  except Exception as e:
    log.error(str(e))

def check_condition_to_start(version_id):
  global product_api

  product_api = Products()
  crawl_api = Crawls()

  try:
    log.info("check_condition_to_start")
    # Check if crawling process is done
    total_crawl_size = crawl_api.get_size_crawls(version_id)
    crawled_size = crawl_api.get_size_crawls(version_id, status='done')
    if total_crawl_size != crawled_size:
      return False

    queue_size = rconn.llen(REDIS_PRODUCT_TEXT_MODEL_PROCESS_QUEUE)
    if queue_size > 0:
      return False

    total_product_size = product_api.get_size_products(version_id)
    processed_product_size = product_api.get_size_products(version_id, is_processed_for_text_class_model=True)
    not_processed_product_size = product_api.get_size_products(version_id, is_processed_for_text_class_model=False)

    if (processed_product_size + not_processed_product_size) == total_product_size:
      return False

  except Exception as e:
    log.error(str(e))

  return True

def start(rconn):
  while True:
    version_id = get_latest_crawl_version(rconn)
    if version_id is not None:
      log.info("check_condition_to_start")
      ok = check_condition_to_start(version_id)
      log.info("check_condition_to_start: " + str(ok))
      if ok is True:
        rconn.lpush(REDIS_PRODUCT_TEXT_MODEL_PROCESS_QUEUE, 'start')
        dispatch(rconn, version_id)
    time.sleep(60*10)

if __name__ == '__main__':
  log.info('Start bl-model:1')
  try:
    Process(target=start, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
