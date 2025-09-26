import time
import datetime
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.anr import update_anr
from project.server.main.inca import update_inca

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_update(arg):
    cache_participant = enrich_cache()
    if arg.get('anr'):
        update_anr(arg, cache_participant)
    if arg.get('inca'):
        update_inca(arg, cache_participant)
