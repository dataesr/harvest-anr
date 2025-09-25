import time
import datetime
import os
import requests
from project.server.main.anr import update_anr

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_update_anr(arg):
    update_anr(arg)
