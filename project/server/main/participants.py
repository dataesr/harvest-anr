import json
import pandas as pd

from project.server.main.utils import normalize, get_all_struct
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def enrich_cache():
    logger.debug(f'getting cache participant')
    cache_participant = json.load(open('cache_participant.json', 'r'))
    all_struct = get_all_struct()
    for s in all_struct:
        label = s.get('label')
        if isinstance(label, dict):
            for k in ['fr', 'en', 'default']:
                if label.get(k)==label.get(k):
                    label_n = normalize(label.get(k))
                    if len(label_n) > 5:
                        cache_participant[label_n] = s['id']
        if isinstance(s.get('alias'), list):
            for w in s.get('alias'):
                w_n = normalize(w)
                if len(w_n)>5:
                    cache_participant[w_n] = s['id']
    return cache_participant

def identify_participant(s, cache_participant):
    s_n = normalize(s)
    if s_n in cache_participant:
        return cache_participant[s_n]
    return None
