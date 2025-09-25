import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, get_ods_data
from project.server.main.anr import URL_ANR_PROJECTS_DGPIE
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def update_pia(args):
    df_pia_anr = pd.read_json(URL_ANR_PROJECTS_DGPIE, orient='split')
    df_pia = get_ods_data('fr-esr-piaweb')
    reset_db('ANR', 'projects')
    reset_db('ANR', 'participations')
    new_data_anr = harvest_anr_projects('ANR', cache_participant)
    post_data(new_data_anr)
    
    reset_db('PIA', 'projects')
    reset_db('PIA', 'participations')
    new_data_pia = harvest_anr_projects('PIA', cache_participant)
    post_data(new_data_pia)

