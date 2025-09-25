import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data
from project.server.main.anr import URL_ANR_PROJECTS_DGPIE
from project.server.main.logger import get_logger

logger = get_logger(__name__)

URL_INCA_2020_2021 = 'https://www.data.gouv.fr/api/1/datasets/r/14df9170-a0f9-4d52-8f91-ebecb8fcfc30'
URL_INCA_2008_2019 = 'https://www.data.gouv.fr/api/1/datasets/r/9f5ab856-9b65-4446-a014-474e76fcd4db'
URL_INCA_2022 = 'https://www.data.gouv.fr/api/1/datasets/r/9411c01a-5c91-467f-846c-70c9f2631c0c'

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

