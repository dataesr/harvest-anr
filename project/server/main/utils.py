import json
import os
import requests
import pandas as pd
from retry import retry

from project.server.main.logger import get_logger

logger = get_logger(__name__)

ODS_API_KEY = os.getenv('ODS_API_KEY')

@retry(delay=20, tries=5)
def get_url(url, headers):
    return requests.get(url, headers=headers)

@retry(delay=20, tries=5)
def post_url(url, json, headers):
    return requests.post(url, json=json, headers=headers)

@retry(delay=20, tries=5)
def delete_url(url, headers):
    return requests.delete(url, headers=headers)

def get_ods_data(key):
    logger.debug(f'getting ods data {key}')
    current_df = pd.read_csv(f'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/{key}/download/?format=csv&apikey={ODS_API_KEY}', sep=';')
    return current_df

def post_data(data, delete_before=False):
    projects = data['projects']
    assert(len(projects) == len(set([e['id'] for e in projects])))
    logger.debug(f'{len(projects)} projects to post')
    nb_post_ok = 0
    for p in projects:
        nb_post_ok += upload_elt(p, 'projects')
    logger.debug(f'{nb_post_ok} OK')
    partners = data['partners']
    assert(len(partners) == len(set([e['id'] for e in partners])))
    logger.debug(f'{len(partners)} partners to post')
    nb_post_ok = 0
    for p in partners:
        nb_post_ok += upload_elt(p, 'participations', delete_before)
    logger.debug(f'{nb_post_ok} OK')

def reset_db(project_type, elt_type):
    logger.debug(f'reset {project_type} {elt_type}')
    assert(elt_type in ['projects', 'participations'])
    url = os.getenv('DATAESR_URL') + f'/projects/operations/remove/{elt_type}/{project_type}'
    r = requests.get(url, headers={"Authorization":os.getenv('AUTHORIZATION')}, timeout=600)
    logger.debug(r.text)

def reset_db_projects_and_partners(project_type):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')


def upload_elt(new_elt, elt_type, delete_before = False):
    assert(elt_type in ['projects', 'participations'])
    url = os.getenv('DATAESR_URL') + f'/projects/{elt_type}'
    if delete_before:
        elt_id = new_elt['id']
        old = get_url(url + f'/{elt_id}', headers={"Authorization":os.getenv('AUTHORIZATION')}).json()
        try:
            etag = old['etag']
            new_headers = {"Authorization":os.getenv('AUTHORIZATION'), 'If-Match': etag}
            delete_old = delete_url(url + f'/{elt_id}', headers=new_headers)
            if delete_old.status_code != 204:
                logger.debug(delete_old.text)
        except:
            pass
    r = post_url(url, json = new_elt, headers={"Authorization":os.getenv('AUTHORIZATION')})
    if(r.status_code != 201):
        time.sleep(10)
        r = post_url(url, json = new_elt, headers={"Authorization":os.getenv('AUTHORIZATION')})
        if(r.status_code != 201):
            logger.debug(r.text)
            logger.debug(new_elt)
            return 0
    return 1

def normalize(x):
    if not isinstance(x, str):
        return ''
    y = x.lower().replace('-', ' ').replace('é', 'e').replace('è', 'e')\
    .replace('-', ' ').replace('é', 'e').replace('è', 'e')\
    .replace('_', ' ').replace('à', 'a').replace('ê', 'e')\
    .replace(':', ' ').replace('ë', 'e').replace('"', ' ')\
    .replace('(', ' ').replace(')', ' ')\
    .replace('\xa0', ' ').replace(',', ' ')\
    .replace("'", ' ').replace('  ', '').replace(' ', '').strip()
    return y

def get_all_struct():
    df_struct = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz', lines=True)
    print(f'{len(df_struct)} struct downloaded')
    return df_struct.to_dict(orient='records')

def build_correspondance_structures(all_struct):
    corresp = {}
    for e in all_struct:
        corresp[e['id']] = e['id']
        for k in e.get('externalIds'):
            corresp[k['id']] = e['id']
    return corresp

def clean_json(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
        elif (isinstance(elt[f], str) and len(elt[f])==0):
            del elt[f]
        elif (isinstance(elt[f], list) and len(elt[f])==0):
            del elt[f]
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')
