import pandas as pd
import os
import requests
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, get_ods_data, get_all_struct, build_correspondance_structures
from project.server.main.anr import URL_ANR_PROJECTS_DGPIE
from project.server.main.logger import get_logger

logger = get_logger(__name__)

project_type = 'PIA hors ANR'
def update_pia(args):
    reset_db(project_type, 'projects')
    reset_db(project_type, 'participations')
    new_data_pia = harvest_pia_projects()
    post_data(new_data_pia)

def get_pid(x, df_paysage, corresp):
    try:
        siret = df_paysage[(df_paysage.index==x) & (df_paysage.id_type=='siret')].value[0]
        siren = siret[0:9]
        return corresp[siren]
    except:
        return None

def harvest_pia_projects():
    df_pia_anr = pd.read_json(URL_ANR_PROJECTS_DGPIE, orient='split')
    pia_anr_code = set(df_pia_anr['Projet.Code_Decision_ANR'].apply(lambda x:x[4:]).to_list())
    df_pia = get_ods_data('fr-esr-piaweb')
    df_pia = pd.read_csv('/Users/eric/Downloads/fr-esr-piaweb.csv', sep=';')
    df_pia = df_pia[df_pia['Code projet'].apply(lambda x: x not in pia_anr_code)]
    # for ids
    df_paysage = get_ods_data('fr-esr-paysage_structures_identifiants', sep=';')
    df_paysage = df_paysage.set_index('id_structure_paysage')
    all_struct = get_all_struct()
    corresp = build_correspondance_structures(all_struct)

    df_projects = df_pia[['Code projet', 'Acronyme',  'Domaine thématique',
        'Stratégie nationale', 'Action', 'Libellé', 'Dotation', 'Résumés', 'Début du projet']].drop_duplicates()
    projects = []
    for e in df_projects.to_dict(orient='records'):
        new_elt = {}
        new_elt['id'] = e['Code projet']
        new_elt['type'] = project_type
        if isinstance(e.get('Début du projet'), str):
            try:
                year = int(e['Début du projet'][0:4])
                new_elt['year'] = year
            except:
                pass
        acronym, title = None, None
        if isinstance(e.get('Acronyme'), str):
            acronym = e['Acronyme']
            new_elt['acronym'] = acronym
        if isinstance(e.get('Libellé'), str):
            title = e['Libellé']
            new_elt['name'] = {'en': title}
        elif acronym:
            new_elt['name'] = {'en': acronym}
        if isinstance(e.get('Action'), str):
            new_elt['action'] = [{'level': '1', 'code': e.get('Action'), 'name': e.get('Action')}]
        #if isinstance(e.get('Dotation'), float):
        #    new_elt['budget_financed'] = e['Dotation']
        if isinstance(e.get('Résumés'), str):
            new_elt['description']['en'] = e['Résumés']
        projects.append(new_elt)
    partners = []
    for e in df_pia.to_dict(orient='records'):
        new_elt = {}
        new_elt['project_type'] = project_type
        new_elt['project_id'] = e['Code projet']
        part_id = None
        if isinstance(e.get("Établissement"), str):
            new_part['name'] = e["Établissement"]
        new_part['role'] = 'participant'
        if isinstance(e.get('Coordinateur (oui/non)'), str):
            if e["Coordinateur (oui/non)"] == "Oui":
                new_part['role'] = 'coordinator'
        if isinstance(e.get('id_paysage'), str):
            paysage_id = e['id_paysage'].split(',')[0]
            part_id = get_pid(paysage_id, df_paysage, corresp)
            part_id = identify_participant(new_part['name'], cache_participant)
        if part_id:
            new_part['participant_id'] = part_id
            new_part['organizations_id'] = part_id
            new_part['identified'] = True
        else:
            new_part['identified'] = False
        partners.append(new_elt)
    return {'projects': projects, 'partners': partners}
    
