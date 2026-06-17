import pandas as pd
import os
import requests
from retry import retry
from project.server.main.participants import identify_participant, enrich_cache
from project.server.main.utils import reset_db, upload_elt, post_data, transform_scanr
from project.server.main.logger import get_logger

logger = get_logger(__name__)

#URL_INCA_2020_2021 = 'https://www.data.gouv.fr/api/1/datasets/r/14df9170-a0f9-4d52-8f91-ebecb8fcfc30'
#URL_INCA_2008_2019 = 'https://www.data.gouv.fr/api/1/datasets/r/9f5ab856-9b65-4446-a014-474e76fcd4db'
#URL_INCA_2022 = 'https://www.data.gouv.fr/api/1/datasets/r/9411c01a-5c91-467f-846c-70c9f2631c0c'
URL_INCA = 'https://www.data.gouv.fr/api/1/datasets/r/478b1659-1b3c-4cd6-8f47-190a5bf542a9'

def update_inca_v2(args, cache_participant):
    new_data_inca = harvest_inca_projects(cache_participant)
    transform_scanr(new_data_inca)

def update_inca(args, cache_participant):
    reset_db('INCa', 'projects')
    reset_db('INCa', 'participations')
    new_data_inca = harvest_inca_projects(cache_participant)
    post_data(new_data_inca)

@retry(delay=20, tries=3)
def harvest_inca_projects(cache_participant):
    projects, partners = [], []
    #df1 = pd.read_excel(URL_INCA_2020_2021)
    #df2 = pd.read_excel(URL_INCA_2008_2019)
    #df3 = pd.read_excel(URL_INCA_2022)
    #df_inca = pd.concat([df1, df2, df3]).drop_duplicates()
    df_inca = pd.read_excel(URL_INCA).drop_duplicates()
    for e in df_inca.to_dict(orient='records'):
        new_elt = {}
        #project_id = str(e['N° subvention']).replace('\xa0', '')
        project_id = str(e['Reference']).replace('\xa0', '')
        if 'INCa' not in project_id:
            project_id = 'INCa-'+project_id
        new_elt['id'] = project_id
        project_type = 'INCa'
        new_elt['type'] = project_type
        year = None
        try:
            year = int(e['Call.Year'])
        except:
            pass
        if year:
            new_elt['year'] = year

        new_elt['name'] = {}
        if isinstance(e.get('Title'), str):
            new_elt['name']['en'] = e.get('Title')
            new_elt['name']['fr'] = e.get('Title')
        description = {}
        if isinstance(e.get('Summary.En'), str):
            description['en'] = e.get('Summary.En')
        if isinstance(e.get('Summary.Fr'), str):
            description['fr'] = e.get('Summary.Fr')
        if description:
            new_elt['description'] = description
        if isinstance(e.get('Call.Description'), str):
            new_elt['action'] = [{'level': '1', 'code': e.get('Call.Reference'), 'name': e.get('Call.Description')}]

        if isinstance(e.get('Amount'), float) or isinstance(e.get('Amount'), int):
            if e.get('Amount') == e.get('Amount'):
                new_elt['budget_financed'] = float(e.get('Amount'))
        elif isinstance(e.get('Amount'), str):
            new_elt['budget_financed'] = float(e.get('Amount').replace('€', '')\
                                               .replace('\xa0', '').replace(',', '.')\
                                               .replace(' ', '').strip())
        person = {}
        if isinstance(e.get('Investigator.Lastname'), str):
            person['last_name'] = e.get('Investigator.Lastname')
            person['role'] = 'coordinator'
        if isinstance(e.get('Investigator.Firstname'), str):
            person['first_name'] = e.get('Investigator.Firstname')
        if person:
            new_elt['persons'] = [person]
        projects.append(new_elt)

        new_part = {}
        new_part['id'] = project_id+'-01'
        new_part['project_id'] = project_id
        new_part['project_type'] = project_type
        part_id = None
        if isinstance(e.get("Investigator.Research_Organization.Name"), str):
            new_part['name'] = e["Investigator.Research_Organization.Name"]
            new_part['role'] = 'coordinator'
        if new_part.get('name'):
            part_id = identify_participant(new_part['name'], cache_participant)
        if part_id:
            new_part['participant_id'] = part_id
            new_part['organizations_id'] = part_id
            new_part['identified'] = True
        else:
            new_part['identified'] = False
        address = {}
        if isinstance(e['Investigator.Research_Organization.City'], str):
            address['city'] = e['Investigator.Research_Organization.City']
        if address:
            new_part['address'] = address
        partners.append(new_part)
    return {'projects': projects, 'partners': partners}


