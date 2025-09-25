import json
import pandas as pd

from project.server.main.utils import normalize, build_correspondance_structures
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def build_participant_map(all_struct):
    participant_map = {}
    corresp = build_correspondance_structures(all_struct)
    print(f'correspondance between ids built with {len(corresp)} entries')
    # data manually coded
    de = pd.read_excel('partenaires_non_identifies.xlsx')
    print(f'{len(de)} manual data loaded')
    for e in de.to_dict(orient='records'):
        if e['code'] != e['code']:
            continue
        if len(str(e['code']))<3:
            continue
        part_name = normalize(e['Nom'])
        e['code'] = str(e['code']).replace(',', ';')
        for code in e['code'].split(';'):
            if len(code)<3:
                continue
            part_id = code.strip()
            if part_id not in corresp:
                #print(part_id)
                continue
            part_id = corresp[part_id]
            if part_name not in participant_map:
                participant_map[part_name] = {}
            if part_id not in participant_map[part_name]:
                participant_map[part_name][part_id] = 0
            participant_map[part_name][part_id] += 1
    print('done')
    # then old data
    df_old = pd.read_json('https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/scanR/projects.json')
    print(f'{len(df_old)} old data loaded')
    for e in df_old.to_dict(orient='records'):
        project_id = e['id']
        for part in e.get('participants'):
            if part.get('structure'):
                part_name = part.get('label', {}).get('default')
                if part_name:
                    part_name = normalize(part_name.split('__-__')[0])
                    part_id = part['structure']
                    if part_id not in corresp:
                        continue
                    part_id = corresp[part_id]
                    if part_name not in participant_map:
                        participant_map[part_name] = {}
                    if part_id not in participant_map[part_name]:
                        participant_map[part_name][part_id] = 0
                    participant_map[part_name][part_id] += 1
    cache_participant = {}
    for p in participant_map:
        x = participant_map[p]
        cache_participant[p] = sorted(x, key=x.get, reverse=True)[0]
    json.dump(cache_participant, open('cache_participant.json', 'w'))
    return cache_participant

