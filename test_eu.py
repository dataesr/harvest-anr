import json
import requests

eu_url = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
eu_headers = {"Accept": "application/json, text/plain, */*"}
eu_query = {
    "bool": {
        "must": [
            {"terms": {"DATASOURCE": ["SEDIA_NONH2020_PROD"]}},
            {"terms": {"status": ["Ongoing", "Ended"]}},
            {"terms": {"language": ["en"]}},
        ],
        "must_not": [
            {"terms": {"programId": ["43108390", "31045243"]}},  # H2020, Horizon
        ],
    }
}
eu_sort = {"order": "DESC", "field": "es_SortDate"}
eu_fields = [
    "title",
    "acronym",
    "objective",
    "projectId",
    "programId",
    "callIdentifier",
    "programAbbreviation",
    "programmes",
    "status",
    "participants",
    "numberOfContributors",
    "topicAbbreviation",
    "topicDescription",
    "overallBudget",
    "euContributionRate",
    "euContributionAmount",
    "freeKeywords",
    "startDate",
    "endDate",
    "ecSignatureDate",
    "typeOfAction",
    "typeOfActions",
]
eu_languages = ["en"]
eu_files = {
    "query": ("blob", json.dumps(eu_query), "application/json"),
    "sort": ("blob", json.dumps(eu_sort), "application/json"),
    "displayFields": ("blob", json.dumps(eu_fields), "application/json"),
    "languages": ("blob", json.dumps(eu_languages), "application/json"),
}
eu_params = {"apiKey": "SEDIA_NONH2020_PROD", "text": "***", "pageSize": 1, "pageNumber": 1}


def fetch_one_page(page_number: int, page_size: int) -> list:
    params = {**eu_params, "pageNumber": page_number, "pageSize": page_size}
    response = requests.post(
        url=eu_url,
        headers=eu_headers,
        files=eu_files,
        params=params,
    )
    data = response.json()
    return data


def fetch_all(stop_page: int, start_page: int = 1, page_size: int = 50):
    next_page = start_page
    results = []
    print("Start fetching EU API...")

    while next_page > 0:
        print(f"Fetching page {next_page}...")
        data = fetch_one_page(next_page, page_size)
        results.extend(data.get("results", []))

        next_page = next_page + 1 if data["totalResults"] > (page_size * next_page) else 0

        if next_page == stop_page:
            next_page = 0

    print(f"Successfully fetched {len(results)} projects")
    return results


def extract_participants(project_id, raw_text: str) -> list:
    participants = []

    if not len(raw_text):
        print(f"[{project_id}] Json participants raw text empty")
        return participants

    try:
        data = json.loads(raw_text)
    except Exception as error:
        print(f"[{project_id}] Error while parsing json particiants: {error}")
        return participants

    for index, d in enumerate(data):
        participant = {}
        participant["role"] = d["role"]
        participant["funding"] = d["eucontribution"]
        participant["id"] = f"{project_id}-{index+1:02d}"
        participant["label"] = {"default": d["legalName"]}

        address = {}
        postal_address = d.get("postalAddress", {})
        country = postal_address.get("countryCode", {})
        if postal_address.get("city"):
            address["city"] = postal_address["city"]
        if country.get("abbreviation"):
            address["country_code"] = country["abbreviation"]
        if country.get("description"):
            address["country"] = country["description"]
        if address:
            participant["address"] = address

        participants.append(participant)

    return participants


def extract_projects(data: list):
    projects = []

    if not len(data):
        print("No data to extract")
        return projects

    print(f"Start extracting {len(data)} EU projects")

    for d in data:
        project = {}
        reference = d["reference"]

        if "metadata" not in d:
            print(f"No metadata for project {reference=}")

        metadata = d["metadata"]
        project_id = metadata["projectId"][0]

        project["id"] = project_id
        project["url"] = metadata["url"][0]
        project["type"] = metadata["programAbbreviation"][0]  # TODO mapping ?

        project["startDate"] = metadata["startDate"][0]
        project["endDate"] = metadata["endDate"][0]
        project["signatureDate"] = metadata["ecSignatureDate"][0]
        project["year"] = project["startDate"][0:4]

        project["acronym"] = {"default": metadata["acronym"][0]}
        project["label"] = {"default": metadata["title"][0]}
        if len(metadata["objective"]):
            project["description"] = {"default": metadata["objective"][0]}

        project["participantCount"] = metadata["numberOfContributors"][0]
        project["participants"] = extract_participants(project_id, metadata["participants"][0])

        if len(metadata["overallBudget"]):
            project["budgetTotal"] = metadata["overallBudget"][0]
        if len(metadata["euContributionAmount"]):
            project["budgetFinanced"] = metadata["euContributionAmount"][0]

        project["callIdentifier"] = {"id": metadata["callIdentifier"][0]}

        action_code = metadata["typeOfActions"][0]
        action_label = metadata["typeOfAction"][0]
        project["instrument"] = action_label
        project["action"] = {
            "code": action_code,
            "label": {"default": action_label},
            # "level": 1
        }

        if len(metadata.get("freeKeywords", [])):
            project["keywords"] = {"en": metadata["freeKeywords"]}

        # TODO:
        # priorities ?
        projects.append(project)

    print(f"{len(projects)} EU projects extracted")
    return projects


def harvest_eu_projects(start_page: int = 1):
    results = fetch_all(start_page=400, stop_page=1000)
    projects = extract_projects(results)

    print("project_sample:")
    print(f"{projects[0]}")

    return projects


def update_eu():
    new_data_eu = harvest_eu_projects()
    # to_jsonl(new_data_eu, "projects.jsonl")


if __name__ == "__main__":
    harvest_eu_projects()
