import os
from project.server.main.eu import harvest_eu_projects
from project.server.main.utils import to_jsonl

if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(current_directory, "eu_projects.jsonl")
    projects = harvest_eu_projects({"test": "1234"})
    if os.path.exists(output_file):
        os.remove(output_file)
    to_jsonl(projects, output_file)
