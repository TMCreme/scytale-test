"""
Main module for data processing
"""
import os
import time
import json
import requests

from transform import transformation_with_spark


def get_all_repositories(org_name: str, access_token: str):
    """Retrieve all repositories given the organization name"""
    url = f"https://api.github.com/orgs/{org_name}/repos"
    headers = {"Authorization": f"Bearer {access_token}"}
    repositories = []

    while url:
        response = requests.get(url, headers=headers)
        repositories.extend(response.json())

        # Check for rate limit exceeded
        if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
            remaining_limit = int(response.headers['X-RateLimit-Remaining'])
            if remaining_limit == 0:
                reset_time = int(response.headers['X-RateLimit-Reset'])
                wait_time = reset_time - int(time.time()) + 5
                print(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                time.sleep(wait_time)
                continue

        # Check for pagination
        links = response.headers.get('Link')
        if links:
            next_url = get_next_page(links)
            url = next_url
        else:
            url = None

    return repositories


def get_next_page(links):
    """Extract the next page URL from the Link header"""
    link_items = links.split(',')
    for link_item in link_items:
        link, rel = link_item.split(';')
        if 'rel="next"' in rel:
            return link.strip('<>')


def get_all_pull_requests(repo_full_name: str, access_token: str):
    """Retrieve all pull requests given the repository's name"""
    url = f"https://api.github.com/repos/{repo_full_name}/pulls?state=all"
    headers = {"Authorization": f"Bearer {access_token}"}
    pull_requests = []

    while url:
        response = requests.get(url, headers=headers)
        pull_requests.extend(response.json())

        # Check for rate limit exceeded
        if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
            remaining_limit = int(response.headers['X-RateLimit-Remaining'])
            if remaining_limit == 0:
                reset_time = int(response.headers['X-RateLimit-Reset'])
                wait_time = reset_time - int(time.time()) + 5
                print(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                time.sleep(wait_time)
                continue

        # Check for pagination
        links = response.headers.get('Link')
        if links:
            next_url = get_next_page(links)
            url = next_url
        else:
            url = None

    return pull_requests


def save_data_to_json(data, filename):
    """Checks to see a given prefix directory exists or create and save data"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)


def main(organization: str, github_token: str):
    """The main entry point to the extracting"""

    repositories = get_all_repositories(organization, github_token)

    if not os.path.exists(organization):
        os.makedirs(organization)
        save_data_to_json(
            repositories, f"{organization}/repositories/data.json"
            )

    for repo in repositories:
        repo_full_name = repo['full_name']
        pull_requests = get_all_pull_requests(
            repo_full_name, github_access_token
            )

        # Save pull requests data to a JSON file for each repository
        filename = f"{repo_full_name.split('/')[0]}/prs/{repo_full_name.split('/')[1]}_pull_requests.json"
        save_data_to_json(pull_requests, filename)


if __name__ == "__main__":
    organization_name = os.environ.get("ORGANIZATION", "Scytale-exercise")
    github_access_token = os.environ.get("GITHUB_ACCESS_TOKEN")
    main(organization_name, github_access_token)
    transformation_with_spark(organization_name, "result")
