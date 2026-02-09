"""

to do:

add steps to

split enriched into two csvs -- 1 with linkedin enriched, 1 without -- will need separate prompts for without.
"""

from dataclasses import dataclass
import requests
import time
import pandas as pd
from datetime import datetime
import os
import time
import requests
from requests.exceptions import RequestException
from typing import List

class BrightData:

    def __init__(self, api_token: str, dataset_id: str):
        
        self.api_token: str= api_token
        
        self.dataset_id: str= dataset_id
        self.base_url: str= "https://api.brightdata.com/datasets/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
        self.ts_start = datetime.now()
        # self.config: Config= config
    



    def chunk_list(self, input_list, chunk_size):
        """Helper function to chunk a list into smaller parts."""
        for i in range(0, len(input_list), chunk_size):
            yield input_list[i:i + chunk_size]

    def trigger_data_request_people(self, data):
        """Takes in list of linkedin profiles; returns list of snapshot ids"""
        snapshot_ids = []

        for batches in self.chunk_list(data, 50):

            """Trigger a data request for people profiles."""
            batch = [{"url": url} for url in batches]
            print(len(batch))
            endpoint = f"{self.base_url}/trigger?dataset_id={self.dataset_id}&include_errors=true"
            response = requests.post(endpoint, headers=self.headers, json=batch)
            response.raise_for_status()  # Raise an exception if the request failed
            snapshot_id = response.json().get("snapshot_id")
            snapshot_ids.append(snapshot_id)

        return snapshot_ids

    def trigger_data_request_posts_discovery(self, data, limit=2):
        """Takes in list of linkedin profile urls; returns list of snapshot ids"""
        snapshot_ids = []

        for batches in self.chunk_list(data, 50):
            batch = [{"url": url, "limit": limit} for url in batches]
            endpoint = f"{self.base_url}/trigger?dataset_id={self.dataset_id}&include_errors=true&type=discover_new&discover_by=url"
            response = requests.post(endpoint, headers=self.headers, json=batch)
            response.raise_for_status()  # Raise an exception if the request failed
            snapshot_id = response.json().get("snapshot_id")
            snapshot_ids.append(snapshot_id)

        return snapshot_ids



    def trigger_data_request_posts_direct(self, posts):
        """takes in list of linkedin post urls; returns list of snapshot ids."""
        snapshot_ids = []

        for batches in self.chunk_list(posts, 50):
            batch = [{"url": url} for url in batches]
            endpoint = f"{self.base_url}/trigger?dataset_id={self.dataset_id}&include_errors=true"
            response = requests.post(endpoint, headers=self.headers, json=batch)
            response.raise_for_status()  # Raise an exception if the request failed
            snapshot_id = response.json().get("snapshot_id")
            snapshot_ids.append(snapshot_id)

        return snapshot_ids

    # def trigger_data_request_company_profile(self, company_urls):
    #     """takes in list of company linkedin profile urls; returns list of snapshot ids"""
    #     snapshot_ids = []
    #     for batches in self.chunk_list(company_urls, 50):
    #         pending_snapshots = self.get_snapshots(status='running')
    #         if len(pending_snapshots) >= 99:
    #             print('reached 100 requests, sleeping for 1 min before continuing')
    #             time.sleep(120)
    #             print('done sleeping, bitch. continuing.')
    #         batch = [{"url": url} for url in batches]
    #         endpoint = f"{self.base_url}/trigger?dataset_id={self.dataset_id}&include_errors=true"
    #         response = requests.post(endpoint, headers=self.headers, json=batch)
    #         response.raise_for_status()  # Raise an exception if the request failed
    #         snapshot_id = response.json().get("snapshot_id")
    #         snapshot_ids.append(snapshot_id)
    #
    #     return snapshot_ids

    def trigger_data_request_company_profile(self, company_urls: List[str]) -> List[str]:
        """
        Takes in list of company LinkedIn profile URLs; returns list of snapshot IDs.
        Includes retry logic with exponential backoff for API calls.
        """
        snapshot_ids = []
        max_retries = 3
        base_delay = 2  # Starting delay in seconds


        def make_request_with_retry(batch):
            url = f"{self.base_url}/trigger"
            params = {
                "dataset_id": self.dataset_id,
                "include_errors": "true"
            }
            for attempt in range(max_retries):
                try:
                    response = requests.post(url, headers=self.headers, params=params, json=batch)
                    response.raise_for_status()
                    return response.json().get("snapshot_id")
                except RequestException as e:
                    if attempt == max_retries - 1:  # Last attempt
                        raise Exception(f"Failed after {max_retries} attempts: {str(e)}")

                    # Calculate delay with exponential backoff: 2s, 4s, 8s
                    delay = base_delay * (2 ** attempt)
                    print(f"Request failed, retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)

        for batches in self.chunk_list(company_urls, 50):
            pending_snapshots = self.get_snapshots(status='running')
            if len(pending_snapshots) >= 99:
                print('reached 100 requests, sleeping for 2 mins before continuing')
                time.sleep(120)
                print('resuming requests')

            batch = [{"url": url} for url in batches]
            try:
                snapshot_id = make_request_with_retry(batch)
                snapshot_ids.append(snapshot_id)
                # Add a small delay between batches to avoid rate limiting
                time.sleep(0.5)
            except Exception as e:
                print(f"Failed to process batch: {str(e)}")
                # Optionally, you could raise the exception here if you want to halt processing
                # raise

            print(f'requested {len(snapshot_ids)*50} profiles of {len(company_urls)}')

        return snapshot_ids

    def check_snapshot_status(self, snapshot_id):
        """takes in a single snapshot id as a string and returns the progress. use to determine when to fetch_data"""
        endpoint = f"{self.base_url}/progress/{snapshot_id}"
        response = requests.get(endpoint, headers=self.headers)

        return response.json()

    def get_snapshots(self, status='running'):
        """count the number of snapshots running in bd instance. pass 'ready' or 'failed' to get those"""

        endpoint = f"https://api.brightdata.com/datasets/v3/snapshots/?status={status}"
        response = requests.get(endpoint, headers=self.headers)

        return response.json()

    def fetch_data(self, snapshot_ids):
        """takes in list of snapshot_ids; returns list of responses"""
        out = []
        for i,snapshot_id in enumerate(snapshot_ids):
            print(f'processing {i} of {len(snapshot_ids)} sids')
            url = f"{self.base_url}/snapshot/{snapshot_id}?format=json"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                out.extend(response.json())
            else:
                out.extend(f"Error fetching data: {response.status_code} - {response.text}")
        return out

    def format_direct_posts_res(self, posts_direct):
        """takes in list of dicts from fetch_data() response and outputs a df"""
        return pd.DataFrame(posts_direct)

    def process_company_posts(self, df_in):
        """takes in company profiles dataframe and pulls out the company posts into a text column"""
        df = df_in.copy()

        import ast

        def safe_eval(entry):
            try:
                return ast.literal_eval(entry)
            except Exception as e:
                print(f"Error parsing: {entry[:100]}... - {e}")
                return []

        df['updates'] = df['updates'].apply(safe_eval)

        # Extract the 'text' from the first dictionary in the list
        df['text'] = df['updates'].apply(
            lambda x: x[0]['text'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) and 'text' in x[
                0] else None
        )

        return df

    def wait_on_snapshots(self, snapshot_ids):
        """takes in list of snaphsot_ids and waits on them to be status = 'ready' in brightdata api"""
        statuses = []
        i = 0
        for s in snapshot_ids:

            status = self.check_snapshot_status(s)

            while status['status'] not in ['ready', 'failed']:
                print(f"Snapshot {s} is not ready yet. Status: {status['status']}. Waiting 30 seconds. Total time elapsed: {i*30/60} minutes")
                i += 1
                time.sleep(30)
                status = self.check_snapshot_status(s)

            # print(f"Snapshot {s} is ready")
            statuses.append(status)
        count_complete = len([s for s in statuses if s['status'] == 'ready'])
        count_failed = len([s for s in statuses if s['status'] == 'failed'])

        print(f"All snapshots are done. {count_complete} completed, {count_failed} failed.")

        return statuses

    @staticmethod
    def format_companies_profile_output(fetch_list):
        """takes in list of dicts from fetch response and formats/renames columns to match w/ prospect list"""
        out = pd.DataFrame(fetch_list)
        out.rename(columns={"about": "LI about", "description":"LI description", 
                           "specialties":"LI specialties", "industries":"LI industries", "updates": "LI updates",
                            "followers":"LI followers"}, inplace=True)
        out['url'] = out['url'].str.replace('https://', 'http://')
        full_df = out.copy()
        full_df = full_df.fillna(0).infer_objects(copy=False)
        trimmed_df = out.loc[:, ['url', 'LI about', 'LI specialties', 'LI description', 'LI industries',  'LI updates', 'LI followers', 'employees']]
        trimmed_df = trimmed_df.fillna(0).infer_objects(copy=False)
        return trimmed_df, full_df

    @staticmethod
    def add_enriched_company_data_to_prospect_list(prospect_list, company_data):
        """takes in df of prospect list and df of li company data from bright data and merges"""
        prospect_list['Company Linkedin Url'] = prospect_list['Company Linkedin Url'].fillna('None')

        merged_df = pd.merge(
            prospect_list,
            company_data,
            left_on='Company Linkedin Url',
            right_on='url',
            how='left'
        )

        condition = merged_df['Company Linkedin Url'] == 'None'

        # Fill NaN or None values in these rows with 0
        merged_df.loc[condition] = merged_df.loc[condition].fillna(0).infer_objects(copy=True)

        return merged_df
    
# Example usage
if __name__ == "__main__":

    api_token = "359fda78-3988-4c81-828a-5ab4cd5e057d"
    dataset_id_people = "gd_l1viktl72bvl7bjuj0"
    dataset_id_posts_discovery = "gd_lyy3tktm25m4avu764"
    dataset_id_posts_direct = "gd_lyy3tktm25m4avu764"
    dataset_id_company_profile = "gd_l1vikfnt1wgvvqz95w"
    bright_data_people = BrightData(api_token, dataset_id_people)
    bright_data_posts_discovery = BrightData(api_token, dataset_id_posts_discovery)
    bright_data_posts_direct = BrightData(api_token, dataset_id_posts_direct)
    bright_data_companies = BrightData(api_token, dataset_id_company_profile)




