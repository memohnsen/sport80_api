""" Busy backend shit """
import logging
import json
import re
from urllib.parse import urljoin
from typing import Union, Optional
import requests

from .pages_enum import EndPoint, LegacyEndPoint
from .helpers import pull_tables, convert_to_json, collate_index, event_dict_to_list


class SportEightyHTTP:
    """ Contains all the big annoying functions so the main API file is nice and neat """

    def __init__(self, domain: str, return_dict: bool = True, debug_lvl: logging = logging.WARNING):
        self.http_session = requests.Session()
        self.domain: str = domain
        self.return_dict: bool = return_dict
        logging.basicConfig(level=debug_lvl)
        self.domain_env = self.pull_domain_env()
        self.standard_headers = self.load_standard_headers()

    def load_standard_headers(self):
        """ Standard header payload for each API request """
        headers = {"X-API-TOKEN": self.domain_env['SERVICES_API_PUBLIC_KEY'],
                   "authority": self.domain_env['RANKINGS_DOMAIN_URL'],
                   "accept": "application/json",
                   "Content-Type": "application/json"}
        return headers

    def app_data(self):
        """ Fetches OpenAPI server details """
        get_page = self.http_session.get(self.domain_env['CORE_SERVICE_API_URL'])
        return get_page.json()

    def pull_domain_env(self) -> dict:
        """ On both BWL and USAW sites, there is a JS dict needed for the API calls to work """
        get_page = requests.get(urljoin(self.domain, EndPoint.INDEX_PAGE.value))
        page_data = get_page.text
        reggie = re.compile(r"window.env = ({.*?});", re.DOTALL)
        match = reggie.search(page_data)
        if match:
            try:
                py_dict = json.loads(match.group(1))
                return py_dict
            except json.JSONDecodeError:
                return {}
        return {}

    def test_token(self, token: str):
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.RANKINGS_INDEX.value)
        get_page = self.http_session.get(api_url, headers={"X-API-TOKEN": token})
        if get_page.status_code == 200:
            return True

    def test_core_api(self):
        api_url = "https://core.sport80.com/api/docs"
        get_page = self.http_session.get(api_url, headers=self.standard_headers)
        if get_page.ok:
            return get_page.json()

    def get_weight_class(self):
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.RANKINGS_DATA.value)
        get_page = self.http_session.get(api_url, headers=self.standard_headers)
        if get_page.ok:
            return get_page.json()

    def get_ranking_index(self):
        """ Working """
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.RANKINGS_INDEX.value)
        get_page = self.http_session.get(api_url, headers=self.standard_headers)
        if get_page.ok:
            return get_page.json()['cards']

    def __get_rankings_table(self, category):
        """ Simple GET call for the ranking category specified """
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.rankings_url(category))
        get_page = self.http_session.get(api_url, headers=self.standard_headers)
        return get_page.json()

    def quick_ranking_search(self):
        """ Cycles through the API endpoint available for rankings table """
        start_cat_int = 1
        start_cat = self.__get_rankings_table(start_cat_int)
        err_msg = "An error occurred"
        available_end_points = {}
        while err_msg not in start_cat['title']:
            available_end_points.update({start_cat['title']: start_cat['data_url']})
            start_cat_int += 1
            start_cat = self.__get_rankings_table(start_cat_int)
        return available_end_points

    def get_rankings_table(self, category, a_date, z_date, wt_class):
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.rankings_url(category))
        payload = {"date_range_start": a_date, "date_range_end": z_date, "weight_class": wt_class}
        get_page = self.http_session.get(api_url, headers=self.standard_headers)
        return get_page.json()

    def get_rankings(self, a_date: str, z_date: str, additional_args=None) -> list[dict]:
        """ Returns a dict containing the rankings for the given date range """
        results = []
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.ALL_RANKINGS.value + "?p=0&l=1000&sort=&d=&s=")
        payload = {"date_range_start": a_date, "date_range_end": z_date}
        if additional_args:
            payload.update(additional_args)
        get_page = self.http_session.post(api_url, headers=self.standard_headers, json=payload)
        if get_page.ok:
            front_page = get_page.json()
            collated_pages = self.__collate_results(front_page, payload)
            results = [item for sublist in collated_pages.values() for item in sublist['data']]
        return results

    def get_ranking_filters(self):
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], "/api/categories/rankings/table")
        get_page = self.http_session.get(api_url, headers=self.standard_headers)
        if get_page.ok:
            return get_page.json()

    def get_event_index(self, year: int) -> dict:
        """ Fetches the event index per year """
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.EVENT_INDEX.value)
        payload = {"date_range_start": f"{year}-01-01", "date_range_end": f"{year}-12-31"}
        print(f"Fetching events for {year} with payload:", payload)
        
        get_page = self.http_session.post(api_url, headers=self.standard_headers, json=payload)
        if not get_page.ok:
            print(f"Error fetching events: {get_page.status_code}")
            return {}
            
        response = get_page.json()
        print(f"Total events: {response.get('total', 'unknown')}")
        print(f"Items per page: {response.get('items_per_page', 'unknown')}")
        print(f"Current page: {response.get('current_page', 'unknown')}")
        
        if get_page.ok:
            page_data = self.__collate_results(response, payload)
            print(f"Number of pages collected: {len(page_data)}")
            for page_num, page in page_data.items():
                print(f"Page {page_num} has {len(page.get('data', []))} events")
            
            collated_index = collate_index(page_data)
            print(f"Total events after collation: {len(collated_index)}")
            return collated_index

    def get_event_results(self, event_dict: dict):
        """ Uses the integer that follows the event url API """
        # todo: below line needs serious refactoring
        event_id: str = event_dict['action'][0]['route'].split('/')[-1]
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.event_results_url(event_id))
        get_page = self.http_session.post(api_url, headers=self.standard_headers)
        collated_pages = self.__collate_results(get_page.json())
        combined_data = collate_index(collated_pages)
        if get_page.ok and self.return_dict:
            return combined_data
        if get_page.ok and not self.return_dict:
            return event_dict_to_list(combined_data)

    def __collate_results(self, page_one: dict, payload: Optional[dict] = None) -> dict:
        """ Cycles through the passed dict and checks for a URL """
        all_pages = {0: page_one}
        current_page = page_one
        index = 1
        
        while current_page.get('next_page_url'):
            print(f"Fetching page {index} from {current_page['next_page_url']}")
            next_page = self.__next_page(current_page['next_page_url'], payload)
            if not next_page:
                print(f"Failed to fetch page {index}")
                break
                
            all_pages[index] = next_page
            current_page = next_page
            index += 1
            
        return all_pages

    def __next_page(self, next_url: str, payload: Optional[dict] = None) -> Optional[dict]:
        """ Designed around the events dict """
        try:
            get_page = self.http_session.post(next_url, headers=self.standard_headers, json=payload)
            if get_page.ok:
                return get_page.json()
            print(f"Error fetching next page: {get_page.status_code}")
            return None
        except Exception as e:
            print(f"Exception fetching next page: {e}")
            return None

    def get_lifter_data(self, lifter_id):
        """ Historical performance of a lifter  """
        api_url = urljoin(self.domain_env['RANKINGS_DOMAIN_URL'], EndPoint.lifter_url(lifter_id))
        get_page = self.http_session.post(api_url, headers=self.standard_headers)
        if get_page.ok:
            return self.__collate_results(get_page.json())

    # LEGACY CODE THAT STILL WORKS
    def get_upcoming_events(self) -> Union[list, dict]:
        """ Returns the upcoming events list """
        logging.info("get_upcoming_events called")
        api_url = urljoin(self.domain, LegacyEndPoint.UPCOMING_EVENTS.value)
        get_page = self.http_session.get(api_url)
        upcoming_events = pull_tables(get_page)
        if self.return_dict:
            return convert_to_json(upcoming_events)
        return upcoming_events

    def get_start_list(self, event_id: str) -> Union[list, dict]:
        """ Returns a specific upcoming events start list """
        logging.info("get_start_list called")
        api_url = urljoin(self.domain, LegacyEndPoint.START_LIST.value + event_id)
        get_page = self.http_session.get(api_url)
        start_list = pull_tables(get_page)
        if self.return_dict:
            return convert_to_json(start_list)
        return start_list
