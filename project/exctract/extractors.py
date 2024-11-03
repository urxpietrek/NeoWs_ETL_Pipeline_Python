from abc import ABC, abstractmethod
from typing import Union
from datetime import datetime

from project.utils import (
    create_filename,
    save_to_json,
    check_and_set_date_format
)

import requests
import re

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

class SessionFactory:
    def __init__(self, proxy: dict = None, headers: dict = None):
        self._proxy = proxy
        self._headers = headers
        
    def get_session(self):
        new_session = requests.Session()
        
        if self._proxy:
            new_session.proxies.update(self._proxy)
        
        if self._headers:
            new_session.headers.update(self._headers)
        
        return new_session

class NeoWsExtractor(Extractor):
    url = 'https://api.nasa.gov/neo/rest/v1/feed?'
    
    def __init__(self, apikey:str,
                 proxy: dict = None,
                 headers: dict = None):
        
        self.initialize_apikey(apikey)
        session_configs = SessionFactory(proxy, headers)
        self.session = session_configs.get_session()
    
    def initialize_apikey(self, apikey: str) -> None:
        self._api_key = apikey
    
    def extract(self,
                start_date: Union[datetime, str],
                end_date: Union[datetime, str] | None = None):
        
        if self._api_key is None:
            raise ValueError('Set API_KEY to have access to NASA datasets.')
        
        start_date = check_and_set_date_format(start_date)
        end_date = check_and_set_date_format(end_date) if end_date else None
        
        params = dict(
            api_key=self._api_key,
            start_date=start_date,
            end_date=end_date
        )
        
        result = self.session.get(self.url, params=params)
        
        try:
            result.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return str(e)
        
        data = result.json()
        
        filename = create_filename(start_date, end_date)
        isSaved, info = save_to_json(filename, data)
        
        if isSaved:
            return f'Extracted. You will find data in {info}'
        
        return f'Not able to save data to json. Error: {info}'
        

if __name__ == '__main__':
    print(create_filename('2024-10-27'))