from dataclasses import dataclass

BASE_URL = ('https://www.bayut.com/property-market-analysis/sale/labour-camps/'
            '')
OTHER_URL_PARAMS = '?time_since_creation=36m&page='
TABLE_CLASS = '_3336fc4e _14fa9064 _4d57381b' # '_3336fc4e _1ec30276 _14fa9064 _4d57381b'
TABLE_COLUMNS_NUM = 5
MAX_PAGES = 1000
TIME_SLEEP = 5
OUTPUT_DIR = 'scraped_data'
FILE_NAME = 'UAE_labour-camps'
FILE_FORMAT = 'parquet'


@dataclass
class DataColumnsNames:
    date: str = 'date'
    location: str = 'location'
    price_aed: str = 'price_aed'
    # beds: str = 'beds'
    built_up: str = 'built_up'
    # plot: str = 'plot'
    unit: str = 'unit'
    time_updated: str = 'time_updated'


@dataclass
class ScraperConfig:
    base_url: str = BASE_URL
    other_url_params: str = OTHER_URL_PARAMS
    table_class: str = TABLE_CLASS
    max_pages: int = MAX_PAGES
    time_sleep_s: int = TIME_SLEEP
    table_columns_num: int = TABLE_COLUMNS_NUM


@dataclass
class FileConfig:
    output_dir: str = OUTPUT_DIR
    file_name: str = FILE_NAME
    file_format: str = FILE_FORMAT


data_columns = DataColumnsNames()
scraper_config = ScraperConfig()
file_config = FileConfig()
