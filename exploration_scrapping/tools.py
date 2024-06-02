import logging
import os
from datetime import datetime

import polars as pl
from time import sleep

from pydantic import ValidationError
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm

from models import RealtyData, OutSchema, InSchema
from pandera.typing import DataFrame

from config import ScraperConfig, FileConfig, data_columns


class RealtyScraper:
    """
    A class to scrape realty data from a specified URL.
    """

    def __init__(self, config: ScraperConfig):
        """
        Initializes.

        Args:
            config: ScraperConfig:
                    base_url (str): The base URL of the website to scrape.
                    other_url_params (str): Additional URL parameters for pagination.
                    table_class (str): The CSS class of the table containing the data.
                    max_pages (int): The maximum number of pages to scrape.
                    time_sleep_s (int): The time to sleep between page requests.
        """
        self.base_url = config.base_url
        self.params = config.other_url_params
        self.table_class = config.table_class
        self.max_pages = config.max_pages
        self.time_sleep_s = config.time_sleep_s
        self.table_columns_num = config.table_columns_num
        self.all_data: list = []
        self.driver = None  # instance initialized later

    def initialize_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    def scrape(self) -> pl.DataFrame:
        """
        Scrapes the data from the specified URL and returns it as a pandas DataFrame.

        Returns: pd.DataFrame: A DataFrame containing the scraped data.
        """
        self.initialize_driver()
        consecutive_no_table = 0
        logging.info('Start scraping')
        try:
            for page in tqdm(range(1, self.max_pages + 1), desc="Processing pages"):
                if consecutive_no_table >= 3:
                    logging.info("No table found for 3 consecutive pages. Stopping the scraper.")
                    break

                if page == 1:
                    url = f"{self.base_url}{self.params[: -6]}"
                else:
                    url = f"{self.base_url}{self.params}{page}"
                if self.driver:
                    self.driver.get(url)
                    sleep(self.time_sleep_s)

                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                    table = soup.find('table', {'class': self.table_class})

                    if table:
                        consecutive_no_table = 0
                        rows = table.find_all('tr')[1:]
                        for row in rows:
                            if len(row.find_all('th')) > 0:
                                continue
                            try:
                                data = RealtyData.from_row(row=row, col_num=self.table_columns_num)
                                self.all_data.append(data)
                            except (ValidationError, ValueError) as e:
                                logging.error(f"Error processing row: {e}")
                                logging.error(f"Row content: {row}")
                                if len(row.find_all('td')) > 2:
                                    logging.info(f"Content: {row.find_all('td')[2]}")
                    else:
                        consecutive_no_table += 1
                        logging.warning(f"No table found on page {page}")

        except RequestException as e:
            logging.error(f"An error occurred during scraping: {e}")
        finally:
            self.close_driver()

        return pl.DataFrame([data.dict(by_alias=True) for data in self.all_data])


class DataFramePreprocessor:
    """A class to preprocess and validate the DataFrame."""

    @staticmethod
    def preprocess(data: DataFrame[InSchema]) -> DataFrame[InSchema]:
        """
        Adds a new column with the current timestamp to the DataFrame.

        Args: data DataFrame[InSchema]: The DataFrame to enhance.
        Returns: DataFrame[OutSchema]: The enhanced DataFrame with the new column.
        """
        data = data.with_columns(pl.lit(datetime.now()).alias(data_columns.time_updated))
        return data


class FileManager:
    """
    A class to manage input/output operations for DataFrame.
    """

    def __init__(self, config: FileConfig):
        """
        Initializes the IOManager with the specified output directory.

        Args:output_dir (str): The directory where the files will be saved.
        """
        self.output_dir = config.output_dir
        self.file_name = config.file_name
        self.file_format = config.file_format
        if not os.path.exists(config.output_dir):
            os.makedirs(config.output_dir)

    def save_dataframe(self, data: DataFrame[OutSchema]) -> None:
        """
        Saves the DataFrame to a file in the specified format.

        Args:
            data (pd.DataFrame): The DataFrame to save.
            filename (str): The name of the file (without extension).
            file_format (str): The format of the file (default is "parquet").

        Raises:
            ValueError: If the file format is not supported.
        """
        file_path = os.path.join(self.output_dir, f"{self.file_name}.{self.file_format}")

        if self.file_format == 'parquet':
            data.write_parquet(file=file_path, use_pyarrow=False)
        elif self.file_format == 'csv':
            data.write_csv(file=file_path, separator=';')
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        logging.info(f"DataFrame saved to {file_path}")
