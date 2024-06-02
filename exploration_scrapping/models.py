from datetime import datetime

import pandera.polars as pa
from bs4 import BeautifulSoup
from polars import Datetime
from pydantic import BaseModel, Field
from typing import Optional, Generic, TypeVar

T = TypeVar("T", bound="CommonDataFrame")


class RealtyData(BaseModel):
    """
    A class used to represent a Realty Data row.
    """

    date: Optional[str] = Field(default=None, alias='date')
    location: Optional[str] = Field(default=None, alias='location')
    price_aed: Optional[str] = Field(default=None, alias='price_aed')
    # beds: Optional[str] = Field(default="N/A", alias='beds')
    built_up: Optional[str] = Field(default="N/A", alias='built_up')
    # plot: Optional[str] = Field(default="N/A", alias='plot')
    unit: Optional[str] = Field(default="N/A", alias='unit')

    @classmethod
    def from_row(cls, row: BeautifulSoup, col_num: int) -> "RealtyData":
        """
        Class method to create a RealtyData instance from a BeautifulSoup row.

        Args: row (BeautifulSoup): A BeautifulSoup object representing a table row.
        Returns: RealtyData: An instance of the RealtyData class.
        """
        cols = row.find_all('td')
        if len(cols) >= col_num:
            price_div = cols[2].find("span", attrs={"aria-label": "Price"})
            price_text = price_div.text.strip().replace("\xa0", " ") if price_div else "-"
            return cls(
                date=" ".join(cols[0].stripped_strings),
                location=" ".join(cols[1].stripped_strings),
                price_aed=price_text,
                #beds=cols[4].text.strip().replace("\xa0", " "),
                built_up=cols[4].text.strip().replace("\xa0", " "),
                #plot=cols[6].text.strip().replace("\xa0", " "),
                unit=cols[5].text.strip().replace("\xa0", " "),
            )
        raise ValueError("Not enough data in row")


class CommonDataFrame(pa.DataFrameModel):
    """Base class for dataframe models."""
    Date: str = pa.Field(nullable=False)


class Model(CommonDataFrame, Generic[T]):
    """Generic data model class."""


class InSchema(pa.DataFrameModel):
    """
    A SchemaModel for validating in DataFrame.
    """

    location: str = pa.Field(nullable=False)
    price_aed: str = pa.Field(nullable=False)
    # beds: str = pa.Field(nullable=True)
    built_up: str = pa.Field(nullable=True)
    # plot: str = pa.Field(nullable=True)
    unit: str = pa.Field(nullable=True)

    class Config:
        coerce = True


class OutSchema(InSchema):
    """
    A SchemaModel for validating out DataFrame.
    """

    time_updated: Datetime(time_unit='ns', time_zone=None) = pa.Field(nullable=True, default=datetime.now())
