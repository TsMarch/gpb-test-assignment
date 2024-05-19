from pydantic import BaseModel, field_validator, ValidationError
import datetime
import pandas as pd
import itertools


class CheckJson(BaseModel):
    Columns: list
    Description: list
    RowCount: int
    Rows: list[list]

    @field_validator('Columns')
    def elements_in_columns(cls, columns):
        if len(columns) < 3:
            raise ValueError('List must contain at least three elements')

        if not isinstance(columns[0], int):
            raise ValueError('Первый элемент должен быть типа int')

        if not isinstance(columns[1], datetime.datetime):
            raise ValueError('Второй элемент должен быть типа datetime')

        if not isinstance(columns[2], str):
            raise ValueError('Третий элемент должен быть типа str')

        return columns
