from sqlalchemy import cast
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.dialects.postgresql import ARRAY


class Base(DeclarativeBase):
    pass

class JSONBArray(ARRAY):
    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)