from enum import Enum


class StringEnum(str, Enum):
    """Base class to ensure __str__ always returns the value."""
    def __str__(self) -> str:
        return str(self.value)