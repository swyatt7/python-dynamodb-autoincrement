from decimal import Decimal
from typing import Mapping, Optional, Sequence, Union

PrimitiveDynamoDBValues = Optional[Union[str, int, float, Decimal, bool]]
DynamoDBValues = Union[
    PrimitiveDynamoDBValues,
    Mapping[str, PrimitiveDynamoDBValues],
    Sequence[PrimitiveDynamoDBValues],
]
DynamoDBItem = Mapping[str, DynamoDBValues]
