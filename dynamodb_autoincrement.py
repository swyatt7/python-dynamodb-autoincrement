from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Mapping, Sequence, Union
from decimal import Decimal

from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource


PrimitiveDynamoDBValues = Optional[Union[str, int, float, Decimal, bool]]
DynamoDBValues = Union[
    PrimitiveDynamoDBValues,
    Mapping[str, PrimitiveDynamoDBValues],
    Sequence[PrimitiveDynamoDBValues],
]
DynamoDBItem = Mapping[str, DynamoDBValues]


@dataclass(frozen=True)
class BaseDynamoDBAutoIncrement(ABC):
    dynamodb: DynamoDBServiceResource
    counter_table_name: str
    counter_table_key: DynamoDBItem
    attribute_name: str
    table_name: str
    initial_value: int
    dangerously: bool = False

    @abstractmethod
    def next(self, item: DynamoDBItem) -> tuple[Iterable[dict[str, Any]], str]:
        raise NotImplementedError

    def _put_item(self, *, TableName, **kwargs):
        # FIXME: DynamoDB resource does not have put_item method; emulate it
        self.dynamodb.Table(TableName).put_item(**kwargs)

    def _get_item(self, *, TableName, **kwargs):
        # FIXME: DynamoDB resource does not have get_item method; emulate it
        return self.dynamodb.Table(TableName).get_item(**kwargs)

    def _query(self, *, TableName, **kwargs):
        return self.dynamodb.Table(TableName).query(**kwargs)

    def put(self, item: DynamoDBItem):
        TransactionCanceledException = (
            self.dynamodb.meta.client.exceptions.TransactionCanceledException
        )
        while True:
            puts, next_counter = self.next(item)
            if self.dangerously:
                for put in puts:
                    self._put_item(**put)
            else:
                try:
                    # FIXME: depends on unreleased code in boto3.
                    # See https://github.com/boto/boto3/pull/4010
                    self.dynamodb.transact_write_items(  # type: ignore[attr-defined]
                        TransactItems=[{"Put": put} for put in puts]
                    )
                except TransactionCanceledException:
                    continue
            return next_counter


class DynamoDBAutoIncrement(BaseDynamoDBAutoIncrement):
    def next(self, item):
        counter = (
            self._get_item(
                AttributesToGet=[self.attribute_name],
                Key=self.counter_table_key,
                TableName=self.counter_table_name,
            )
            .get("Item", {})
            .get(self.attribute_name)
        )

        if counter is None:
            next_counter = self.initial_value
            put_kwargs = {"ConditionExpression": "attribute_not_exists(#counter)"}
        else:
            next_counter = counter + 1
            put_kwargs = {
                "ConditionExpression": "#counter = :counter",
                "ExpressionAttributeValues": {
                    ":counter": counter,
                },
            }

        puts = [
            {
                **put_kwargs,
                "ExpressionAttributeNames": {
                    "#counter": self.attribute_name,
                },
                "Item": {
                    **self.counter_table_key,
                    self.attribute_name: next_counter,
                },
                "TableName": self.counter_table_name,
            },
            {
                "ConditionExpression": "attribute_not_exists(#counter)",
                "ExpressionAttributeNames": {
                    "#counter": self.attribute_name,
                },
                "Item": {self.attribute_name: next_counter, **item},
                "TableName": self.table_name,
            },
        ]

        return puts, next_counter


class DynamoDBHistoryAutoIncrement(BaseDynamoDBAutoIncrement):
    def list(self) -> list[int]:
        result = self._query(
            TableName=self.table_name,
            ExpressionAttributeNames={
                **{f"#{i}": key for i, key in enumerate(self.counter_table_key.keys())},
                "#counter": self.attribute_name,
            },
            ExpressionAttributeValues={
                f":{i}": value
                for i, value in enumerate(self.counter_table_key.values())
            },
            KeyConditionExpression=" AND ".join(
                f"#{i} = :{i}" for i in range(len(self.counter_table_key.keys()))
            ),
            ProjectionExpression="#counter",
        )
        return sorted(item[self.attribute_name] for item in result["Items"])

    def get(self, version: Optional[int] = None) -> DynamoDBItem:
        if version is None:
            kwargs = {
                "TableName": self.counter_table_name,
                "Key": self.counter_table_key,
            }
        else:
            kwargs = {
                "TableName": self.table_name,
                "Key": {**self.counter_table_key, self.attribute_name: version},
            }
        return self._get_item(**kwargs).get("Item")

    def next(self, item):
        existing_item = self._get_item(
            TableName=self.counter_table_name,
            Key=self.counter_table_key,
        ).get("Item")

        counter = (
            None if existing_item is None else existing_item.get(self.attribute_name)
        )

        if counter is None:
            next_counter = self.initial_value
            put_kwargs = {"ConditionExpression": "attribute_not_exists(#counter)"}
        else:
            next_counter = counter + 1
            put_kwargs = {
                "ConditionExpression": "#counter = :counter",
                "ExpressionAttributeValues": {
                    ":counter": counter,
                },
            }

        if existing_item is not None and counter is None:
            existing_item[self.attribute_name] = next_counter
            next_counter += 1

        puts = [
            {
                **put_kwargs,
                "ExpressionAttributeNames": {
                    "#counter": self.attribute_name,
                },
                "Item": {
                    **item,
                    **self.counter_table_key,
                    self.attribute_name: next_counter,
                },
                "TableName": self.counter_table_name,
            },
        ]

        if existing_item is not None:
            puts.append(
                {
                    "ConditionExpression": "attribute_not_exists(#counter)",
                    "ExpressionAttributeNames": {
                        "#counter": self.attribute_name,
                    },
                    "Item": existing_item,
                    "TableName": self.table_name,
                }
            )

        return puts, next_counter
