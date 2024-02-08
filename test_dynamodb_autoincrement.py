# Copyright © 2023 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.

from botocore.exceptions import ClientError
import asyncio
import pytest
from pytest_asyncio import fixture as asyncio_fixture

from dynamodb_autoincrement import DynamoDBAutoIncrement, DynamoDBHistoryAutoIncrement


N = 20


@asyncio_fixture
async def create_tables(asyncio_dynamodb):
    await asyncio.gather(
        *(
            asyncio_dynamodb.create_table(**kwargs)
            for kwargs in [
                {
                    "AttributeDefinitions": [
                        {"AttributeName": "tableName", "AttributeType": "S"}
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                    "KeySchema": [{"AttributeName": "tableName", "KeyType": "HASH"}],
                    "TableName": "autoincrement",
                },
                {
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [
                        {"AttributeName": "widgetID", "AttributeType": "N"}
                    ],
                    "KeySchema": [{"AttributeName": "widgetID", "KeyType": "HASH"}],
                    "TableName": "widgets",
                },
                {
                    "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [
                        {"AttributeName": "widgetID", "AttributeType": "N"},
                        {"AttributeName": "version", "AttributeType": "N"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "widgetID", "KeyType": "HASH"},
                        {"AttributeName": "version", "KeyType": "RANGE"},
                    ],
                    "TableName": "widgetHistory",
                },
            ]
        )
    )


@pytest.fixture
def autoincrement_safely(asyncio_dynamodb, create_tables):
    return DynamoDBAutoIncrement(
        counter_table_name="autoincrement",
        counter_table_key={"tableName": "widgets"},
        table_name="widgets",
        attribute_name="widgetID",
        initial_value=1,
        dynamodb=asyncio_dynamodb,
    )


@pytest.fixture
def autoincrement_dangerously(asyncio_dynamodb, create_tables):
    return DynamoDBAutoIncrement(
        counter_table_name="autoincrement",
        counter_table_key={"tableName": "widgets"},
        table_name="widgets",
        attribute_name="widgetID",
        initial_value=1,
        dynamodb=asyncio_dynamodb,
        dangerously=True,
    )


@pytest.fixture
def autoincrement_version(asyncio_dynamodb, create_tables):
    return DynamoDBHistoryAutoIncrement(
        dynamodb=asyncio_dynamodb,
        counter_table_name="widgets",
        counter_table_key={
            "widgetID": 1,
        },
        attribute_name="version",
        table_name="widgetHistory",
        initial_value=1,
    )


@pytest.mark.parametrize("last_id", [None, 1, 2, 3])
@pytest.mark.asyncio
async def test_autoincrement_safely(autoincrement_safely, asyncio_dynamodb, last_id):
    if last_id is None:
        next_id = 1
    else:
        await asyncio_dynamodb.put_item(
            TableName="autoincrement",
            Item={"tableName": "widgets", "widgetID": last_id},
        )
        next_id = last_id + 1

    result = await autoincrement_safely.put({"widgetName": "runcible spoon"})
    assert result == next_id

    assert (await asyncio_dynamodb.scan(TableName="widgets"))["Items"] == [
        {"widgetID": next_id, "widgetName": "runcible spoon"},
    ]

    assert (await asyncio_dynamodb.scan(TableName="autoincrement"))["Items"] == [
        {
            "tableName": "widgets",
            "widgetID": next_id,
        },
    ]


@pytest.mark.asyncio
async def test_autoincrement_safely_handles_many_parallel_puts(autoincrement_safely):
    ids = list(range(1, N + 1))
    result = sorted(
        await asyncio.gather(*(autoincrement_safely.put({}) for _ in range(N)))
    )
    assert result == ids


@pytest.mark.asyncio
async def test_autoincrement_safely_raises_error_for_unhandled_dynamodb_exceptions(
    autoincrement_safely,
):
    with pytest.raises(
        ClientError, match="Item size has exceeded the maximum allowed size"
    ):
        await autoincrement_safely.put(
            {
                "widgetName": "runcible spoon",
                "description": "Hello world! " * 32000,
            }
        )


@pytest.mark.asyncio
async def test_autoincrement_dangerously_handles_many_serial_puts(
    autoincrement_dangerously,
):
    ids = list(range(1, N + 1))
    results = [await autoincrement_dangerously.put({"widgetName": id}) for id in ids]
    assert sorted(results) == ids


@pytest.mark.asyncio
async def test_autoincrement_dangerously_fails_on_many_parallel_puts(
    autoincrement_dangerously,
):
    with pytest.raises(ClientError, match="The conditional request failed"):
        await asyncio.gather(*(autoincrement_dangerously.put({}) for _ in range(N)))


@asyncio_fixture(params=[None, {"widgetID": 1}, {"widgetID": 1, "version": 1}])
async def initial_item(request, create_tables, asyncio_dynamodb):
    if request.param is not None:
        await asyncio_dynamodb.put_item(TableName="widgets", Item=request.param)
    return request.param


@pytest.mark.parametrize("tracked_attribute_value", [None, 42])
@pytest.mark.asyncio
async def test_autoincrement_version(
    autoincrement_version, asyncio_dynamodb, initial_item, tracked_attribute_value
):
    assert await autoincrement_version.list() == []
    assert await autoincrement_version.get() == initial_item
    assert await autoincrement_version.get(1) is None
    has_initial_item = initial_item is not None

    new_version = await autoincrement_version.put(
        {
            "name": "Handy Widget",
            "description": "Does Everything!",
            "version": tracked_attribute_value,
        }
    )
    assert new_version == 1 + has_initial_item

    history_items = (
        await asyncio_dynamodb.query(
            TableName="widgetHistory",
            KeyConditionExpression="widgetID = :widgetID",
            ExpressionAttributeValues={
                ":widgetID": 1,
            },
        )
    )["Items"]
    assert len(history_items) == int(has_initial_item)

    assert await autoincrement_version.list() == list(range(1, len(history_items) + 1))


@pytest.mark.parametrize("tracked_attribute_value", [None, 42])
@pytest.mark.asyncio
async def test_autoincrement_version_handles_many_serial_puts(
    autoincrement_version, initial_item, tracked_attribute_value
):
    has_initial_item = initial_item is not None
    versions = list(range(1 + has_initial_item, N + has_initial_item + 1))
    result = sorted(
        await asyncio.gather(
            *(
                autoincrement_version.put({"version": tracked_attribute_value})
                for _ in range(N)
            )
        )
    )
    assert result == versions
