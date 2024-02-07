# Copyright © 2023 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.

from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
import pytest

from dynamodb_autoincrement import DynamoDBAutoIncrement, DynamoDBHistoryAutoIncrement


N = 20


@pytest.fixture
def create_tables(dynamodb):
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
    ]:
        dynamodb.create_table(**kwargs)


@pytest.fixture
def autoincrement_safely(dynamodb, create_tables):
    return DynamoDBAutoIncrement(
        counter_table_name="autoincrement",
        counter_table_key={"tableName": "widgets"},
        table_name="widgets",
        attribute_name="widgetID",
        initial_value=1,
        dynamodb=dynamodb,
    )


@pytest.fixture
def autoincrement_dangerously(dynamodb, create_tables):
    return DynamoDBAutoIncrement(
        counter_table_name="autoincrement",
        counter_table_key={"tableName": "widgets"},
        table_name="widgets",
        attribute_name="widgetID",
        initial_value=1,
        dynamodb=dynamodb,
        dangerously=True,
    )


@pytest.fixture
def autoincrement_version(dynamodb, create_tables):
    return DynamoDBHistoryAutoIncrement(
        dynamodb=dynamodb,
        counter_table_name="widgets",
        counter_table_key={
            "widgetID": 1,
        },
        attribute_name="version",
        table_name="widgetHistory",
        initial_value=1,
    )


@pytest.mark.parametrize("last_id", [None, 1, 2, 3])
def test_autoincrement_safely(autoincrement_safely, dynamodb, last_id):
    if last_id is None:
        next_id = 1
    else:
        dynamodb.put_item(
            TableName="autoincrement",
            Item={"tableName": "widgets", "widgetID": last_id},
        )
        next_id = last_id + 1

    result = autoincrement_safely.put({"widgetName": "runcible spoon"})
    assert result == next_id

    assert dynamodb.scan(TableName="widgets")["Items"] == [
        {"widgetID": next_id, "widgetName": "runcible spoon"},
    ]

    assert dynamodb.scan(TableName="autoincrement")["Items"] == [
        {
            "tableName": "widgets",
            "widgetID": next_id,
        },
    ]


def test_autoincrement_safely_handles_many_parallel_puts(autoincrement_safely):
    ids = list(range(1, N + 1))
    with ThreadPoolExecutor() as executor:
        result = sorted(executor.map(autoincrement_safely.put, [{} for _ in range(N)]))
    assert result == ids


def test_autoincrement_safely_raises_error_for_unhandled_dynamodb_exceptions(
    autoincrement_safely,
):
    with pytest.raises(
        ClientError, match="Item size has exceeded the maximum allowed size"
    ):
        autoincrement_safely.put(
            {
                "widgetName": "runcible spoon",
                "description": "Hello world! " * 32000,
            }
        )


def test_autoincrement_dangerously_handles_many_serial_puts(autoincrement_dangerously):
    ids = list(range(1, N + 1))
    results = [autoincrement_dangerously.put({"widgetName": id}) for id in ids]
    assert sorted(results) == ids


def test_autoincrement_dangerously_fails_on_many_parallel_puts(
    autoincrement_dangerously, dynamodb
):
    with pytest.raises(dynamodb.meta.client.exceptions.ConditionalCheckFailedException):
        with ThreadPoolExecutor() as executor:
            for _ in executor.map(
                autoincrement_dangerously.put, [{} for _ in range(N)]
            ):
                pass


@pytest.fixture(params=[None, {"widgetID": 1}, {"widgetID": 1, "version": 1}])
def initial_item(request, create_tables, dynamodb):
    if request.param is not None:
        dynamodb.put_item(TableName="widgets", Item=request.param)
    return request.param


@pytest.mark.parametrize("tracked_attribute_value", [None, 42])
def test_autoincrement_version(
    autoincrement_version, dynamodb, initial_item, tracked_attribute_value
):
    assert autoincrement_version.list() == []
    assert autoincrement_version.get() == initial_item
    assert autoincrement_version.get(1) is None
    has_initial_item = initial_item is not None

    new_version = autoincrement_version.put(
        {
            "name": "Handy Widget",
            "description": "Does Everything!",
            "version": tracked_attribute_value,
        }
    )
    assert new_version == 1 + has_initial_item

    history_items = dynamodb.query(
        TableName="widgetHistory",
        KeyConditionExpression="widgetID = :widgetID",
        ExpressionAttributeValues={
            ":widgetID": 1,
        },
    )["Items"]
    assert len(history_items) == int(has_initial_item)

    assert autoincrement_version.list() == list(range(1, len(history_items) + 1))


@pytest.mark.parametrize("tracked_attribute_value", [None, 42])
def test_autoincrement_version_handles_many_serial_puts(
    autoincrement_version, initial_item, tracked_attribute_value
):
    has_initial_item = initial_item is not None
    versions = list(range(1 + has_initial_item, N + has_initial_item + 1))
    with ThreadPoolExecutor() as executor:
        result = sorted(
            executor.map(
                autoincrement_version.put,
                [{"version": tracked_attribute_value} for _ in range(N)],
            )
        )
    assert result == versions
