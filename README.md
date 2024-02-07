[![codecov](https://codecov.io/gh/nasa-gcn/python-dynamodb-autoincrement/graph/badge.svg?token=ezsMkImff0)](https://codecov.io/gh/nasa-gcn/python-dynamodb-autoincrement)

# python-dynamodb-autoincrement

Use optimistic locking to put DynamoDB records with auto-incrementing attributes. This is a Python port of https://github.com/nasa-gcn/dynamodb-autoincrement. Inspired by:

- https://aws.amazon.com/blogs/aws/new-amazon-dynamodb-transactions/
- https://bitesizedserverless.com/bite/reliable-auto-increments-in-dynamodb/

## FIXME

This package currently depends on code that is in a pull request for boto3 that is not yet merged or released.
See https://github.com/boto/boto3/pull/4010.
