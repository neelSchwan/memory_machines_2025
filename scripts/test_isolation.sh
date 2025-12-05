#!/bin/bash

TABLE_NAME="tenant_processed_logs"

echo "----- DynamoDB Table Schema -----"
aws dynamodb describe-table \
  --table-name $TABLE_NAME \
  --query 'Table.KeySchema'

echo -e "\n------ Query tenant-1 data -----"
aws dynamodb query \
  --table-name $TABLE_NAME \
  --key-condition-expression "tenant_id = :tid" \
  --expression-attribute-values '{":tid":{"S":"tenant-1"}}' \
  --return-consumed-capacity TOTAL

echo -e "\n---- Query tenant-2 data -----"
aws dynamodb query \
  --table-name $TABLE_NAME \
  --key-condition-expression "tenant_id = :tid" \
  --expression-attribute-values '{":tid":{"S":"tenant-2"}}' \
  --return-consumed-capacity TOTAL

echo -e "\n----- Query tenant-3 data -----"
aws dynamodb query \
  --table-name $TABLE_NAME \
  --key-condition-expression "tenant_id = :tid" \
  --expression-attribute-values '{":tid":{"S":"tenant-3"}}' \
  --return-consumed-capacity TOTAL
