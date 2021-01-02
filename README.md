# Athena Gmail Connector

_Another Thanksgiving day experiment from @dacort_

## Overview

Ever wanted to query your email from Athena? Well now you can!

## Usage

You can (eventually) use any advanched search syntax Gmail supports in your `WHERE` clause.

- `SELECT * FROM gmail.messages WHERE meta_gmailquery='from:amazonaws.com'`

For this experiment, we only load 100 messages.

## Requirements

- Create a Google OAuth client configured as a "Desktop App"
- Run `python quickstart.py` to populate local credentials

## Docker Usage

- In this directory, build the Docker image:

```shell
docker build -t gathena .
```

- Start the container

```shell
docker run -p 9000:8080 gathena:latest
```

- Test the endpoint!

```shell
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"@type": "PingRequest", "identity": {"id": "UNKNOWN", "principal": "UNKNOWN", "account": "123456789012", "arn": "arn:aws:iam::123456789012:root", "tags": {}, "groups": []}, "catalogName": "gmail", "queryId": "1681559a-548b-4771-874c-2aa2ea7c39ab"}'
```

## Uploading

- Create a container repository

```shell
export AWS_REGION=us-east-1
aws ecr create-repository --repository-name gathena --image-scanning-configuration scanOnPush=true
docker tag gathena:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/gathena:latest
aws ecr get-login-password | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/gathena:latest
```

- Create a Lambda function with the above container

- Add a new data source to Athena pointing to the Lambda function

- If changing code, use `AWS_ACCOUNT_ID=123456789012 make docker` to rebuild and update your Lambda function.

## Schema thoughts

- old schema
source_file (string)
ts (string)
from (string)
to (string)
subject (string)
message_id (string)
in_reply_to_id (string)
dt (string) (Partitioned)

- thoughts from https://stackoverflow.com/questions/14641865/email-database-design-schema
from : string
to : string
subject: string
date (range): datetime
attachments (names & types only) : Object Array
message contents : string
(optional) mailbox / folder structure: string

- https://cwiki.apache.org/confluence/display/solr/MailEntityProcessor
single valued fields :

messageId
subject
from
sentDate
xMailer

multi valued fields :

allTo
flags : possible flags are 'answered', 'deleted', 'draft', 'flagged' , 'recent', 'seen'
content
attachment
attachmentNames;