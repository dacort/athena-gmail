import base64
import pickle
import os
import time
from uuid import uuid4

from googleapiclient.discovery import build
from googleapiclient.http import BatchHttpRequest
import pyarrow as pa

from athena.federation.utils import AthenaSDKUtils
from athena.federation.federator import AthenaFederator
import athena.federation.models as models


# These variables are used for S3 spill locations
S3_BUCKET = os.environ['TARGET_BUCKET']
# S3_PREFIX = os.environ['TARGET_PREFIX'].rstrip('/')  # Ensure that the prefix does *not* have a slash at the end


# We maintain a cache of Label names to IDs as we allow people to query by Label name
# e.g. SELECT * FROM "your.email"."All Mail"
# "All Mail" is a reserved system label that _does not_ show up in `users.labels.list` response


class GmailAthena(AthenaFederator):
    def __init__(self, event) -> None:
        super().__init__(event)

    def PingRequest(self) -> models.PingResponse:
        return models.PingResponse("gmail", self.event['queryId'], "gmail")

    def ListSchemasRequest(self):
        return models.ListSchemasResponse("gmail", ['personal'])

    def ListTablesRequest(self) -> models.ListTablesResponse:
        tableResponse = models.ListTablesResponse("gmail")
        tableResponse.addTableDefinition("personal", "All Mail")
        return tableResponse

    def GetTableRequest(self) -> models.GetTableResponse:
        schema = pa.schema([('messageId', pa.string()),
                            ('subject', pa.string()),
                            ('from', pa.string()),
                            ('sentDate', pa.string()),
                            ('meta_gmailquery', pa.string())])
        tr = models.GetTableResponse("gmail", "personal", "All Mail", schema)
        return tr

    def GetTableLayoutRequest(self) -> models.GetTableLayoutResponse:
        # There are a lot of search operators built into Gmail ( https://support.google.com/mail/answer/7190?hl=en )
        # and, as such, probably a lot of things we _can_ partition on.
        # We won't partition yet, but at the very least it probably makes sense to partition on date...

        # The partition schema above was reused from CloudTrail example - we need to
        # add (also?) the schema we want to pass back in a split?
        # e.g. messageIds: pa.list_(pa.int64())
        return models.GetTableLayoutResponse("gmail", "personal", "All Mail", None)

    def GetSplitsRequest(self) -> models.GetSplitsResponse:
        splits = [
            {
                "spillLocation": {
                    "@type": "S3SpillLocation",
                    "bucket": S3_BUCKET,
                    "key": "athena-spill/7b2b96c9-1be5-4810-ac2a-163f754e132c/1a50edb8-c4c7-41d7-8a0d-1ce8e510755f",
                    "directory": True
                },
                "properties": {}
            }
        ]
        sr = models.GetSplitsResponse("gmail", splits)
        return sr

    def ReadRecordsRequest(self) -> models.ReadRecordsResponse:
        schema = AthenaSDKUtils.parse_encoded_schema(
            self.event['schema']['schema'])

        # Try to get a list of message IDs from the gmail API
        svc = self._get_gmail_service()
        message_list = self._get_messages(svc)
        records = {k: [] for k in schema.names}

        # Create a function to process messages from the batch request
        def process_message(request_id, response, exception):
            if exception is not None:
                # Do something with the exception
                print("oops", exception)
            else:
                # Do something with the response
                records['messageId'].append(response['id'])
                records['subject'].append(
                    [h['value'] for h in response.get("payload").get("headers") if h['name'] == 'Subject'][0])
                records['from'].append(
                    [h['value'] for h in response.get("payload").get("headers") if h['name'] == 'From'][0])
                records['sentDate'].append(time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(int(response.get("internalDate"))/1000)))
                records['meta_gmailquery'].append("")

        # Create a new batch request that fetches each message from the API
        batch = svc.new_batch_http_request()
        for msg in message_list['messages']:
            batch.add(svc.users().messages().get(
                userId='me', id=msg['id']), callback=process_message)
        batch.execute()
        # .execute() is a blocking function

        # Convert the records to pyarrow records
        pa_records = AthenaSDKUtils.encode_pyarrow_records(schema, records)
        rrr = models.ReadRecordsResponse("gmail", schema, pa_records)
        return rrr

    def _parse_schema(self, encoded_schema):
        return pa.read_schema(pa.BufferReader(base64.b64decode(encoded_schema)))

    def _get_sample_records(self, schema):
        # records = {k: [] for k in schema.names}
        records = {'messageId': ["1", "2", "3", "4"],
                   'subject': ["hello", "happy", "boxing", "day"],
                   'from': ["i@loveyou.to", "me@you.ca", "you@somewhere.com", "bob@bob.com"],
                   'sentDate': ["2020-12-18", "2020-12-20", "2020-12-26", "2020-12-26"],
                   'meta_gmailquery': ['', '', '', '']}
        pa_records = pa.RecordBatch.from_arrays(
            [pa.array(records[name]) for name in schema.names], schema=schema)
        return pa_records

    def _get_gmail_service(self):
        creds = None
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
        service = build('gmail', 'v1', credentials=creds)

        return service

    def _get_messages(self, service):
        # Perform a basic search
        return service.users().messages().list(userId='me').execute()


def lambda_handler(event, context):
    print(event)
    request_type = event['@type']

    ga = GmailAthena(event)
    response = getattr(ga, request_type)().as_dict()
    print(response)
    return response
