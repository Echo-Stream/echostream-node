from datetime import datetime
from typing import TYPE_CHECKING, Dict, NamedTuple, TypedDict, Union

from gql import gql
import logging


def getLogger() -> logging.Logger:
    return logging.getLogger("echostream-node")


getLogger().addHandler(logging.NullHandler())

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef
else:
    MessageAttributeValueTypeDef = dict

LambdaEvent = LambdaReturn = Union[bool, dict, float, int, list, str, tuple, None]

_CREATE_AUDIT_RECORDS = gql(
    """
    query getNode($name: String!, $tenant: $String!, $messageType: String!, $auditRecords: [AuditRecord!]!) {
        GetNode(name: $name, tenant: $tenant) {
            ... on ExternalNode {
                CreateAuditRecords(messageType: $messageType, auditRecords: $auditRecords) 
            }
            ... on ManagedNode {
                CreateAuditRecords(messageType: $messageType, auditRecords: $auditRecords)
            }
        }
    }
    """
)

_GET_BULK_DATA_STORAGE_GQL = gql(
    """
    query getBulkDataStorage($tenant: String!) {
        GetBulkDataStorage(tenant: $tenant, contentEncoding: gzip, count: 20) {
            presignedGet
            presignedPost {
                expiration
                fields
                url
            }
        }
    }
    """
)

_GET_CREDENTIALS_GQL = gql(
    """
    query getAppAwsCredentials($name: String!, $tenant: String!, $duration: Int!) {
        GetApp(name: $name, tenant: $tenant) {
            ... on ExternalApp {
                GetAwsCredentials(duration: $duration) {
                    accessKeyId
                    secretAccessKey
                    sessionToken
                    expiration
                }
            }
            ... on ManagedApp {
                GetAwsCredentials(duration: $duration) {
                    accessKeyId
                    secretAccessKey
                    sessionToken
                    expiration
                }
            }
        }
    }
    """
)

_GET_APP_GQL = gql(
    """
    query getNode($name: String!, $tenant: $String!) {
        GetNode(name: $name, tenant: $tenant) {
            __typename
            ... on ExternalNode {
                app {
                    name
                }
            }
            ... on ManagedNode {
                app {
                    name
                }
        }
    }
    """
)

_GET_CONFIG_GQL = gql(
    """
    query getNode($name: String!, $tenant: $String!) {
        GetNode(name: $name, tenant: $tenant) {
            ... on ExternalNode {
                app {
                    .. on CrossAccountApp {
                        config
                    }
                    .. on ExternalApp {
                        config
                    }
                }
                config
            }
            ... on ManagedNode {
                app {
                    config
                }
                config
            }
            tenant {
                config
            }
        }
    }
    """
)

_GET_MESSAGE_TYPES = gql(
    """
    query getNode($name: String!, $tenant: $String!) {
        GetNode(name: $name, tenant: $tenant) {
            ... on ExternalNode {
                receiveMessageType {
                    auditor
                    name
                }
                sendMessageType {
                    auditor
                    name
                }
            }
            ... on ManagedNode {
                receiveMessageType {
                    auditor
                    name
                }
                sendMessageType {
                    auditor
                    name
                }
            }
        }
    }
    """
)

_GET_EDGES = gql(
    """
    query getNode($name: String!, $tenant: $String!) {
        GetNode(name: $name, tenant: $tenant) {
            ... on ExternalNode {
                receiveEdges {
                    queue
                    source {
                        name
                    }
                }
                sendEdges {
                    queue
                    target {
                        name
                    }
                }
            }
            ... on ManagedNode {
                receiveEdges {
                    queue
                    source {
                        name
                    }
                }
                sendEdges {
                    queue
                    target {
                        name
                    }
                }
            }
        }
    }
    """
)


class Message(TypedDict, total=False):
    MessageAttributes: Dict[str, MessageAttributeValueTypeDef]
    MessageBody: str
    MessageDeduplicationId: str
    MessageGroupId: str
    ReceiptHandle: str


class PresignedPost(TypedDict):
    expiration: datetime
    fields: Dict[str, str]
    url: str


class AuditRecord(TypedDict):
    attributes: dict[str, Union[bool, int, float, str]]
    datetime: datetime
    previousTrackingIds: list[str]
    sourceNode: str
    trackingId: str


class BulkDataStorage:
    def __init__(self, bulk_data_storage: Dict[str, Union[str, PresignedPost]]) -> None:
        self.__presigned_get = bulk_data_storage["presignedGet"]
        self.__presigned_post = PresignedPost(
            expiration=datetime.fromisoformat(
                bulk_data_storage["presignedPost"]["expiration"]
            ),
            fields=bulk_data_storage["presignedPost"]["fields"],
            url=bulk_data_storage["presignedPost"]["url"],
        )
        super().__init__()

    @property
    def expired(self) -> bool:
        return self.presignedPost["expiration"] < datetime.utcnow()

    @property
    def presignedGet(self) -> str:
        return self.__presigned_get

    @property
    def presignedPost(self) -> PresignedPost:
        return self.__presigned_post
