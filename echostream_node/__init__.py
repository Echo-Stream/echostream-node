from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timezone
from os import cpu_count, environ
from typing import TYPE_CHECKING, Any, Callable, Union

import awsserviceendpoints
import simplejson as json
from boto3.session import Session
from botocore.config import Config
from botocore.credentials import DeferredRefreshableCredentials
from botocore.session import Session as BotocoreSession
from gql import Client as GqlClient
from gql import gql
from pycognito import Cognito


def getLogger() -> logging.Logger:
    return logging.getLogger("echostream-node")


getLogger().addHandler(logging.NullHandler())

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef
else:
    MessageAttributeValueTypeDef = dict
    SQSClient = object

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
                    __typename
                    ... on ExternalApp {
                        name
                    }
                    ... on CrossAccountApp {
                        name
                    }
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

_GET_NODE_GQL = gql(
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
                receiveEdges {
                    queue
                    source {
                        name
                    }
                }
                receiveMessageType {
                    auditor
                    name
                }
                sendEdges {
                    queue
                    target {
                        name
                    }
                }
                sendMessageType {
                    auditor
                    name
                }
            }
            ... on ManagedNode {
                app {
                    config
                }
                config
                receiveEdges {
                    queue
                    source {
                        name
                    }
                }
                receiveMessageType {
                    auditor
                    name
                }
                sendEdges {
                    queue
                    target {
                        name
                    }
                }
                sendMessageType {
                    auditor
                    name
                }
            }
            tenant {
                config
            }
        }
    }
    """
)


class _NodeSession(BotocoreSession):
    def __init__(self, node: Node, duration: int = 900) -> None:
        super().__init__()

        def refresher():
            credentials = node._gql_client.execute(
                _GET_CREDENTIALS_GQL,
                variable_values=dict(
                    name=node.app, tenant=node.tenant, duration=duration
                ),
            )["GetApp"]["GetAwsCredentials"]
            return dict(
                access_key=credentials["accessKeyId"],
                secret_key=credentials["secret_key"],
                token=credentials["sessionToken"],
                expiry_time=credentials["expiration"],
            )

        setattr(
            self,
            "_credentials",
            DeferredRefreshableCredentials(
                method="GetApp.GetAwsCredentials", refresh_using=refresher
            ),
        )


Auditor = Callable[..., dict[str, Any]]


@dataclass(frozen=True, init=False)
class AuditRecord:
    attributes: dict[str, Any]
    date_time: datetime
    tracking_id: str
    previous_tracking_ids: frozenset[str]
    source_node: str

    def __init__(
        self, *, attributes: dict[str, Any], message: Message, source: str = None
    ) -> None:
        super().__init__()
        super().__setattr__("attributes", attributes)
        super().__setattr__("date_time", datetime.now(timezone.utc))
        super().__setattr__("previous_tracking_ids", message.previous_tracking_ids)
        super().__setattr__("source_node", source)
        super().__setattr__("tracking_id", message.tracking_id)


@dataclass(frozen=True, init=False)
class BulkDataStorage:
    presigned_get: str
    presigned_post: PresignedPost

    def __init__(self, bulk_data_storage: dict[str, Union[str, PresignedPost]]) -> None:
        self.presigned_get = bulk_data_storage["presignedGet"]
        self.presigned_post = PresignedPost(
            expiration=datetime.fromisoformat(
                bulk_data_storage["presignedPost"]["expiration"]
            ),
            fields=bulk_data_storage["presignedPost"]["fields"],
            url=bulk_data_storage["presignedPost"]["url"],
        )

    @property
    def expired(self) -> bool:
        return self.presigned_post.expiration < datetime.utcnow()


@dataclass(frozen=True)
class Edge:
    name: str
    queue: str


LambdaEvent = Union[bool, dict, float, int, list, str, tuple, None]


@dataclass(frozen=True, init=False)
class Message:
    body: str
    group_id: str
    tracking_id: str
    previous_tracking_ids: frozenset[str]

    def __init__(
        self,
        body: str,
        tracking_id: str,
        group_id: str = None,
        node: Node = None,
        previous_tracking_ids: Union[set[str], list[str], str] = None,
    ) -> None:
        super().__init__()
        super().__setattr__("body", body)
        super().__setattr__("group_id", group_id or node.name.replace(" ", "_"))
        super().__setattr__("tracking_id", tracking_id)
        if isinstance(previous_tracking_ids, str):
            previous_tracking_ids = json.loads(previous_tracking_ids)
        super().__setattr__(
            "previous_tracking_ids",
            frozenset(previous_tracking_ids) if previous_tracking_ids else None,
        )
        if self.length > 262144:
            raise ValueError(f"Message is > 262,144 in size")

    @property
    def sqs_message(self) -> dict:
        return dict(
            MessageAttributes=self.message_attributes,
            MessageBody=self.body,
            MessageGroupId=self.group_id,
        )

    @property
    def length(self) -> int:
        length = len(self.body)
        for name, attribute in self.message_attributes.items():
            value = attribute[
                "StringValue"
                if (data_type := attribute["DataType"]) in ("String", "Number")
                else "BinaryValue"
            ]
            length += len(name) + len(data_type) + len(value)
        return length

    @property
    def message_attributes(self) -> dict[str, MessageAttributeValueTypeDef]:
        message_attributes = dict(
            trackingId=MessageAttributeValueTypeDef(
                DataType="String", StringValue=self.tracking_id
            )
        )
        if self.previous_tracking_ids:
            message_attributes["prevTrackingIds"] = MessageAttributeValueTypeDef(
                DataType="String",
                StringValue=json.dumps(
                    list(self.previous_tracking_ids), separators=(",", ":")
                ),
            )
        return message_attributes


class Node(ABC):
    def __init__(
        self,
        *,
        gql_transport_cls: type,
        appsync_endpoint: str = None,
        client_id: str = None,
        name: str = None,
        password: str = None,
        tenant: str = None,
        user_pool_id: str = None,
        username: str = None,
    ) -> None:
        super().__init__()
        cognito = Cognito(
            client_id=client_id or environ["CLIENT_ID"],
            user_pool_id=user_pool_id or environ["USER_POOL_ID"],
            username=username or environ["USERNAME"],
        )
        cognito.authenticate(password=password or environ["PASSWORD"])
        self.__gql_client = GqlClient(
            fetch_schema_from_transport=True,
            transport=gql_transport_cls(
                cognito,
                appsync_endpoint or environ["APPSYNC_ENDPOINT"],
            ),
        )
        self.__name = name or environ["NODE"]
        self.__tenant = tenant or environ["TENANT"]
        data = self.__gql_client.execute(
            _GET_APP_GQL,
            variable_values=dict(name=self.__name, tenant=self.__tenant),
        )["GetNode"]
        self.__app = data["app"]["name"]
        self.__node_type = data["__typename"]
        self.__app_type = data["app"]["__typename"]
        session: Session = None
        if self.__node_type == "ExternalNode" and self.__app_type == "CrossAccountApp":
            session = Session()
        else:
            session = Session(botocore_session=_NodeSession(self))
        self.__sqs_client: SQSClient = session.client(
            "sqs",
            Config(
                max_pool_connections=min(20, ((cpu_count() or 1) + 4) * 2),
                retries={"mode": "standard"},
            ),
        )
        self.__config: dict[str, Any] = None
        self._receive_message_auditor: Auditor = None
        self._receive_message_type: str = None
        self._send_message_auditor: Auditor = None
        self._send_message_type: str = None
        self.__sources: frozenset[Edge] = None
        self.__targets: frozenset[Edge] = None

    @property
    def _gql_client(self) -> GqlClient:
        return self.__gql_client

    @property
    def _sources(self) -> frozenset[Edge]:
        return self.__sources

    @_sources.setter
    def _sources(self, sources: set[Edge]) -> None:
        self.__sources = frozenset(sources)

    @property
    def _sqs_client(self) -> SQSClient:
        return self.__sqs_client

    @property
    def _targets(self) -> frozenset[Edge]:
        return self.__targets

    @_targets.setter
    def _targets(self, targets: set[Edge]) -> None:
        self.__targets = frozenset(targets)

    @property
    def app(self) -> str:
        return self.__app

    @property
    def app_type(self) -> str:
        return self.__app_type

    @property
    def config(self) -> dict[str, Any]:
        return self.__config

    @config.setter
    def config(self, config: dict[str, Any]) -> None:
        self.__config = config

    @property
    def name(self) -> str:
        return self.__name

    @property
    def node_type(self) -> str:
        return self.__node_type

    @property
    def receive_message_auditor(self) -> Auditor:
        return self._receive_message_auditor or (lambda x: {})

    @property
    def receive_message_type(self) -> str:
        return self._receive_message_type

    @property
    def send_message_type(self) -> str:
        return self._send_message_type

    @property
    def send_message_auditor(self) -> Auditor:
        return self._send_message_auditor or (lambda x: {})

    @property
    def tenant(self) -> str:
        return self.__tenant


@dataclass(frozen=True)
class PresignedPost:
    expiration: datetime
    fields: dict[str, str]
    url: str
