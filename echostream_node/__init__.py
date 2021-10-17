from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from os import cpu_count, environ
from typing import TYPE_CHECKING, Any, Callable, Union

import dynamic_function_loader
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


class __NodeSession(BotocoreSession):
    def __init__(self, node: BaseNode, duration: int = 900) -> None:
        super().__init__()

        def refresher():
            credentials = node.gql_client.execute(
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

    def __init__(self, auditor: Auditor, message: Message, source: str = None) -> None:
        super().__init__()
        super().__setattr__("attributes", auditor(message.body))
        super().__setattr__("date_time", datetime.now(timezone.utc))
        super().__setattr__("previous_tracking_ids", message.previous_tracking_ids)
        super().__setattr__("source_node", source)
        super().__setattr__("tracking_id", message.tracking_id)


class BaseNode(ABC):
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
        self.__client = GqlClient(
            fetch_schema_from_transport=True,
            transport=gql_transport_cls(
                cognito,
                appsync_endpoint or environ["APPSYNC_ENDPOINT"],
            ),
        )
        self.__name = name or environ["NODE"]
        self.__tenant = tenant or environ["TENANT"]
        data = self.__client.execute(
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
            session = Session(botocore_session=__NodeSession(self))
        self.__sqs_client: SQSClient = session.client(
            "sqs",
            Config(
                max_pool_connections=min(20, ((cpu_count() or 1) + 4) * 2),
                retries={"mode": "standard"},
            ),
        )
        with ThreadPoolExecutor() as executor:
            wait(
                [
                    executor.submit(self.__initialize_config),
                    executor.submit(self._initialize_edges),
                    executor.submit(self.__initialize_message_types),
                ]
            )

    def __initialize_config(self) -> None:
        data: dict[str, dict] = self.gql_client.execute(
            _GET_CONFIG_GQL,
            variable_values=dict(name=self.name, tenant=self.tenant),
        )["GetNode"]
        self._config: dict[str, Any] = (
            json.loads(data["tenant"].get("config", {}))
            | json.loads(data["app"].get("config", {}))
            | json.loads(data.get("config", {}))
        )

    def __initialize_message_types(self) -> None:
        data: dict[str, Any] = self.gql_client.execute(
            _GET_MESSAGE_TYPES,
            variable_values=dict(name=self.name, tenant=self.tenant),
        )["GetNode"]
        self.__receive_message_type: str = None
        self.__receive_message_auditor: Auditor = None
        if receive_message_type := data.get("receiveMessageType"):
            self.__receive_message_type = receive_message_type["name"]
            self.__receive_message_auditor = dynamic_function_loader.load(
                receive_message_type["auditor"]
            )
        self.__send_message_type: str = None
        self.__send_message_auditor = None
        if send_message_type := data.get("sendMessageType"):
            self.__send_message_type = send_message_type["name"]
            self.__send_message_auditor: Auditor = dynamic_function_loader.load(
                send_message_type["auditor"]
            )

    @abstractmethod
    def _initialize_edges(self) -> None:
        data: dict[str, list[dict[str, Union[str, dict]]]] = self.gql_client.execute(
            _GET_EDGES,
            variable_values=dict(name=self.name, tenant=self.tenant),
        )
        self.__sources = frozenset(
            {
                Edge(name=edge["source"]["name"], queue=edge["queue"])
                for edge in data["receiveEdges"]
            }
        )
        self.__targets = frozenset(
            {
                Edge(name=edge["target"]["name"], queue=edge["queue"])
                for edge in data["sendEdges"]
            }
        )

    @property
    def app(self) -> str:
        return self.__app

    @property
    def app_type(self) -> str:
        return self.__app_type

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @property
    def gql_client(self) -> GqlClient:
        return self.__client

    @property
    def name(self) -> str:
        return self.__name

    @property
    def node_type(self) -> str:
        return self.__node_type

    @property
    def receive_message_auditor(self) -> Auditor:
        return self.__receive_message_auditor or (lambda x: {})

    @property
    def receive_message_type(self) -> str:
        return self.__receive_message_type

    @property
    def send_message_auditor(self) -> Auditor:
        return self.__send_message_auditor or (lambda x: {})

    @property
    def send_message_type(self) -> str:
        return self.__send_message_type

    @property
    def sources(self) -> frozenset[Edge]:
        return self.__sources

    @property
    def sqs_client(self) -> SQSClient:
        return self.__sqs_client

    @property
    def targets(self) -> frozenset[Edge]:
        return self.__targets

    @property
    def tenant(self) -> str:
        return self.__tenant


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
        node: BaseNode = None,
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


@dataclass(frozen=True)
class PresignedPost:
    expiration: datetime
    fields: dict[str, str]
    url: str


@dataclass(frozen=True)
class Edge:
    name: str
    queue: str
