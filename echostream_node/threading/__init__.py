from __future__ import annotations

import atexit
import json
from concurrent.futures import ThreadPoolExecutor, wait
from copy import deepcopy
from datetime import datetime, timezone
from gzip import GzipFile
from io import BytesIO
from os import cpu_count, environ
from queue import Empty, Queue, SimpleQueue
from threading import Condition, Event, Thread
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, Iterator, Literal, Union
from uuid import uuid4

import dynamic_function_loader
from boto3 import Session
from botocore.config import Config
from botocore.credentials import DeferredRefreshableCredentials
from botocore.session import Session as BotocoreSession
from gql import Client as GqlClient
from gql.transport.requests import RequestsHTTPTransport
from pycognito import Cognito
from requests import post

from .. import (
    _CREATE_AUDIT_RECORDS,
    _GET_APP_GQL,
    _GET_BULK_DATA_STORAGE_GQL,
    _GET_CONFIG_GQL,
    _GET_CREDENTIALS_GQL,
    _GET_EDGES,
    _GET_MESSAGE_TYPES,
    AuditRecord,
    BulkDataStorage,
    LambdaEvent,
    Message,
    PresignedPost,
    Source,
    getLogger,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient
    from mypy_boto3_sqs.type_defs import (
        DeleteMessageBatchRequestEntryTypeDef,
        MessageAttributeValueTypeDef,
        SendMessageBatchRequestEntryTypeDef,
    )
else:
    DeleteMessageBatchRequestEntryTypeDef = dict
    MessageAttributeValueTypeDef = dict
    SendMessageBatchRequestEntryTypeDef = dict
    SQSClient = object


class __AuditRecordQueue(Queue):
    def __init__(self, message_type: str, node: Node) -> None:
        super().__init__()

        def sender() -> None:
            while True:
                audit_records: list[AuditRecord] = list()
                while len(audit_records) < 500:
                    try:
                        audit_records.append(self.__get())
                    except Empty:
                        break
                if not audit_records:
                    continue
                try:
                    node.gql_client.execute(
                        _CREATE_AUDIT_RECORDS,
                        variable_values=dict(
                            name=node.name,
                            tenant=node.tenant,
                            messageType=message_type,
                            auditRecords=[
                                dict(
                                    attributes=json.dumps(
                                        audit_record["attributes"],
                                        separators=(",", ":"),
                                    ),
                                    datetime=audit_record["datetime"].isoformat(),
                                    previousTrackingIds=audit_record[
                                        "previousTrackingIds"
                                    ],
                                    sourceNode=audit_record["sourceNode"],
                                    trackingId=audit_record["trackingId"],
                                )
                                for audit_record in audit_records
                            ],
                        ),
                    )
                except Exception:
                    getLogger().exception("Error creating audit records")
                finally:
                    for _ in range(len(audit_records)):
                        self.task_done()

        Thread(daemon=True, name="AuditRecordsSender", target=sender).start()

    def __get(self) -> AuditRecord:
        return self.get(block=True, timeout=0.1)


class __BulkDataStorage(BulkDataStorage):
    def __init__(self, bulk_data_storage: dict[str, Union[str, PresignedPost]]) -> None:
        super().__init__(bulk_data_storage)

    def handle_bulk_data(self, data: Union[bytearray, bytes, BinaryIO]) -> str:
        if isinstance(data, BinaryIO):
            data = data.read()
        with BytesIO() as buffer:
            with GzipFile(mode="wb", fileobj=buffer) as gzf:
                gzf.write(data)
            buffer.seek(0)
            post(
                self.presignedPost["url"],
                data=self.presignedPost["fields"],
                files=dict(file=("bulk_data", buffer)),
            ).raise_for_status()
        return self.presignedGet


class __BulkDataStorageQueue(SimpleQueue):
    def __init__(self, node: Node) -> None:
        super().__init__()
        self.fill = Condition()

        def filler() -> None:
            while True:
                if not self.empty():
                    with self.fill:
                        self.fill.wait()
                bulk_data_storages = node.gql_client.execute(
                    _GET_BULK_DATA_STORAGE_GQL,
                    variable_values={"tenant", node.tenant},
                )
                for bulk_data_storage in bulk_data_storages:
                    self.put(__BulkDataStorage(bulk_data_storage))

        Thread(daemon=True, name="BulkDataStorageQueueFiller", target=filler).start()

    def get(self) -> __BulkDataStorage:
        with self.fill:
            if self.qsize() < 20:
                self.fill.notify()
        bulk_data_storage: __BulkDataStorage = super().get()
        return self.get() if bulk_data_storage.expired else bulk_data_storage


class __DynamicAuthRequestsHTTPTransport(RequestsHTTPTransport):
    def __init__(self, cognito: Cognito, url: str, **kwargs: Any) -> None:
        self._cognito = cognito
        super().__init__(url, **kwargs)

    def __getattribute__(self, name: str) -> Any:
        if name == "headers":
            self._cognito.check_token()
            return dict(Authorization=self._cognito.access_token)
        return super().__getattribute__(name)


class __NodeSession(BotocoreSession):
    def __init__(self, node: Node, duration: int = 900) -> None:
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


class __TargetMessageQueue(Queue):
    def __init__(self, node: Node, queue: str, target: str) -> None:
        super().__init__()

        def batcher() -> Iterator[list[SendMessageBatchRequestEntryTypeDef]]:
            batch: list[SendMessageBatchRequestEntryTypeDef] = list()
            batch_length = 0
            id = 0
            while True:
                try:
                    message = self.__get()
                    if "ReceiptHandle" in message:
                        del message["ReceiptHandle"]
                    entry = SendMessageBatchRequestEntryTypeDef(Id=str(id), **message)
                    id += 1
                    entry_length = len(entry["MessageBody"])
                    for name, attribute in entry.get("MessageAttributes", {}):
                        value = attribute[
                            "StringValue"
                            if (data_type := attribute["DataType"])
                            in ("String", "Number")
                            else "BinaryValue"
                        ]
                        entry_length += len(name) + len(data_type) + len(value)
                    if entry_length > 262144:
                        raise ValueError(f"Message is > 262,144 in size")
                    if batch_length + entry_length > 262144 or len(batch) == 10:
                        yield batch
                        batch = list()
                        batch_length = 0
                        id = 0
                    batch.append(entry)
                    batch_length += entry_length
                except Empty:
                    if batch:
                        yield batch
                    batch = list()
                    batch_length = 0
                    id = 0

        def sender() -> None:
            for entries in batcher():
                try:
                    response = node.sqs_client.send_message_batch(
                        Entries=entries, QueueUrl=queue
                    )
                    for failed in response["Failed"]:
                        id = failed.pop("Id")
                        getLogger().error(
                            f"Unable to send message {entries[id]} to {target}, reason {failed}"
                        )
                except Exception:
                    getLogger().exception(f"Error sending messages to {target}")
                finally:
                    for _ in range(len(entries)):
                        self.task_done()

        Thread(
            daemon=True, name=f"TargetMessageSender({target})", target=sender
        ).start()

    def __get(self) -> Message:
        return self.get(block=True, timeout=0.1)


class Node:
    def __init__(
        self,
        *,
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
            transport=__DynamicAuthRequestsHTTPTransport(
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
                    executor.submit(self.__initialize_edges),
                    executor.submit(self.__initialize_message_types),
                ]
            )
        self._bulk_data_storage_queue = __BulkDataStorageQueue(self)
        self._receive_audit_records_queue = __AuditRecordQueue(
            self.receive_message_type, self
        )
        self._send_audit_records_queue = __AuditRecordQueue(
            self.send_message_type, self
        )
        atexit.register(self.join)

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

    def __initialize_edges(self) -> None:
        data: dict[str, list[dict[str, Union[str, dict]]]] = self.gql_client.execute(
            _GET_EDGES,
            variable_values=dict(name=self.name, tenant=self.tenant),
        )
        self.__sources = frozenset(
            {
                Source(queue=edge["queue"], name=edge["source"]["name"])
                for edge in data["receiveEdges"]
            }
        )
        self.__targets = frozenset(
            {edge["target"]["name"] for edge in data["sendEdges"]}
        )
        self.__target_message_queues = {
            edge["target"]["name"]: __TargetMessageQueue(self, edge["queue"])
            for edge in data["sendEdges"]
        }

    def __initialize_message_types(self) -> None:
        data: dict[str, Any] = self.gql_client.execute(
            _GET_MESSAGE_TYPES,
            variable_values=dict(name=self.name, tenant=self.tenant),
        )["GetNode"]
        if receive_message_type := data.get("receiveMessageType"):
            self.__receive_message_type = receive_message_type["name"]
            self.__receive_message_auditor = dynamic_function_loader.load(
                receive_message_type["auditor"]
            )
        if send_message_type := data.get("sendMessageType"):
            self.__send_message_type = send_message_type["name"]
            self.__send_message_auditor = dynamic_function_loader.load(
                send_message_type["auditor"]
            )

    @property
    def _sources(self) -> frozenset[Source]:
        return self.__sources

    @property
    def app(self) -> str:
        return self.__app

    @property
    def app_type(self) -> str:
        return self.__app_type

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    def create_audit_record(
        self,
        /,
        message: Message,
        *,
        auditor: Literal["receive", "send"],
        source: str = None,
    ) -> None:
        audit_records_queue: Queue = None
        if auditor == "receive":
            auditor: Callable[..., dict[str, Any]] = self.receive_message_auditor
            audit_records_queue = self._receive_audit_records_queue
        elif auditor == "send":
            auditor: Callable[..., dict[str, Any]] = self.send_message_auditor
            audit_records_queue = self._send_audit_records_queue
        else:
            raise ValueError(f"auditor must be either receive or send")
        audit_record = AuditRecord(
            attributes=auditor(message=message),
            datetime=datetime.now(timezone.utc),
            trackingId=message["MessageAttributes"]["trackingId"]["StringValue"],
        )
        if source:
            audit_record["sourceNode"] = source
        if (
            prev_tracking_ids := message["MessageAttributes"]
            .get("prevTrackingIds", {})
            .get("StringValue")
        ):
            audit_record["previousTrackingIds"] = json.loads(prev_tracking_ids)
        audit_records_queue.put_nowait(audit_record)

    def create_message(self, body: str, *, targets: set[str] = None) -> None:
        if (len(body) + 10 + 6 + 32) > 262144:
            raise ValueError(f"Message is > 262,144 in size")
        message = Message(
            MessageAttributes=dict(
                trackingId=MessageAttributeValueTypeDef(
                    DataType="String", StringValue=uuid4().hex
                )
            ),
            MessageBody=body,
            MessageGroupId=self.name.replace(" ", "_"),
        )
        self.send_message(message, targets=targets)
        self.create_audit_record(message, auditor="send")

    @property
    def gql_client(self) -> GqlClient:
        return self.__client

    def handle_bulk_data(self, data: Union[bytearray, bytes]) -> str:
        return self._bulk_data_storage_queue.get().handle_bulk_data(data)

    def handle_received_messages(self, *, messages: list[Message], source: str) -> None:
        for message in messages:
            self.create_audit_record(message, auditor="receive", source=source)

    def join(self) -> None:
        self._receive_audit_records_queue.join()
        self._send_audit_records_queue.join()
        for target_message_queue in self.__target_message_queues:
            target_message_queue.join()

    @property
    def name(self) -> str:
        return self.__name

    @property
    def node_type(self) -> str:
        return self.__node_type

    def receive_message_auditor(self, message: str) -> dict[str, Any]:
        return self.__receive_message_auditor(message=message)

    @property
    def receive_message_type(self) -> str:
        return self.__receive_message_type

    def send_message_auditor(self, message: str) -> dict[str, Any]:
        return self.__send_message_auditor(message=message)

    @property
    def send_message_type(self) -> str:
        return self.__send_message_type

    def send_message(self, /, message: Message, *, targets: set[str] = None) -> None:
        self.send_messages([message], targets=targets)

    def send_messages(
        self, /, messages: list[Message], *, targets: set[str] = None
    ) -> None:
        if messages:
            for target in targets or self.targets:
                for message in messages:
                    self.__target_message_queues[target].put_nowait(deepcopy(message))

    @property
    def sqs_client(self) -> SQSClient:
        return self.__sqs_client

    @property
    def targets(self) -> frozenset[str]:
        return self.__targets

    @property
    def tenant(self) -> str:
        return self.__tenant


class __AppNodeReceiver(Thread):
    def __init__(self, node: Node, queue: str, source: str) -> None:
        super().__init__(daemon=True, name=f"AppNodeReceiver({source})")
        self.__continue = Event()
        self.__continue.set()
        self.__delete_message_queue = __DeleteMessageQueue(node, queue, source)
        self.__node = node
        self.__queue = queue
        self.__source = source

    def join(self) -> None:
        self.__delete_message_queue.join()
        super().join()

    def run(self) -> None:
        getLogger().info(f"Receiving messages from {self.__source}")
        while self.__continue.is_set():
            try:
                response = self.__node.sqs_client.receive_message(
                    AttributeNames=["All"],
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=["All"],
                    QueueUrl=self.__queue,
                    WaitTimeSeconds=20,
                )
            except Exception:
                getLogger().exception(f"Error receiving messages from {self.__source}")
            else:
                if not (self.__continue.is_set() and response["Messages"]):
                    continue
                getLogger().info(f"Received {len(messages)} from {self.__source}")
                messages: list[Message] = list()
                for message in response["Messages"]:
                    messages.append(
                        Message(
                            MessageAttributes=message["MessageAttributes"],
                            MessageBody=message["Body"],
                            MessageDeduplicationId=message["Attributes"][
                                "MessageDeduplicationId"
                            ],
                            MessageGroupId=message["Attributes"]["MessageGroupId"],
                            ReceiptHandle=message["ReceiptHandle"],
                        )
                    )
                try:
                    self.__node.handle_received_messages(
                        messages=messages, source=self.__source
                    )
                except Exception:
                    getLogger().exception(
                        f"Error handling recevied messages for {self.__source}"
                    )
                else:
                    for message in messages:
                        self.__delete_message_queue.put_nowait(message["ReceiptHandle"])
        getLogger().info(f"Stopping receiving messages from {self.__source}")

    def stop(self) -> None:
        self.__continue.clear()


class __DeleteMessageQueue(Queue):
    def __init__(self, node: Node, queue: str, source: str) -> None:
        super().__init__()

        def deleter() -> None:
            while True:
                receipt_handles: list[str] = list()
                while len(receipt_handles) < 10:
                    try:
                        receipt_handles.append(self.__get())
                    except Empty:
                        break
                if not receipt_handles:
                    continue
                try:
                    response = node.sqs_client.delete_message_batch(
                        QueueUrl=queue,
                        Entries=[
                            DeleteMessageBatchRequestEntryTypeDef(
                                Id=str(id), ReceiptHandle=receipt_handle
                            )
                            for id, receipt_handle in enumerate(receipt_handles)
                        ],
                    )
                    for failed in response["Failed"]:
                        id = failed.pop("Id")
                        getLogger().error(
                            f"Unable to delete message {receipt_handles[id]} from {source}, reason {failed}"
                        )
                except Exception:
                    getLogger().exception(f"Error deleting messages from {source}")
                finally:
                    for _ in range(len(receipt_handles)):
                        self.task_done()

        Thread(
            daemon=True, name=f"SourceMessageDeleter({source})", target=deleter
        ).start()

    def __get(self) -> str:
        return self.get(block=True, timeout=0.1)


class AppNode(Node):
    def __init__(
        self,
        *,
        appsync_endpoint: str = None,
        client_id: str = None,
        name: str = None,
        password: str = None,
        tenant: str = None,
        user_pool_id: str = None,
        username: str = None,
    ) -> None:
        super().__init__(
            appsync_endpoint=appsync_endpoint,
            client_id=client_id,
            name=name,
            password=password,
            tenant=tenant,
            user_pool_id=user_pool_id,
            username=username,
        )
        self.__app_node_receivers: list[__AppNodeReceiver] = list()
        for edge in self._sources:
            app_node_receiver = __AppNodeReceiver(self, edge.queue, edge.name)
            self.__app_node_receivers.append(app_node_receiver)
            app_node_receiver.start()

    def join(self) -> None:
        for app_node_receiver in self.__app_node_receivers:
            app_node_receiver.join()
        super().join()

    def shutdown(self):
        for app_node_receiver in self.__app_node_receivers:
            app_node_receiver.stop()


class LambdaNode(Node):
    def __init__(
        self,
        *,
        appsync_endpoint: str = None,
        client_id: str = None,
        name: str = None,
        password: str = None,
        tenant: str = None,
        user_pool_id: str = None,
        username: str = None,
    ) -> None:
        super().__init__(
            appsync_endpoint=appsync_endpoint,
            client_id=client_id,
            name=name,
            password=password,
            tenant=tenant,
            user_pool_id=user_pool_id,
            username=username,
        )
        self.__queue_name_to_source = {
            edge.queue.split("/")[-1:][0]: edge.name for edge in self._sources
        }

    def _get_source(self, queue_arn: str) -> str:
        return self.__queue_name_to_source[queue_arn.split(":")[-1:][0]]

    def handle_event(self, event: LambdaEvent) -> None:
        records: list[
            dict[
                str,
                Union[
                    str,
                    dict[str, str],
                    dict[
                        str,
                        dict[str, dict[str, Union[str, bytes, list[str], list[bytes]]]],
                    ],
                ],
            ]
        ] = None
        if not (records := event.get("Records")):
            getLogger().warning(f"No Records found in event {event}")
            return
        source: str = None
        messages: list[Message] = list()
        for record in records:
            if not source:
                source = self._get_source(record["eventSourceARN"])
                getLogger().info(f"Received {len(records)} messages from {source}")
            message = Message(
                MessageAttributes={
                    k1: {k2.capitalize(): v2 for k2, v2 in v1.items()}
                    for k1, v1 in record["messageAttributes"].items()
                },
                MessageBody=record["body"],
                MessageGroupId=record["attributes"]["MessageGroupId"],
                ReceiptHandle=record["receiptHandle"],
            )
            if "trackingId" not in message["MessageAttributes"]:
                message["MessageAttributes"][
                    "trackingId"
                ] = MessageAttributeValueTypeDef(
                    DataType="String", StringValue=uuid4().hex
                )
            messages.append(message)

        try:
            if messages:
                self.handle_received_messages(messages=messages, source=source)
        finally:
            self.join()
