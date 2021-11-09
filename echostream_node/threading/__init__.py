from __future__ import annotations

import json
from gzip import GzipFile
from io import BytesIO
from queue import Empty, Queue
from threading import Condition, Event, Thread
from time import sleep
from typing import TYPE_CHECKING, Any, BinaryIO, Generator, Union
from uuid import uuid4

import dynamic_function_loader
from botocore.exceptions import ClientError
from gql.transport.requests import RequestsHTTPTransport
from pycognito import Cognito
from requests import post

from .. import (
    _CREATE_AUDIT_RECORDS,
    _GET_BULK_DATA_STORAGE_GQL,
    _GET_NODE_GQL,
    getLogger,
    AuditRecord,
    BulkDataStorage as BaseBulkDataStorage,
    Edge,
    LambdaEvent,
    Message,
    Node as BaseNode,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import (
        DeleteMessageBatchRequestEntryTypeDef,
        SendMessageBatchRequestEntryTypeDef,
    )
else:
    DeleteMessageBatchRequestEntryTypeDef = dict
    SendMessageBatchRequestEntryTypeDef = dict


class _AuditRecordQueue(Queue):
    def __init__(self, message_type: str, node: Node) -> None:
        super().__init__()
        self.__continue = Event()
        self.__continue.set()

        def sender() -> None:
            while self.__continue.is_set() or not self.empty():
                batch: list[AuditRecord] = list()
                while len(batch) < 500:
                    try:
                        batch.append(self.get(timeout=0.1))
                    except Empty:
                        break
                if not batch:
                    continue
                try:
                    node._gql_client.execute(
                        _CREATE_AUDIT_RECORDS,
                        variable_values=dict(
                            name=node.name,
                            tenant=node.tenant,
                            messageType=message_type,
                            auditRecords=[
                                dict(
                                    attributes=json.dumps(
                                        audit_record.attributes,
                                        separators=(",", ":"),
                                    ),
                                    datetime=audit_record.date_time.isoformat(),
                                    previousTrackingIds=list(
                                        audit_record.previous_tracking_ids
                                    ),
                                    sourceNode=audit_record.source_node,
                                    trackingId=audit_record.tracking_id,
                                )
                                for audit_record in batch
                            ],
                        ),
                    )
                except Exception:
                    getLogger().exception("Error creating audit records")
                finally:
                    for _ in range(len(batch)):
                        self.task_done()

        Thread(daemon=True, name=f"AuditRecordsSender", target=sender).start()

    def get(self, block: bool = True, timeout: float = None) -> AuditRecord:
        return self.get(block=block, timeout=timeout)

    def stop(self) -> None:
        self.__continue.clear()


class _BulkDataStorage(BaseBulkDataStorage):
    def handle_bulk_data(self, data: Union[bytearray, bytes, BinaryIO]) -> str:
        if isinstance(data, BinaryIO):
            data = data.read()
        with BytesIO() as buffer:
            with GzipFile(mode="wb", fileobj=buffer) as gzf:
                gzf.write(data)
            buffer.seek(0)
            post(
                self.presigned_post.url,
                data=self.presigned_post.fields,
                files=dict(file=("bulk_data", buffer)),
            ).raise_for_status()
        return self.presigned_get


class _BulkDataStorageQueue(Queue):
    def __init__(self, node: Node) -> None:
        super().__init__()
        self.fill = Condition()

        def filler() -> None:
            while True:
                if not self.empty():
                    with self.fill:
                        self.fill.wait()
                bulk_data_storages = node._gql_client.execute(
                    _GET_BULK_DATA_STORAGE_GQL,
                    variable_values={"tenant", node.tenant},
                )
                for bulk_data_storage in bulk_data_storages:
                    self.put(_BulkDataStorage(bulk_data_storage))

        Thread(daemon=True, name="BulkDataStorageQueueFiller", target=filler).start()

    def get(self, block: bool = True, timeout: float = None) -> _BulkDataStorage:
        with self.fill:
            if self.qsize() < 20:
                self.fill.notify()
        bulk_data_storage: _BulkDataStorage = super().get(block=block, timeout=timeout)
        return (
            self.get(block=block, timeout=timeout)
            if bulk_data_storage.expired
            else bulk_data_storage
        )


class _TargetMessageQueue(Queue):
    def __init__(self, node: Node, edge: Edge) -> None:
        super().__init__()
        self.__continue = Event()
        self.__continue.set()

        def batcher() -> Generator[
            list[SendMessageBatchRequestEntryTypeDef],
            None,
            list[SendMessageBatchRequestEntryTypeDef],
        ]:
            batch: list[SendMessageBatchRequestEntryTypeDef] = list()
            batch_length = 0
            id = 0
            while self.__continue.is_set() or not self.empty():
                try:
                    message = self.get(timeout=0.1)
                    if batch_length + message.length > 262144:
                        yield batch
                        batch = list()
                        batch_length = 0
                        id = 0
                    batch.append(
                        SendMessageBatchRequestEntryTypeDef(
                            Id=str(id), **message.sqs_message
                        )
                    )
                    if len(batch) == 10:
                        yield batch
                        batch = list()
                        batch_length = 0
                        id = 0
                    id += 1
                    batch_length += message.length
                except Empty:
                    if batch:
                        yield batch
                    batch = list()
                    batch_length = 0
                    id = 0
            if batch:
                return batch

        def sender() -> None:
            for entries in batcher():
                try:
                    response = node._sqs_client.send_message_batch(
                        Entries=entries, QueueUrl=edge.queue
                    )
                    for failed in response["Failed"]:
                        id = failed.pop("Id")
                        getLogger().error(
                            f"Unable to send message {entries[id]} to {edge.name}, reason {failed}"
                        )
                except Exception:
                    getLogger().exception(f"Error sending messages to {edge.name}")
                finally:
                    for _ in range(len(entries)):
                        self.task_done()

        Thread(
            daemon=True, name=f"TargetMessageSender({edge.name})", target=sender
        ).start()

    def get(self, block: bool = True, timeout: float = None) -> Message:
        return super().get(block=block, timeout=timeout)

    def stop(self) -> None:
        self.__continue.clear()


class CognitoRequestsHTTPTransport(RequestsHTTPTransport):
    def __init__(self, cognito: Cognito, url: str, **kwargs: Any) -> None:
        self._cognito = cognito
        super().__init__(url, **kwargs)

    def __getattribute__(self, name: str) -> Any:
        if name == "headers":
            self._cognito.check_token()
            return dict(Authorization=self._cognito.access_token)
        return super().__getattribute__(name)


class Node(BaseNode):
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
            gql_transport_cls=CognitoRequestsHTTPTransport,
            appsync_endpoint=appsync_endpoint,
            client_id=client_id,
            name=name,
            password=password,
            tenant=tenant,
            user_pool_id=user_pool_id,
            username=username,
        )
        self.__bulk_data_storage_queue = _BulkDataStorageQueue(self)
        self.__audit_records_queue: _AuditRecordQueue = None
        self.__stop = Event()
        self.__target_message_queues: dict[str, _TargetMessageQueue] = dict()

    def handle_bulk_data(self, data: Union[bytearray, bytes]) -> str:
        return self.__bulk_data_storage_queue.get().handle_bulk_data(data)

    def handle_received_message(self, *, message: Message, source: str) -> None:
        pass

    def join(self) -> None:
        self.__stop.wait()
        for target_message_queue in self.__target_message_queues.values():
            target_message_queue.join()
        if self.__audit_records_queue:
            self.__audit_records_queue.join()

    def put_audit_record(self, audit_record: AuditRecord) -> None:
        self.__audit_records_queue.put_nowait(audit_record)

    def send_message(self, /, message: Message, *, targets: set[str] = None) -> None:
        self.send_messages([message], targets=targets)

    def send_messages(
        self, /, messages: list[Message], *, targets: set[str] = None
    ) -> None:
        if messages:
            for target in targets or self._targets:
                target_message_queue = self.__target_message_queues[target]
                for message in messages:
                    target_message_queue.put_nowait(message)

    def start(self) -> None:
        self.__stop.clear()
        data: dict[str, dict] = self._gql_client.execute(
            _GET_NODE_GQL,
            variable_values=dict(name=self.name, tenant=self.tenant),
        )["GetNode"]
        self.config = (
            json.loads(data["tenant"].get("config", {}))
            | json.loads(data["app"].get("config", {}))
            | json.loads(data.get("config", {}))
        )
        if receive_message_type := data.get("receiveMessageType"):
            self._receive_message_type = receive_message_type["name"]
            self._receive_message_auditor = dynamic_function_loader.load(
                receive_message_type["auditor"]
            )
        if send_message_type := data.get("sendMessageType"):
            self._send_message_type = send_message_type["name"]
            self._send_message_auditor = dynamic_function_loader.load(
                send_message_type["auditor"]
            )
        self._sources = {
            Edge(name=edge["source"]["name"], queue=edge["queue"])
            for edge in data["receiveEdges"]
        }
        self._targets = {
            Edge(name=edge["target"]["name"], queue=edge["queue"])
            for edge in data["sendEdges"]
        }
        self.__audit_records_queue = _AuditRecordQueue(self._receive_message_type, self)
        self.__target_message_queues = {
            edge.name: _TargetMessageQueue(self, edge) for edge in self._targets
        }

    def stop(self) -> None:
        for target_message_queue in self.__target_message_queues.values():
            target_message_queue.stop()
        self.__audit_records_queue.stop()
        self.__stop.set()


class _DeleteMessageQueue(Queue):
    def __init__(self, edge: Edge, node: Node) -> None:
        super().__init__()
        self.__continue = Event()
        self.__continue.set()

        def deleter() -> None:
            while self.__continue.is_set() or not self.empty():
                receipt_handles: list[str] = list()
                while len(receipt_handles) < 10:
                    try:
                        receipt_handles.append(self.get(timeout=0.1))
                    except Empty:
                        break
                if not receipt_handles:
                    continue
                try:
                    response = node._sqs_client.delete_message_batch(
                        Entries=[
                            DeleteMessageBatchRequestEntryTypeDef(
                                Id=str(id), ReceiptHandle=receipt_handle
                            )
                            for id, receipt_handle in enumerate(receipt_handles)
                        ],
                        QueueUrl=edge.queue,
                    )
                    for failed in response["Failed"]:
                        id = failed.pop("Id")
                        getLogger().error(
                            f"Unable to delete message {receipt_handles[id]} from {edge.name}, reason {failed}"
                        )
                except Exception:
                    getLogger().exception(f"Error deleting messages from {edge.name}")
                finally:
                    for _ in range(len(receipt_handles)):
                        self.task_done()

        Thread(
            daemon=True, name=f"SourceMessageDeleter({edge.name})", target=deleter
        ).start()

    def get(self, block: bool = True, timeout: float = None) -> str:
        return self.get(block=block, timeout=timeout)

    def stop(self) -> None:
        self.__continue.clear()


class _SourceMessageReceiver(Thread):
    def __init__(self, edge: Edge, node: Node) -> None:
        self.__continue = Event()
        self.__continue.set()
        self.__delete_message_queue = _DeleteMessageQueue(edge, node)

        def receive() -> None:
            self.__continue.wait()
            getLogger().info(f"Receiving messages from {edge.name}")
            error_count = 0
            while self.__continue.is_set():
                try:
                    response = node._sqs_client.receive_message(
                        AttributeNames=["All"],
                        MaxNumberOfMessages=10,
                        MessageAttributeNames=["All"],
                        QueueUrl=edge.queue,
                        WaitTimeSeconds=20,
                    )
                    error_count = 0
                except Exception as e:
                    if (
                        isinstance(e, ClientError)
                        and e.response["Error"]["Code"]
                        == "AWS.SimpleQueueService.NonExistentQueue"
                    ):
                        getLogger().warning(
                            f"Queue {edge.queue} does not exist, exiting"
                        )
                        break
                    error_count += 1
                    if error_count == 10:
                        getLogger().critical(
                            f"Recevied 10 errors in a row trying to receive from {edge.queue}, exiting"
                        )
                        raise e
                    else:
                        getLogger().exception(
                            f"Error receiving messages from {edge.name}, retrying"
                        )
                        sleep(10)
                else:
                    sqs_messages = response["Messages"]
                    if not (self.__continue.is_set() and sqs_messages):
                        continue
                    getLogger().info(f"Received {len(sqs_messages)} from {edge.name}")
                    for sqs_message in sqs_messages:
                        message = Message(
                            body=sqs_message["Body"],
                            group_id=sqs_message["Attributes"]["MessageGroupId"],
                            tracking_id=sqs_message["MessageAttributes"]
                            .get("trackingId", {})
                            .get("StringValue"),
                            previous_tracking_ids=sqs_message["MessageAttributes"]
                            .get("prevTrackingIds", {})
                            .get("StringValue"),
                        )
                        receipt_handle = sqs_message["ReceiptHandle"]
                        try:
                            node.handle_received_message(
                                message=message, source=edge.name
                            )
                        except Exception:
                            getLogger().exception(
                                f"Error handling recevied message for {edge.name}"
                            )
                        self.__delete_message_queue.put_nowait(receipt_handle)
            getLogger().info(f"Stopping receiving messages from {edge.name}")

        super().__init__(
            daemon=True, name=f"SourceMessageReceiver({edge.name})", target=receive
        )
        self.start()

    def join(self) -> None:
        super().join()
        self.__delete_message_queue.join()

    def stop(self) -> None:
        self.__continue.clear()
        self.__delete_message_queue.stop()


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
        self.__source_message_receivers: list[_SourceMessageReceiver] = list()

    def join(self) -> None:
        for app_node_receiver in self.__source_message_receivers:
            app_node_receiver.join()
        super().join()

    def start(self) -> None:
        super().start()
        self.__source_message_receivers = [
            _SourceMessageReceiver(edge, self) for edge in self._sources
        ]

    def stop(self) -> None:
        for app_node_receiver in self.__source_message_receivers:
            app_node_receiver.stop()
        super().stop()


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
        super().start()
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
        try:
            for record in records:
                if not source:
                    source = self._get_source(record["eventSourceARN"])
                    getLogger().info(f"Received {len(records)} messages from {source}")
                message = Message(
                    body=record["body"],
                    group_id=record["attributes"]["MessageGroupId"],
                    previous_tracking_ids=record["messageAttributes"]
                    .get("prevTrackingIds", {})
                    .get("stringValue"),
                    tracking_id=record["messageAttributes"]
                    .get("trackingId", {})
                    .get("stringValue")
                    or uuid4().hex,
                )
                self.handle_received_message(message=message, source=source)
        finally:
            self.join()
