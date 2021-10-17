from __future__ import annotations

import json
from gzip import GzipFile
from io import BytesIO
from queue import Empty, Queue, SimpleQueue
from threading import Condition, Event, RLock, Thread
from time import sleep
from typing import TYPE_CHECKING, Any, BinaryIO, Iterator, Union
from uuid import uuid4

from botocore.exceptions import ClientError
from gql.transport.requests import RequestsHTTPTransport
from pycognito import Cognito
from requests import post

from .. import (
    _CREATE_AUDIT_RECORDS,
    _GET_BULK_DATA_STORAGE_GQL,
    AuditRecord,
    BaseNode,
    BulkDataStorage,
    LambdaEvent,
    Message,
    PresignedPost,
    getLogger,
)

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import (
        DeleteMessageBatchRequestEntryTypeDef,
        SendMessageBatchRequestEntryTypeDef,
    )
else:
    DeleteMessageBatchRequestEntryTypeDef = dict
    SendMessageBatchRequestEntryTypeDef = dict


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
                self.presigned_post.url,
                data=self.presigned_post.fields,
                files=dict(file=("bulk_data", buffer)),
            ).raise_for_status()
        return self.presigned_get


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
                    if batch_length + message.length > 262144 or len(batch) == 10:
                        yield batch
                        batch = list()
                        batch_length = 0
                        id = 0
                    batch.append(
                        SendMessageBatchRequestEntryTypeDef(
                            Id=str(id), **message.sqs_message
                        )
                    )
                    id += 1
                    batch_length += message.length
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
            gql_transport_cls=__DynamicAuthRequestsHTTPTransport,
            appsync_endpoint=appsync_endpoint,
            client_id=client_id,
            name=name,
            password=password,
            tenant=tenant,
            user_pool_id=user_pool_id,
            username=username,
        )
        self._bulk_data_storage_queue = __BulkDataStorageQueue(self)
        self._receive_audit_records_queue = __AuditRecordQueue(
            self.receive_message_type, self
        )
        self._send_audit_records_queue = __AuditRecordQueue(
            self.send_message_type, self
        )
        self._lock = RLock()
        self.__target_message_queues: dict[str, __TargetMessageQueue] = dict()

    def _initialize_edges(self) -> None:
        super()._initialize_edges()
        with self._lock:
            for target_message_queue in self.__target_message_queues.values():
                target_message_queue.join()
            self.__target_message_queues = {
                edge.name: __TargetMessageQueue(self, edge.queue)
                for edge in self.targets
            }

    def handle_bulk_data(self, data: Union[bytearray, bytes]) -> str:
        return self._bulk_data_storage_queue.get().handle_bulk_data(data)

    def handle_received_messages(self, *, messages: list[Message], source: str) -> None:
        for message in messages:
            self._receive_audit_records_queue.put_nowait(
                AuditRecord(self.receive_message_auditor, message, source)
            )

    def join(self) -> None:
        self._receive_audit_records_queue.join()
        self._send_audit_records_queue.join()
        with self._lock:
            for target_message_queue in self.__target_message_queues.values():
                target_message_queue.join()

    def send_message(self, /, message: Message, *, targets: set[str] = None) -> None:
        self.send_messages([message], targets=targets)

    def send_messages(
        self, /, messages: list[Message], *, targets: set[str] = None
    ) -> None:
        if messages:
            for target in targets or self.targets:
                with self._lock:
                    target_message_queue = self.__target_message_queues[target]
                for message in messages:
                    target_message_queue.put_nowait(message)
                    self._send_audit_records_queue.put_nowait(
                        AuditRecord(self.send_message_auditor, message)
                    )


class __AppNodeReceiver(Thread):
    def __init__(self, node: Node, queue: str, source: str) -> None:
        super().__init__(daemon=True, name=f"AppNodeReceiver({source})")
        self.__continue = Event()
        self.__continue.set()
        self.__delete_message_queue = __DeleteMessageQueue(node, queue, source)
        self.__node = node
        self.__queue = queue
        self.__source = source
        self.start()

    def join(self) -> None:
        self.__delete_message_queue.join()
        super().join()

    def run(self) -> None:
        getLogger().info(f"Receiving messages from {self.__source}")
        error_count = 0
        while self.__continue.is_set():
            try:
                response = self.__node.sqs_client.receive_message(
                    AttributeNames=["All"],
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=["All"],
                    QueueUrl=self.__queue,
                    WaitTimeSeconds=20,
                )
                error_count = 0
            except Exception as e:
                if (
                    isinstance(e, ClientError)
                    and e.response["Error"]["Code"]
                    == "AWS.SimpleQueueService.NonExistentQueue"
                ):
                    getLogger().warning(f"Queue {self.__queue} does not exist, exiting")
                    break
                error_count += 1
                if error_count == 10:
                    getLogger().critical(
                        f"Recevied 10 errors in a row trying to receive from {self.__source}, exiting"
                    )
                    raise e
                else:
                    getLogger().exception(
                        f"Error receiving messages from {self.__source}, retrying"
                    )
                    sleep(10)
            else:
                if not (self.__continue.is_set() and response["Messages"]):
                    continue
                messages: list[Message] = list()
                receipt_handles: list[str] = list()
                for sqs_message in response["Messages"]:
                    messages.append(
                        Message(
                            body=sqs_message["Body"],
                            group_id=sqs_message["Attributes"]["MessageGroupId"],
                            tracking_id=sqs_message["MessageAttributes"]
                            .get("trackingId", {})
                            .get("StringValue"),
                            previous_tracking_ids=sqs_message["MessageAttributes"]
                            .get("prevTrackingIds", {})
                            .get("StringValue"),
                        )
                    )
                    receipt_handles.append(sqs_message["ReceiptHandle"])
                getLogger().info(f"Received {len(messages)} from {self.__source}")
                try:
                    self.__node.handle_received_messages(
                        messages=messages, source=self.__source
                    )
                except Exception:
                    getLogger().exception(
                        f"Error handling recevied messages for {self.__source}"
                    )
                else:
                    for receipt_handle in receipt_handles:
                        self.__delete_message_queue.put_nowait(receipt_handle)
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

    def _initialize_edges(self) -> None:
        super()._initialize_edges()
        with self._lock:
            for app_node_receiver in self.__app_node_receivers:
                app_node_receiver.stop()
            for app_node_receiver in self.__app_node_receivers:
                app_node_receiver.join()
            self.__app_node_receivers = [
                __AppNodeReceiver(self, edge.queue, edge.name) for edge in self.sources
            ]

    def join(self) -> None:
        with self._lock:
            for app_node_receiver in self.__app_node_receivers:
                app_node_receiver.join()
        super().join()

    def stop(self):
        with self._lock:
            for app_node_receiver in self.__app_node_receivers:
                app_node_receiver.stop()


class LambdaNode(Node):
    def _get_source(self, queue_arn: str) -> str:
        return self.__queue_name_to_source[queue_arn.split(":")[-1:][0]]

    def _initialize_edges(self) -> None:
        super()._initialize_edges()
        self.__queue_name_to_source = {
            edge.queue.split("/")[-1:][0]: edge.name for edge in self.sources
        }

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
            messages.append(
                Message(
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
            )
        try:
            if messages:
                self.handle_received_messages(messages=messages, source=source)
        finally:
            self.join()
