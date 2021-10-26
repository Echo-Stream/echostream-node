from __future__ import annotations

from asyncio import (
    Event,
    Queue,
    Task,
    TimeoutError,
    create_task,
    get_running_loop,
    sleep,
    wait,
    wait_for,
)
from functools import partial
from gzip import GzipFile
from io import BytesIO
from typing import TYPE_CHECKING, Any, AsyncGenerator, BinaryIO, Union

import dynamic_function_loader
import simplejson as json
from aiohttp import ClientSession, FormData
from aiohttp.helpers import get_running_loop
from botocore.exceptions import ClientError
from gql.transport.aiohttp import AIOHTTPTransport
from pycognito import Cognito

from .. import (
    _CREATE_AUDIT_RECORDS,
    _GET_BULK_DATA_STORAGE_GQL,
    _GET_NODE_GQL,
    AuditRecord,
    BulkDataStorage,
    Edge,
    Message,
)
from .. import Node as BaseNode
from .. import PresignedPost, getLogger

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
        self.__message_type = message_type
        self.__node = node
        self.__task: Task = None

    async def __batcher(
        self,
    ) -> AsyncGenerator[list[AuditRecord], None,]:
        batch: list[AuditRecord] = list()
        while not self.empty():
            try:
                batch.append(await wait_for(self.get(), timeout=0.1))
            except TimeoutError:
                if batch:
                    yield batch
                    batch = list()
            else:
                if len(batch) == 500:
                    yield batch
                    batch = list()
        if batch:
            yield batch

    async def __sender(self) -> None:
        async for batch in self.__batcher():
            try:
                async with self.__node._gql_client as session:
                    await session.execute(
                        _CREATE_AUDIT_RECORDS,
                        variable_values=dict(
                            name=self.__node.name,
                            tenant=self.__node.tenant,
                            messageType=self.__message_type,
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

    def _put(self, item: Any) -> None:
        if self.empty() and (not self.__task or self.__task.done()):
            self.__task = create_task(self.__sender(), name=f"AuditRecordsSender")
        return super()._put(item)

    async def get(self) -> AuditRecord:
        return await super().get()


class __BulkDataStorageQueue(Queue):
    def __init__(self, node: Node) -> None:
        super().__init__()
        self.__client_session = ClientSession()
        self.__node = node
        self.__filler_task: Task = None

    async def __filler(self) -> None:
        async with self.__node._gql_client as session:
            bulk_data_storages = await session.execute(
                _GET_BULK_DATA_STORAGE_GQL,
                variable_values={"tenant", self.__node.tenant},
            )
        for bulk_data_storage in bulk_data_storages:
            await self.put(__BulkDataStorage(bulk_data_storage, self.__client_session))

    async def _stop(self) -> None:
        await self.__client_session.close()

    async def get(self) -> __BulkDataStorage:
        if self.qsize() < 20:
            if not self.__filler_task or self.__filler_task.done():
                self.__filler_task = create_task(
                    self.__filler(), name="BulkDataStorageQueueFiller"
                )
        bulk_data_storage: __BulkDataStorage = await super().get()
        return await self.get() if bulk_data_storage.expired else bulk_data_storage


class __BulkDataStorage(BulkDataStorage):
    def __init__(
        self,
        bulk_data_storage: dict[str, Union[str, PresignedPost]],
        session: ClientSession,
    ) -> None:
        super().__init__(bulk_data_storage)
        self.__session = session

    async def handle_bulk_data(self, data: Union[bytearray, bytes, BinaryIO]) -> str:
        if isinstance(data, BinaryIO):
            data = data.read()
        with BytesIO() as buffer:
            with GzipFile(mode="wb", fileobj=buffer) as gzf:
                gzf.write(data)
            buffer.seek(0)
            form_data = FormData(self.presigned_post.fields)
            form_data.add_field("file", buffer, filename="bulk_data")
            async with self.__session.post(
                self.presigned_post.url, data=form_data
            ) as response:
                response.raise_for_status()
        return self.presigned_get


class __DeleteMessageQueue(Queue):
    def __init__(self, edge: Edge, node: Node) -> None:
        super().__init__()
        self.__edge = edge
        self.__node = node
        self.__task: Task = None

    async def __batcher(
        self,
    ) -> AsyncGenerator[list[str], None,]:
        batch: list[str] = list()
        while not self.empty():
            try:
                batch.append(await wait_for(self.get(), timeout=0.1))
            except TimeoutError:
                if batch:
                    yield batch
                    batch = list()
            else:
                if len(batch) == 10:
                    yield batch
                    batch = list()
        if batch:
            yield batch

    async def __deleter(self) -> None:
        loop = get_running_loop()
        async for receipt_handles in self.__batcher():
            try:
                response = await loop.run_in_executor(
                    None,
                    partial(
                        self.__node._sqs_client.delete_message_batch,
                        Entries=[
                            DeleteMessageBatchRequestEntryTypeDef(
                                Id=str(id), ReceiptHandle=receipt_handle
                            )
                            for id, receipt_handle in enumerate(receipt_handles)
                        ],
                        QueueUrl=self.__edge.queue,
                    ),
                )
                for failed in response["Failed"]:
                    id = failed.pop("Id")
                    getLogger().error(
                        f"Unable to send message {receipt_handles[id]} to {self.__edge.name}, reason {failed}"
                    )
            except Exception:
                getLogger().exception(f"Error sending messages to {self.__edge.name}")
            finally:
                for _ in range(len(receipt_handles)):
                    self.task_done()

    def _put(self, item: Any) -> None:
        if self.empty() and (not self.__task or self.__task.done()):
            self.__task = create_task(
                self.__deleter(), name=f"SourceMessageDeleter({self.__edge.name})"
            )
        return super()._put(item)

    async def get(self) -> str:
        return await super().get()


class __DynamicAuthAIOHTTPTransport(AIOHTTPTransport):
    def __init__(self, cognito: Cognito, url: str, **kwargs: Any) -> None:
        self._cognito = cognito
        super().__init__(url, **kwargs)

    def __getattribute__(self, name: str) -> Any:
        if name == "headers":
            self._cognito.check_token()
            return dict(Authorization=self._cognito.access_token)
        return super().__getattribute__(name)


class __SourceMessageReceiver:
    def __init__(self, edge: Edge, node: Node) -> None:
        self.__continue = Event()
        self.__continue.set()
        self.__delete_message_queue = __DeleteMessageQueue(edge, node)
        self.__node = node
        self.__edge = edge
        self.__task: Task = create_task(self.__receive())

    async def __receive(self) -> None:
        await self.__continue.wait()
        loop = get_running_loop()
        getLogger().info(f"Receiving messages from {self.__edge.name}")
        error_count = 0
        while self.__continue.is_set():
            try:
                response = await loop.run_in_executor(
                    None,
                    partial(
                        self.__node._sqs_client.receive_message,
                        AttributeNames=["All"],
                        MaxNumberOfMessages=10,
                        MessageAttributeNames=["All"],
                        QueueUrl=self.__edge.queue,
                        WaitTimeSeconds=20,
                    ),
                )
                error_count = 0
            except Exception as e:
                if (
                    isinstance(e, ClientError)
                    and e.response["Error"]["Code"]
                    == "AWS.SimpleQueueService.NonExistentQueue"
                ):
                    getLogger().warning(
                        f"Queue {self.__edge.queue} does not exist, exiting"
                    )
                    break
                error_count += 1
                if error_count == 10:
                    getLogger().critical(
                        f"Recevied 10 errors in a row trying to receive from {self.__edge.queue}, exiting"
                    )
                    raise e
                else:
                    getLogger().exception(
                        f"Error receiving messages from {self.__edge.name}, retrying"
                    )
                    await sleep(10)
            else:
                sqs_messages = response["Messages"]
                if not (self.__continue.is_set() and sqs_messages):
                    continue
                getLogger().info(
                    f"Received {len(sqs_messages)} from {self.__edge.name}"
                )
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
                        await self.__node.handle_received_message(
                            message=message, source=self.__edge.name
                        )
                    except Exception:
                        getLogger().exception(
                            f"Error handling recevied message for {self.__edge.name}"
                        )
                    else:
                        self.__delete_message_queue.put_nowait(receipt_handle)
        getLogger().info(f"Stopping receiving messages from {self.__edge.name}")

    async def join(self) -> None:
        await wait({self.__task})
        await self.__delete_message_queue.join()

    def stop(self) -> None:
        self.__continue.clear()


class __TargetMessageQueue(Queue):
    def __init__(self, node: Node, edge: Edge) -> None:
        self.__node = node
        self.__edge = edge
        self.__task: Task = None

    async def __batcher(
        self,
    ) -> AsyncGenerator[list[SendMessageBatchRequestEntryTypeDef], None]:
        batch: list[SendMessageBatchRequestEntryTypeDef] = list()
        batch_length = 0
        id = 0
        while not self.empty():
            try:
                message = await wait_for(self.get(), timeout=0.1)
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
            except TimeoutError:
                if batch:
                    yield batch
                batch = list()
                batch_length = 0
                id = 0
        if batch:
            yield batch

    async def __sender(self) -> None:
        loop = get_running_loop()
        async for entries in self.__batcher():
            try:
                response = await loop.run_in_executor(
                    None,
                    partial(
                        self.__node._sqs_client.send_message_batch,
                        Entries=entries,
                        QueueUrl=self.__edge.queue,
                    ),
                )
                for failed in response["Failed"]:
                    id = failed.pop("Id")
                    getLogger().error(
                        f"Unable to send message {entries[id]} to {self.__edge.name}, reason {failed}"
                    )
            except Exception:
                getLogger().exception(f"Error sending messages to {self.__edge.name}")
            finally:
                for _ in range(len(entries)):
                    self.task_done()

    def _put(self, item: Any) -> None:
        if self.empty() and (not self.__task or self.__task.done()):
            self.__task = create_task(
                self.__sender(), name=f"TargetMessageSender({self.__edge.name})"
            )
        return super()._put(item)

    async def get(self) -> Message:
        return await super().get()


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
            gql_transport_cls=__DynamicAuthAIOHTTPTransport,
            appsync_endpoint=appsync_endpoint,
            client_id=client_id,
            name=name,
            password=password,
            tenant=tenant,
            user_pool_id=user_pool_id,
            username=username,
        )
        self.__bulk_data_storage_queue: __BulkDataStorageQueue = None
        self.__audit_records_queue: __AuditRecordQueue = None
        self.__running = False
        self.__source_message_receivers: list[__SourceMessageReceiver] = list()
        self.__target_message_queues: dict[str, __TargetMessageQueue] = dict()

    async def handle_bulk_data(self, data: Union[bytearray, bytes]) -> str:
        return await (await self.__bulk_data_storage_queue.get()).handle_bulk_data(data)

    async def handle_received_message(self, *, message: Message, source: str) -> None:
        pass

    async def join(self) -> None:
        for app_node_receiver in self.__source_message_receivers:
            app_node_receiver.join()
        for target_message_queue in self.__target_message_queues.values():
            await target_message_queue.join()
        if self.__audit_records_queue:
            await self.__audit_records_queue.join()
        if not self.__running and self.__bulk_data_storage_queue:
            await self.__bulk_data_storage_queue._stop()

    def put_audit_record(self, audit_record: AuditRecord) -> None:
        self.__audit_records_queue.put_nowait(audit_record)

    async def send_message(
        self, /, message: Message, *, targets: set[str] = None
    ) -> None:
        await self.send_messages([message], targets=targets)

    async def send_messages(
        self, /, messages: list[Message], *, targets: set[str] = None
    ) -> None:
        if messages:
            for target in targets or self._targets:
                target_message_queue = self.__target_message_queues[target]
                for message in messages:
                    target_message_queue.put_nowait(message)

    async def start(self) -> None:
        self.__running = True
        self.__bulk_data_storage_queue = __BulkDataStorageQueue(self)
        async with self._gql_client as session:
            data: dict[str, dict] = await session.execute(
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
        self.__audit_records_queue = __AuditRecordQueue(
            self._receive_message_type, self
        )
        self.__target_message_queues = {
            edge.name: __TargetMessageQueue(self, edge) for edge in self._targets
        }
        self.__source_message_receivers = [
            __SourceMessageReceiver(edge, self) for edge in self._sources
        ]

    def stop(self) -> None:
        self.__running = False
        for app_node_receiver in self.__source_message_receivers:
            app_node_receiver.stop()
