from __future__ import annotations

import asyncio
from typing import Any
from pycognito import Cognito
from os import environ
from gql import Client as GqlClient
from gql.transport.aiohttp import AIOHTTPTransport
from boto3 import Session
from botocore.credentials import DeferredRefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import Session as BotocoreSession

from .. import (
    _GET_APP_GQL,
    _CREATE_AUDIT_RECORDS,
    _GET_BULK_DATA_STORAGE_GQL,
    _GET_CONFIG_GQL,
    _GET_CREDENTIALS_GQL,
    _GET_EDGES,
    _GET_MESSAGE_TYPES,
)


class __DynamicAuthAIOHTTPTransport(AIOHTTPTransport):
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
            transport=__DynamicAuthAIOHTTPTransport(
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
