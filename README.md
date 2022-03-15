# echostream-node

EchoStream library for implementing remote nodes that can be used in the echostream system.

This package supports creating External Nodes and Managed Node Types,
and supports the following EchoStream use cases:
- An External Node in an External App or Cross Account App that is a stand-alone application or part of another application, using either `threading` or `asyncio`.
- An External Node in a Cross Account App that is an AWS Lambda function. This use case only supports `threading`.
- A Managed Node Type, using either `threading` or `asyncio`

## Installation
```bash
pip install echostream-node
```

## Usage


### Threading Application Node
```python
from signal import SIGHUP, SIGINT, SIGTERM, signal, strsignal

from echostream_node import Message
from echostream_node.threading import AppNode


class MyExternalNode(AppNode):

    def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
        
    def signal_handler(self, signum: int, _: object) -> None:
        print(f"{strsignal(signum)} received, shutting down")
        self.stop()

    def start(self) -> None:
        super().start()
        signal(SIGHUP, self.signal_handler)
        signal(SIGINT, self.signal_handler)
        signal(SIGTERM, self.signal_handler)

try:
    my_external_node = MyExternalNode()
    my_external_node.start()
    for i in range(100):
        message = my_external_node.create_message(str(i))
        my_external_node.send_message(message)
        my_external_node.audit_message(message)
    my_external_node.join()
except Exception:
    print("Error running node")
```

### Asyncio Application Node
```python
import asyncio

import aiorun
from echostream_node import Message
from echostream_node.asyncio import Node

class MyExternalNode(Node):

    async def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)


async def main(node: Node) -> None:
    try:
        await node.start()
        for i in range(100):
            message = my_external_node.create_message(str(i))
            my_external_node.send_message(message)
            my_external_node.audit_message(message)
        await node.join()
    except asyncio.CancelledError:
        pass
    except Exception:
        print("Error running node")


if __name__ == "__main__":
    aiorun.run(main(MyExternalNode()), stop_on_unhandled_errors=True, use_uvloop=True)
```

### Cross Account Lambda Node
```python
from echostream_node import Message
from echostream_node.threading import LambdaNode

class MyExternalNode(LambdaNode):
    def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
        
MY_EXTERNAL_NODE = MyExternalNode()

def lambda_handler(event, context):
    MY_EXTERNAL_NODE.handle_event(event)
```
