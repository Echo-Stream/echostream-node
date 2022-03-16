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

## Concurrent vs Sequential Message Processing
By default, all Nodes created using the package will process messages sequentially.
This is normally the behavior that you want, as many messaging protocols require
guaranteed ordering and therefore sequential processing within your Nodes. If this is
the behavior that you require, nothign special is needed to gain it from `echostream-node`.

However, there are use cases where message ordering is not important but processing speed is.
In these cases, you may configure your Node upon creation to concurrently process the messages
that it receives.

### Making a Threading Application Node Concurrent
If your Node inherits from the `echostream_node.threading.AppNode` class you can achieve concurrency
using either threading or multi-processing. The former is appropriate if your processing is IO bound
or your execution platform does not support shared memory (required for multi-processing). The latter
is appropriate if your platform supports shared memory *and* your processing is CPU bound.

#### Creating A Concurrent Application Node Using Threading

This will create an AppNode that uses the provided `ThreadPoolExecutor` to concurrently
process received `Message`s. Note that while you can set the maximum number of workers to
less than 10, there is no gain to setting it to more than 10 since Nodes will only process
up to 10 messages at a time.

```python
from concurrent.futures import ThreadPoolExecutor

from echostream_node import Message
from echostream_node.threading import AppNode

class MyExternalNode(AppNode):

    def __init__(self) -> None:
        super().__init__(executor=ThreadPoolExecutor(max_workers=10))

    def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
```

#### Creating A Concurrent Application Node Using Multi-Processing

This will create an AppNode that uses the provided `ProcessPoolExecutor` to concurrently
process received `Message`s. Note that while you can set the maximum number of workers to
less than 10, there is no gain to setting it to more than 10 since Nodes will only process
up to 10 messages at a time.

```python
from concurrent.futures import ProcessPoolExecutor

from echostream_node import Message
from echostream_node.threading import AppNode

class MyExternalNode(AppNode):

    def __init__(self) -> None:
        super().__init__(executor=ProcessPoolExecutor(max_workers=10))

    def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
```
### Making a Asyncio Application Node Concurrent
If your Node inherits from the `echostream_node.asyncio.Node` you can set the Node to
process incoming `Message`s concurrently. There is no setting for the maximum number of tasks;
a task is created per received `Message`.

```python
import asyncio

from echostream_node import Message
from echostream_node.asyncio import Node

class MyExternalNode(Node):

    def __init__(self) -> None:
        super().__init__(concurrent_processing=True)

    async def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
```

### Making a Lambda Node Concurrent
The AWS Lambda platform does not support shared memory, and therefore will only support concurrency
via threading. This will create a LambdaNode that uses an optimized (to your Lambda function's resources)
`ThreadPoolExecutor` to concurrently process received `Message`s.

```python
from echostream_node import Message
from echostream_node.threading import LambdaNode

class MyExternalNode(LambdaNode):

    def __init__(self) -> None:
        super().__init__(concurrent_processing=True)

    def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
```

## Lambda Nodes and Partial Success Reporting
When you connect an Edge's SQS Queue to the AWS Lambda function implementing your
Lambda Node, you can choose to Report Batch Item Failures. This allows your Lambda Node
to report partial success back to the SQS Queue, but it does require that your Lambda Node
operate differently.

If you wish to take advantage of this, set `report_batch_item_failures` when you create your
Lambda Node. This can be set even if your Node is *not* concurrent processing.

```python
from echostream_node import Message
from echostream_node.threading import LambdaNode

class MyExternalNode(LambdaNode):

    def __init__(self) -> None:
        super().__init__(report_batch_item_failures=True)

    def handle_received_message(self, *, message: Message, source: str) -> None:
        print(f"Got a message:\n{message.body}")
        self.audit_message(message, source=source)
```
