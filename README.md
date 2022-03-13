# echostream-node

EchoStream library for implementing remote nodes that can be used in the echostream system.

## Installation
```bash
pip install echostream-node
```


## Application Node
Implementation of an external application node running in EchoStream system

```python
import asyncio
from echostream_node.asyncio import Node

class ReceiveNode(Node):
    def __init__(self) -> None:
        super().__init__()
        
    async def handle_received_message(self, *, message: Message, source: str) -> None:
        ## Process the message received to the node here
    
    async def join(self) -> None:
        await super().join()
    
     async def start(self) -> None:
        await super().start()
        
async def main(node: Node) -> None:
    try:
        await node.start_and_run_forever()
    except asyncio.CancelledError:
        pass
    except Exception:
        print("Error running node")

if __name__ == "__main__":
    aiorun.run(
        main(ReceiveNode()) #, stop_on_unhandled_errors=True, use_uvloop=True
    )
```

## LambdaNode
To be used in an implementation where the compute resource is AWS Lambda
```python
from echostream_node.threading import LambdaNode


Class ProcessorNode(LambdaNode):
    def handle_received_message(self, *, message: Message, source: str) -> None:
        ## Process the message received to the node here

PROCESSOR_NODE = ProcessorNode()

def handler(event, _):
    try:
        PROCESSOR_NODE.handle_event(event)
    except Exception:
        print(f"Error handling event:\n{json.dumps(event, indent=4)}")
```

