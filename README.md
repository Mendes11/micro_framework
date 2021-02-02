# Micro Framework

(yet another) Python Microservices Framework capable of running functions in
 threads or processes. 

The code structure is inspired by Nameko code (https://github.com/nameko/nameko) 
but with some different concepts regarding the function call.

## DOCS
https://python-micro-framework.readthedocs.io/en/latest/

## Python Version Requirement

* python 3.6 or higher

## INSTALL
`pip install micro_framework`


## Usage
 To use, simply create a module to start, define your routes and done.
  
    
* python main.py
```python
import logging
from micro_framework.runner import Runner, RunnerContext
from micro_framework.routes import Route, CallbackRoute
from micro_framework.entrypoints import TimeEntrypoint
from micro_framework.amqp.entrypoints import EventListener, RPCListener
from micro_framework.retry import AsyncBackOff 
from micro_framework.websocket.entrypoints import WebSocketEntrypoint 
from tasks import test2, test, Class, websocket_task, websocket_caller, \ 
    test_failure, amqp_rpc_task, amqp_rpc_caller

config = {
    'AMQP_URI': 'amqp://guest:guest@localhost:5672',
    'MAX_WORKERS': 3,
    'SERVICE_NAME': 'my_service',
    'MAX_TASKS_PER_CHILD': 2, # currently for process WORKER_MODE only 
    'WORKER_MODE': 'process',
    'WEBSOCKET_IP': '0.0.0.0', # Default value
    'WEBSOCKET_PORT': 8765,  # Default Value
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(message)s')

contexts = [
    RunnerContext(routes=[
        CallbackRoute(
            test,
            callback_target=test_failure, # Called if error and after the backoff max_retries
            entrypoint=EventListener(
                source_service='my_exchange', event_name='my_routing_key'
            ),
            backoff=AsyncBackOff(
                max_retries=5, interval=10000, exception_list=None
            ),
        )],
        max_workers=3, # If not given, we use the config value
        worker_mode="thread", # If not given, we use the config value
        context_metric_label="context_1" # Prometheus Metric Label
    ),
    RunnerContext(routes=[
        Route(
            test2, # We also allow a direct reference instead of path.
            entrypoint=EventListener(
                source_service='my_service', event_name='event_name',
                # Only trigger the Route if this condition bellow is met.
                payload_filter=lambda payload: payload['some_key'] == 'some_value' 
            ),
        ),
        Route(
            Class,
            method_name='class_method', # If not provided, the __call__ method is called
            entrypoint=EventListener(
                source_service='my_service', event_name='event_name',
            ),
            backoff=AsyncBackOff(
                max_retries=5, interval=10000, exception_list=None
            ),
        ),
        # We support RPC  over websockets:
        Route(
            websocket_task,
            entrypoint=WebSocketEntrypoint(),
        ),
        Route(
            websocket_caller,
            entrypoint=TimeEntrypoint(10), 
        ),
        
        # But Also over AMQP
        Route(
            amqp_rpc_task,
            entrypoint=RPCListener()
        ),
        Route(
            amqp_rpc_caller,
            entrypoint=TimeEntrypoint(10)
        )], 
        context_metric_label="context_2"
    )
]


if __name__ == '__main__':
    runner = Runner(contexts, config)
    runner.start()


```

* tasks.py
```python
from micro_framework.amqp.dependencies import Producer
from micro_framework.websocket.dependencies import WebSocketRPCClient
from micro_framework.dependencies import inject
from micro_framework.retry import BackOffData
from micro_framework.amqp.dependencies import RPCProxyProvider
import time

@inject(producer=Producer(), retry=BackOffData())
def test(payload, producer, retry):
    # business_logic
    producer('event_name', {})

def test2(payload):
    print("Called after test function")

def test_failure(payload, producer, retry):
    print("Called after test failed and the max_retries is reached.")

class Class:
    producer = Producer()
    
    @inject(retry=BackOffData())
    def class_method(self, payload, retry):
        # business_logic
        self.producer('event_name', {})


# WebSocket RPC Examples
def websocket_task(arg1, arg2):
    """
    My Docstring
    :param arg1:
    :param arg2:
    :return:
    """
    print(f"Websocket Task Called with {arg1} and {arg2}")
    time.sleep(3)
    return f"{arg1} - {arg2}"

# Service Address to connect the client
@inject(service_rpc=WebSocketRPCClient('127.0.0.1', 8765))
def websocket_caller(reference, service_rpc):
    print(service_rpc.targets)
    print(service_rpc.websocket_task.info)
    async_response = service_rpc.websocket_task.call_async('argument1', 'argument2')
    print("This is an async response future", async_response)
    response = service_rpc.websocket_task('argument3', 'argument4')
    print('Sync Response: ', response)
    print("Async Response result will lock until response: ", async_response.result())


def amqp_rpc_task(arg1, arg2, *args, **kwargs):
    print(arg1, arg2, args, kwargs)

# We inject a RPCProxyProvider aiming at the service we wan't to consume.
# In this case, it is our own service...
@inject(service_rpc=RPCProxyProvider("my_service"))
def amqp_rpc_caller(reference, service_rpc):
    print(service_rpc.targets)
    print(service_rpc.amqp_rpc_task.info)
    async_response = service_rpc.amqp_rpc_task.call_async(
        'argument1', 'argument2', "additional_arg", and_kwargs="too"
    )
    print("This is an async response future", async_response)
    response = service_rpc.amqp_rpc_task(
        'argument3', 'argument4'
    )
    print('Sync Response: ', response)
    print(
        "Async Response result will lock until response: ", 
        async_response.result(timeout=None) # or some float value in seconds
    )

```

## Components

### Runner

It is the component that will start your service. You instantiate it with 
the configuration and the contexts.
  
### Runner Context
Aggregate Routes that share the same workers.
  
   * In the same Runner (your service), you can have multiple 
  contexts that do not share the workers number. That way you can have 
  dedicated workers to do some long-running tasks without letting your other 
  tasks starve due to no workers available.
  * Each Context will have it's own metric label to differentiate it.


### Route

A route acts as a pipeline for the entrypoint payload until reaching the 
**Target** function to be executed. 
  
Inside the route you define the behavior of the framework before creating a 
worker and after the worker finished the execution. It has a default Worker 
class but accepts a custom one. 

Inside the route you can add message translators that will be used by the 
worker when spawned and the entrypoint to trigger it.
  
### Entrypoint

An entrypoint is the trigger of an external action to the execution of the 
route configured to it.

Allowed Entrypoints are:
    
* TimeEntrypoint:
    * Its a timer, that triggers at each specified time. (Not cluster aware)

* amqp.EventListener:
    * It's an AMQP queue listener, that deals automatically configures all 
      AMQP stuff and can be called either by this Framework **Producer** 
      Dependency or by Nameko's EventDispatcher (yes, you can use both!).
      
            The same applies to our Producer allowing to trigger Nameko's 
            event_source

* websocket.WebSocketListener:
    * Start's a Websocket Server that implements our RPC Manager Class.
    You can call it through our WebSocketClient dependency.
      

* amqp.RPCListener:
    * Allows the **Target** to be called through AMQP protocol.
    

### Worker

A worker is the class responsible for the function call and the
 steps before and after such as message translating using the route
  translators and dependency injection from the given dependencies.


### Dependency

It is a class that usually provides a callable to be injected into the 
**Target**.

To Inject a dependency, you can either declare it in a class that has the 
**Target** as a method, or use the @inject decorator in the method/function 
directly, so it will be passed as a argument (named as the attribute name in 
the @inject)
 

### MessageTranslator

Transforms a payload/message into another payload/message.


## Configurations

* AMQP_URI: URI of the broker (When using AMQP Stuff).


* MAX_WORKERS: Number of workers to spawn simultaneously


* SERVICE_NAME: The name of the running service, useful for Producer
 dependency which automatically includes the service_name in the dispatch.
  

* WORKER_MODE: Runner mode. It can be:
    * thread: Each worker will run in the same process as Threads.
     When stopped, the worker will finish only the current 
     running task and ignore the pending ones in the pool
    
    * greedy_thread: Same as thread but the worker will only finish after
      completing all pending tasks in the pool
    
    * process: Each worker will run in a different process.
    

* WEBSOCKET_IP (default="0.0.0.0"): Address that the WS server will listen to.


* WEBSOCKET_PORT (default=8765): PORT that the WS server will listen to.


* ENABLE_METRICS (default=False): Allows the framework to export prometheus 
  metrics in a configured host/port
  

* METRICS dict: Configurations to the Metric Server
  
  * HOST (default="0.0.0.0")
  * PORT (default=8000)


## Notes:

In order to use the AsyncBackoff class, you must install the RabbitMQ 
plugin: [**rabbitmq_delayed_message_exchange**](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)

