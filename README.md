# Micro Framework

(yet another) Python Microservices Framework capable of running functions in
 threads or processes. 

The code structure is inspired by Nameko code (https://github.com/nameko/nameko) 
but with some different concepts regarding the function call.

## Components
* Runner: It is the component that will start your service. You instantiate
 it with the configuration and the routes.
 
* Entrypoint: An entrypoint is the trigger of an external action to the
 execution of the route configured to it.
 
* Route: A route acts as a pipeline for the entrypoint payload until reaching
 the target function to be executed. Inside the route you define the behavior
  of the framework before creating a worker and after the worker finished the
   execution. It has a default Worker class but accepts a custom one.
   
   Inside the route you can add dependencies and message translators that
    will be used by the worker when spawned.
  

* Worker: A worker is the class responsible for the function call and the
 steps before and after such as message translating using the route
  translators and dependency injection from the given dependencies.

* Dependency: It's a class that should be passed to the target function with
 access to the worker context.
 
* MessageTranslator: Transforms a payload/message into another payload/message.


## Usage
 To use, simply create a module to start, define your entrypoints and
  routes.
  
  ** Note that the function is only the path. That is to enable us to do some
   script import configurations like django.setup() in the worker only.
   
* python main.py
```python
import logging
from micro_framework.runner import Runner
from micro_framework.routes import Route
from micro_framework.amqp.entrypoints import EventListener
from micro_framework.amqp.dependencies import Producer
 
config = {
    'AMQP_URI': 'amqp://guest:guest@localhost:5672',
    'MAX_WORKERS': 3,
    'SERVICE_NAME': 'my_service',
    'WORKER_MODE': 'thread'
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(message)s')

entrypoints = [
    EventListener(
        'source_service', 'event_name', 
        route=Route('tasks.test', dependencies={'producer': Producer()})
    ),
]
if __name__ == '__main__':
    runner = Runner(entrypoints, config)
    runner.start()

```

* task.py
```python
def test(payload, producer):
    # business_logic
    producer('event_name', {})
```


## Configurations

* AMQP_URI: URI of the broker.
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
    * greedy_process: Same as process and the same behavior when shutting
     down as greedy_thread.
   