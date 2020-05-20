# Micro Framework
(yet another) Python Microservices Framework capable of running functions in
 threads or processes. 
 
It is inspired by Nameko but without the class service style.

## Components
* Runner: It is the component that will start your service. You instantiate
 it with the configuration and the routes.
 
* Route: A route is the link between an entrypoint and a function with the
 business logic. The route is responsible to trigger based on the kind of
  route you selected and ask for the Runner to call the function.
  
  You can define dependencies that this function has and also add translators
  , which will convert the entrypoint message to a desired format.

* Dependency: It's a class that should be passed to the target function which
 can be configured based on some configurations of the route/runner.
 
* MessageTranslator: Transforms a message into another message.


## Usage
 To use, simply create a module to start, define your routes and identify the
  function to be called by that route.
  
  ** Note that the function is only the path. That is to enable us to do some
   script import configurations like django.setup() in the worker only.
   
* python main.py
```python
import logging
from micro_framework.runner import Runner
from micro_framework.amqp.routes import EventRoute
from micro_framework.amqp.dependencies import Producer
 
config = {
    'AMQP_URI': 'amqp://localhost:5672',
    'MAX_WORKERS': 3,
    'SERVICE_NAME': 'my_service',
    'WORKER_MODE': 'thread' # or process
}

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(message)s')

routes = [
    EventRoute('source_service', 'event_name', 'tasks.test', 
               dependencies={'producer': Producer()}),
]
if __name__ == '__main__':
    runner = Runner(routes, config)
    runner.start()

```

* task.py
```python
def test(payload, producer):
    # business_logic
    producer('event_name', {})
```
