# How the API is envisioned
from typing import Annotated

app = UFW()

p_app = app.runner("process")


class DependencyA:
    ...


@p_app.amqp.on_event("...")
def task1(msg, dep1: DependencyA):
    ...


class ClassTask:
    dep_a = DependencyA

    @p_app.amqp.on_event("...")
    def method1(self, msg, dep2: Annotated[dict, dep_a]):
        ...


    @p_app.schedule.every.one.minute()
    def method2(self, ref):
        ...

