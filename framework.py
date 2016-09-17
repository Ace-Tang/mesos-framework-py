#!/usr/bin/env python

import logging
import uuid
import time

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

def new_task(offer):
    task = mesos_pb2.TaskInfo()
    id = uuid.uuid4()
    task.task_id.value = str(id)
    task.slave_id.value = offer.slave_id.value
    task.name = "task {}".format(str(id))

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    return task

class MyScheduler(Scheduler):
    def registered(self, driver, framework_id, mesos_info):
        logging.info("registering master with framework_id: %s" % framework_id)

    def reregistered(self, driver, mesos_info):
        logging.info("reregistering master")

    def disconnected(self, driver):
        logging.info("framework disconnected")

    def resourceOffers(self, driver, offers):
        logging.info("receive offer {}".format([o.id.value for o in offers]))
        for offer in offers:
            task = new_task(offer)
            task.command.value = "echo hello from py-framework"
            logging.info("launching task {t} with offer {o}".format(t=task.task_id.value, o=offer.id.value))
            task = [task]
            driver.launchTasks(offer.id, task)

if __name__ == '__main__':
    framework = mesos_pb2.FrameworkInfo()
    framework.user = 'ace'
    framework.name = 'py-1'
    driver = MesosSchedulerDriver(MyScheduler(), framework, "10.8.15.206")
    driver.run()
