import logging

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.core import rpc

logger = logging.getLogger(f"distributed.agent.plugin")


class AgentSchedulerPlugin(SchedulerPlugin):
    name = "agent"

    def __init__(self):
        self.scheduler = None
        self.nodes = {}
        self.provision_mode = "auto-cpu"  # TODO Implement more modes
        self.provision_config = None

    def start(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers["get_nodes"] = self.handle_get_nodes
        self.scheduler.handlers["reprovision_nodes"] = self.handle_reprovision_nodes
        self.scheduler.handlers["get_provision_mode"] = self.handle_get_provision_mode
        self.scheduler.handlers["register_node"] = self.handle_register_node
        self.scheduler.handlers["unregister_node"] = self.handle_unregister_node

    async def handle_get_nodes(self, comm):
        return self.nodes

    async def handle_get_provision_mode(self, comm):
        return self.provision_mode, self.provision_config

    async def handle_reprovision_nodes(
        self, comm, mode=None, config=None, remove_existing=True
    ):
        for node_name, node in self.nodes.items():
            logger.info(f"Reprovisioning node {node_name} with mode {mode}")
            node_comm = rpc(node["address"])
            await node_comm.reprovision(
                mode=mode, config=config, remove_existing=remove_existing
            )

    async def handle_register_node(
        self, comm, name, address, cpus=None, memory=None, gpus=None
    ):
        self.nodes[name] = {
            "address": address,
            "cpus": cpus,
            "memory": memory,
            "gpus": gpus,
        }
        logger.info(f"Registered node {name} with address {address}")

    async def handle_unregister_node(self, comm, name):
        del self.nodes[name]
        logger.info(f"Unregistered node {name}")


def dask_setup(scheduler):
    plugin = AgentSchedulerPlugin()
    scheduler.add_plugin(plugin, idempotent=True)
