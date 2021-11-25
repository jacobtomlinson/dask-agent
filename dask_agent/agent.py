import sys
import asyncio
from tornado.ioloop import IOLoop
import signal
import logging
import uuid

from dask.system import CPU_COUNT

from distributed.node import ServerNode
from distributed.objects import SchedulerInfo
from distributed.core import rpc, Status
from distributed.comm.addressing import get_address_host
from distributed.deploy.utils import nprocesses_nthreads
from distributed.cli.utils import install_signal_handlers
from distributed.nanny import Nanny
from distributed.protocol.pickle import dumps

from .scheduler_plugin import AgentSchedulerPlugin


logger = logging.getLogger(f"distributed.agent")


class Agent(ServerNode):
    def __init__(self, scheduler_address: str = None):
        self.name = f"Agent-{uuid.uuid4()}"
        self.scheduler_address = scheduler_address
        self.scheduler_comm = None
        self.scheduler_info = None
        self.nannies = []
        self.ip = None
        self.provision_mode = None
        self.provision_config = None
        self.status = Status.created
        self._event_finished = asyncio.Event()
        if self.scheduler_address is None:
            print("Requires scheduler address")
            sys.exit(1)
        self.handlers = {
            "reprovision": self.reprovision,
        }
        super().__init__(handlers=self.handlers)

    async def start(self):
        if self.status == Status.running or self.status == Status.starting:
            return
        self.status = Status.starting

        logger.info("Starting Dask Agent")
        install_signal_handlers(IOLoop.current(), cleanup=self.on_signal())

        await self.listen(
            "0.0.0.0:0",
            allow_offload=False,
            handshake_overrides={"pickle-protocol": 4, "compression": None},
        )
        self.ip = get_address_host(self.listen_address)

        for listener in self.listeners:
            logger.info("Agent at: %25s", listener.contact_address)

        # Connect to scheduler and register agent plugin
        self.scheduler_comm = rpc(
            self.scheduler_address,
        )

        # TODO Stop additional agents from overwriting the plugin
        await self.scheduler_comm.register_scheduler_plugin(
            plugin=dumps(AgentSchedulerPlugin()),
            name="agent",
            idempotent=True,  # FIXME This should stop multiple agents from clobbering the plugin but it doesn't
        )

        # Register node with scheduler
        # TODO Detect hardware
        await self.scheduler_comm.register_node(
            name=self.name,
            address=self.listeners[0].contact_address,
            cpus=0,
            memory=0,
            gpus=0,
        )

        (
            self.provision_mode,
            self.provision_config,
        ) = await self.scheduler_comm.get_provision_mode()

        await self.provision()

        await super().start()
        self.status = Status.running

    async def close_nannies(self):
        logger.info(f"Closing all subprocesses")
        await asyncio.gather(*(n.close(timeout=2) for n in self.nannies))

    async def close(self):
        self.status = Status.closing
        await self.close_nannies()
        await self.scheduler_comm.unregister_node(name=self.name)
        await self.scheduler_comm.close()
        self._event_finished.set()
        logger.info(f"Agent closed")
        self.status = Status.closed

    async def finished(self):
        """Wait until the server has finished"""
        await self._event_finished.wait()

    async def provision(self):
        if not self.provision_mode:
            logger.info(f"No provision mode set, nothing to do")
            return

        logger.info(f"Provisioning nodes with mode '{self.provision_mode}'")

        # Provision workers
        if self.provision_mode == "auto-cpu":
            self.nannies = await self.provision_auto_cpu()
        elif self.provision_mode == "auto-gpu":
            self.nannies = await self.provision_auto_gpu()
        else:
            logger.error(f"Unknown provisioning mode '{self.provision_mode}'")
            sys.exit(1)

        await asyncio.gather(*self.nannies)

    async def reprovision(self, comm, mode=None, config=None, remove_existing=True):
        self.provision_mode = mode
        self.provision_config = config

        if remove_existing:
            await self.close_nannies()

        await self.provision()

    async def provision_auto_cpu(self):
        nprocs, nthreads = nprocesses_nthreads()
        logger.info(
            f"Found {CPU_COUNT} CPU cores, provisioning {nprocs} processes with {nthreads} threads each"
        )
        return [
            Nanny(
                self.scheduler_address,
                nthreads=nthreads,
            )
            for _ in range(nprocs)
        ]

    async def provision_auto_gpu(self):
        try:
            from dask_cuda import CUDAWorker

            return [CUDAWorker(self.scheduler_address)]
        except ImportError:
            logger.error("Cannot provision GPU workers, unable to find dask_cuda")
            return []

    def on_signal(self):
        close = self.close

        def f(signum):
            if signum != signal.SIGINT:
                logger.info("Exiting on signal %d", signum)
            return asyncio.ensure_future(close())

        return f


async def run_agent(*args, **kwargs):
    agent = Agent(*args, **kwargs)
    await agent.start()
    await agent.finished()
