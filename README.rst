Dask Agent
==========

Dask Agent is a drop in replacement for ``dask-worker``, ``dask-spec`` and ``dask-cuda-worker`` which aims to provide more flexibility and control to power users over starting worker processes.

Features:

- Auto detect CPU size and create workers appropriately (similar to ``LocalCluster``).
- Uses ``dask-cuda`` to launch GPU workers.
- Can dynamically reprovision nodes with different worker types via the ``Client``.
- Allows manual control over worker creation on existing nodes, useful for creating heterogenous clusters.

Problems it solves:

- Decouples node provisioning from worker provisioning
- Allows worker reconfiguration without losing hardware allocation
- Enables packing homogenous workers into heterogenous nodes
- Enables packging of heterogenous workers into homogenous nodes
- Allows mixing worker types without needing support in deployment tooling
- Launches multiple worker types to make best use of advanced hardware

Install
-------

TODO

Quickstart
----------

Start a scheduler

.. code-block:: console

    $ dask-scheduler

Start an agent process pointing to the scheduler

.. code-block:: console

    $ dask-agent tcp://<ip>:8786
    distributed.agent - INFO - Starting Dask Agent
    distributed.agent - INFO - Agent at:  tcp://10.51.100.80:55189
    distributed.agent - INFO - Provisioning nodes with mode 'auto-cpu'
    distributed.agent - INFO - Found 12 CPU cores, provisioning 4 processes with 3 threads each
    distributed.nanny - INFO -         Start Nanny at: 'tcp://127.0.0.1:55191'
    distributed.nanny - INFO -         Start Nanny at: 'tcp://127.0.0.1:55192'
    distributed.nanny - INFO -         Start Nanny at: 'tcp://127.0.0.1:55193'
    distributed.nanny - INFO -         Start Nanny at: 'tcp://127.0.0.1:55194'

Connect a client to the scheduler and change the provisioning mode

.. code-block:: python

    In [1]: from dask.distributed import Client

    In [2]: client = await Client("tcp://localhost:8786", asynchronous=True)

    In [3]: await client.scheduler.reprovision_nodes(mode="auto-gpu")

See the agent closing CPU workers and starting GPU workers

.. code-block:: console

    distributed.agent - INFO - Closing all subprocesses
    distributed.nanny - INFO - Closing Nanny at 'tcp://127.0.0.1:55191'
    distributed.nanny - INFO - Closing Nanny at 'tcp://127.0.0.1:55192'
    distributed.nanny - INFO - Closing Nanny at 'tcp://127.0.0.1:55193'
    distributed.worker - INFO - Stopping worker at tcp://127.0.0.1:55201
    distributed.nanny - INFO - Closing Nanny at 'tcp://127.0.0.1:55194'
    distributed.worker - INFO - Stopping worker at tcp://127.0.0.1:55199
    distributed.worker - INFO - Stopping worker at tcp://127.0.0.1:55200
    distributed.worker - INFO - Stopping worker at tcp://127.0.0.1:55208
    distributed.agent - INFO - Provisioning nodes with mode 'auto-gpu'
    distributed.agent - ERROR - Cannot provision GPU workers, unable to find dask_cuda

Examples
--------

Spec
^^^^

Reprovisioning each agent to run a worker from a spec.

.. code-block:: python

    await client.scheduler.reprovision_nodes(
        mode="spec",
        config={
            "spec": {
                "cls": "dask.distributed.Nanny",
                "opts": {},
            },
        },
    )
