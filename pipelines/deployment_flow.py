from sched import scheduler

from asyncpg import TriggeredActionError
from click import UUID


class Deployment:
    """
    Structure of the schema defining a deployment
    """

    # required defining data
    name: str 
    flow_id: UUID
    entrypoint: str
    path: str = None

    # workflow scheduling and parametrization
    parameters: dict = None
    parameter_openapi_schema: dict = None
    schedule: scheduler = None
    is_schedule_active: bool = True
    trigger: TriggeredActionError = None

    # metadata for bookkeeping
    version: str = None
    description: str = None
    tags: list = None

    # worker-specific fields
    work_pool_name: str = None
    work_queue_name: str = None
    infra_overrides: dict = None
    pull_steps: dict = None
