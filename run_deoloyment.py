from prefect import flow
from prefect.deployments import run_deployment


@flow
def my_flow():
    # The scheduled flow run will not be linked to this flow as a subflow.
    run_deployment(name="my_other_flow/my_deployment_name", as_subflow=False)
