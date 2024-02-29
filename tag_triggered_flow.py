from prefect import flow, task


@task
def my_task():
    print("look a task")


@flow
def downstream_tag_triggered_flow():
    my_task()


from prefect.events.schemas import DeploymentTrigger

downstream_deployment_trigger = DeploymentTrigger(
    name="Wait for Upstream Flow's Result PACC Taylor",
    enabled=True,
    match_related={  # match the flow id of the upstream flow
        "prefect.resource.id": "prefect.flow.00afd929-5829-4e60-934f-e4b51d268fd6"
    },
    # Expect is the main argument of the trigger object, this matches the event name of our emitted event
    expect={"prefect.result.produced"},
    # Here we take from the emitted events payload and apply it to the flows parameter
    parameters={
        "prev_result": "{{event.payload.result}}",
    },
)

if __name__ == "__main__":
    downstream_tag_triggered_flow.deploy(
        name="triggered-deployment",
        work_pool_name="my-k8s-pool",
        image="docker.io/taycurran/data-cleaning:demo",
        push=False,
        tags=["downstream", "triggered"],
        triggers=[
            {
                "match_related": {
                    "prefect.resource.id": [
                        "prefect.tag.demo",
                        "prefect.tag.data-cleaning",
                    ]
                },
                "expect": {"prefect.flow-run.Completed"},
            }
        ],
    )
