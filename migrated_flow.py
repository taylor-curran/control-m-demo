# with Flow(
#         name="etl",
#         schedule=Schedule(clocks=[CronClock("0 6,7,8,9,10 * * *")]),
#         run_config=ECSRun(
#             task_definition_path="s3://bucket/path/to/definition.yaml",
#         )
#     ) as flow:
#     vendor = Parameter("vendor")
#     data = extract_data.map(unmapped(vendor), range(60))
#     transformed_data = transform_data.map(data)
#     load_data(transformed_data)


# if __name__ == "__main__":
#     flow.register(project_name="etl")

from prefect import flow, task


@task
def extract_data(vendor, my_int):
    pass


@task
def transform_data(data):
    pass


@task
def load_data(data):
    pass


@flow(name="etl")
def etl_flow(vendor):
    data = extract_data.map(vendor, range(60))
    transformed_data = transform_data.map(data)
    load_data(transformed_data)


if __name__ == "__main__":
    etl_flow("vendor_a")

    etl_flow.deploy(
        name="production",
        work_pool_name="ecs_push_pool",
        cron="0 6,7,8,9,10 * * *",
        triggers=[
            {
                "match_related": {
                    "prefect.resource.id": "prefect-cloud.webhook.172ec9ec-164a-4706-9f6d-ce5390acdccc"
                },
                "expect": {"webhook.called"},
            }
        ],
    )
