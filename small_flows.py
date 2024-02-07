from prefect import flow, task
import time


@task
def task_1():
    print("This is task 1")
    time.sleep(5)


@task
def task_2(one):
    print("This is task 2")
    time.sleep(5)


@flow
def customer_analysis():
    print("This is my flow")
    one = task_1()
    two = task_2(one)
    time.sleep(5)


@flow
def team_analysis():
    print("This is my flow")
    one = task_1()
    two = task_2(one)
    time.sleep(5)


if __name__ == "__main__":
    customer_analysis.deploy(
        name="on-prem-deployment",
        work_pool_name="on-prem-work-pool",
        interval=12000,
        image="docker.io/taycurran/customer-data:demo",
    )

    team_analysis.deploy(
        name="production-deployment",
        work_pool_name="snowflake-cluster",
        cron="0 0 * * 1",
        image="docker.io/taycurran/team-data:demo",
    )
