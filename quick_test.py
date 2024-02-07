from prefect import flow
import prefect
import time

# Adding a global variable to keep track of attempts
attempt_counter = {"sub_flow_a": 0}


@flow(retries=3)
def sub_flow_a():
    time.sleep(2)
    global attempt_counter

    attempt_counter["sub_flow_a"] += 1

    # Fail the first two attempts
    if attempt_counter["sub_flow_a"] <= 2:
        raise prefect.engine.signals.FAIL("Intentional Failure")

    return 1


@flow
def sub_flow_b():
    time.sleep(2)
    return 2


@flow
def parent_flow():
    a = sub_flow_a()
    b = sub_flow_b()

    return b


if __name__ == "__main__":
    parent_flow()
