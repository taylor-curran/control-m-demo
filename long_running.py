from prefect import flow, task
import time


@task
def retraining_task_1():
    print("This is task 1")
    time.sleep(1000)


@task
def retraining_task_2(one):
    print("This is task 2")
    time.sleep(1000)


@flow
def retrain_model():
    print("This is my flow")
    time.sleep(2000)
    one = retraining_task_1()
    time.sleep(2000)
    two = retraining_task_2(one)
    time.sleep(2000)


if __name__ == "__main__":
    retrain_model()
