import asyncio
import json
import os
from datetime import timedelta
from time import sleep
from typing import Any, List

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from prefect import flow, tags, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.logging import get_run_logger
from prefect.tasks import exponential_backoff, task_input_hash
from pydantic import BaseModel, Field

import random


class Animal(BaseModel):
    name: str = Field(..., description="Name of the animal")
    population: int = Field(..., description="Number of animals in the population")
    size: str = Field(..., description="Size of the animal")
    weight: float = Field(..., description="Weight of the animal in kg")
    geography: str = Field(..., description="Geographic location of the animal")
    lifespan: float = Field(..., description="Average lifespan of the animal in years")
    diet: str = Field(..., description="Diet type of the animal")
    endangered: bool = Field(..., description="Indicates if the animal is endangered")
    habitat: str = Field(..., description="Typical habitat of the animal")
    speed: float = Field(..., description="Maximum speed of the animal in km/h")


@task(
    name="Download Data and Parse",
    retries=10,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=1,
)
def download_object_and_parse(bucket_name, object_name):
    # Create an S3 client
    s3_client = boto3.client(
        "s3",
    )

    try:
        # Download the object content
        object_content = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        json_content = object_content["Body"].read()

        # Parse the JSON content into a Python dictionary
        data = json.loads(json_content)
        return pd.DataFrame(data)

    except NoCredentialsError:
        print("Credentials not available")
        return None
    except ClientError as e:
        print(f"An AWS Client error occurred: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


@task(name="Generate Insights")
def avg_weight(df: pd.DataFrame) -> float:
    # 1. Calculate the average weight of animals
    return df["weight"].mean().round(2)


@task(name="Endangered Count")
def endangered_count(df: pd.DataFrame) -> int:
    # 2. Count the number of endangered animals
    sleep(3)
    return df[df["endangered"] == True].shape[0]


@task(name="Common Habitat")
def common_habitat(df: pd.DataFrame) -> str:
    # 3. Find the most common habitat
    sleep(5)
    return df["habitat"].value_counts().idxmax()


@task(name="Fastest Animal")
def fastest_animal(df: pd.DataFrame) -> str:
    # 4. Find the fastest animal and its speed
    sleep(2)
    return df.loc[df["speed"].idxmax()]["name"]


@task(name="Top Speed")
def top_speed(df: pd.DataFrame) -> float:
    return df["speed"].max()


@task(name="Average Lifespan per Diet")
def avg_lifespan_per_diet(df: pd.DataFrame) -> dict:
    # 5. Calculate the average lifespan based on diet type
    sleep(4)
    return df.groupby("diet")["lifespan"].mean().to_dict()


@task(name="Combine Insights")
def combine(
    avg_weight: float,
    endangered_count: float,
    common_habitat: str,
    fastest_animal: str,
    fastest_speed: float,
    avg_lifespan_per_diet: float,
) -> dict[str, Any]:
    return {
        "Average Weight in Lbs": avg_weight,
        "Number of Endangered Animals": endangered_count,
        "Most Common Habitat": common_habitat,
        "Fastest Animal": fastest_animal,
        "Speed of Fastest Animal": fastest_speed,
        "Average Lifespan per Diet": avg_lifespan_per_diet,
    }


@flow(
    name="Blackstone Animal Data",
    timeout_seconds=45,
    retries=2,
)
def animal_data(artifact_title: str = "Animal Insights", example_bool: bool = True):
    df = download_object_and_parse("se-demo-raw-data-files", "demo/animals.json")
    create_table_artifact(key="animal-data", table=df.to_dict(orient="records"))
    insights = combine(
        avg_weight(df),
        endangered_count(df),
        common_habitat(df),
        fastest_animal(df),
        top_speed(df),
        avg_lifespan_per_diet(df),
    )
    generate_markdown = dict_to_markdown_table(artifact_title, insights)
    create_markdown_artifact(markdown=generate_markdown, key="animal-insights")
    with tags(*df.name.tolist()):
        whalesaid = whalesay_scheduler(generate_markdown)


if __name__ == "__main__":
    # For running locally async
    # asyncio.run(animal_data())
    # for running sync
    animal_data()
    # For serving locally
    # animal_data.serve(name="Zoo Animal Data")
