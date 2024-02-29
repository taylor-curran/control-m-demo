from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject

my_flow = dbt_flow(
    project=DbtProject(
        name="jaffle_shop",
        project_dir="/Users/taylorcurran/Documents/george/control-m-demo/dbt_example/jaffle_shop/",
        profiles_dir="/Users/taylorcurran/.dbt/profiles.yml",
    ),
    profile=DbtProfile(
        target="dev",
        overrides={
            "type": "bigquery",
            "method": "service-account",
            "keyfile": "/Users/taylorcurran/.dbt/prefect-sbx-taylorcurran-c5b4344c8bd8.json",
            "project": "prefect-sbx-taylorcurran",
            "dataset": "jaffle_shop",
            "threads": 3,
            "timeout_seconds": 300,
            "location": "US",
            "priority": "interactive",
        },
    ),
)

if __name__ == "__main__":
    my_flow()
