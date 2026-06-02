from qcfractal.snowflake import FractalSnowflake
import json

if __name__ == "__main__":

    snowflake = FractalSnowflake(compute_workers=0)
    client = snowflake.client()

    print(json.dumps(client.get_server_openapi_spec(), indent=2, sort_keys=True))
