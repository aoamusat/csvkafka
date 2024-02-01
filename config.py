import os

sr_api_key = os.getenv('SR_API_KEY', "")
sr_api_secret = os.getenv('SR_API_SECRET', "")

CONFIG = {
    "REST_ENDPOINT": "https://pkc-p11xm.us-east-1.aws.confluent.cloud:443",
    "SCHEMA_REGISTRY": {
        "url": "https://psrc-zj6ny.us-east-2.aws.confluent.cloud",
        "basic.auth.user.info": f"{sr_api_key}:{sr_api_secret}",
    }
}
