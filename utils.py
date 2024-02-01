"""_summary_

Returns:
    _type_: _description_

Yields:
    _type_: _description_
"""

from typing import Generator, Dict
import csv


def read_ccloud_config(config_file: str = "client.properties") -> Dict:
    """Read Confluent cloud kafka configuration file: client.properties

    Args:
        config_file (str): configuration filename

    Returns:
        Dict: a dictionary
    """
    omitted_fields = set(
        ["schema.registry.url", "basic.auth.credentials.source", "basic.auth.user.info"]
    )
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                if parameter not in omitted_fields:
                    conf[parameter] = value.strip()
    return conf


def read_transaction_file(filename) -> Generator:
    """Read transaction file and return it as a generator

    Args:
        filename (str): transaction file name

    Yields:
        Generator: Transaction generator
    """
    with open(filename, "r") as file:
        rows = csv.reader(file)
        next(rows)  # skip csv header
        for row in rows:
            yield {
                "timestamp": int(row[0]),
                "transaction_type": row[1],
                "token": row[2],
                "amount": float(row[3]),
            }

def delivery_callback(error, message):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if error is not None:
        print('Message delivery failed: {}'.format(error))
    else:
        print('Message delivered to {} [{}]'.format(message.topic(), message.partition()))