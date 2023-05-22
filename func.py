import io
import json
import logging
import os
import requests
from fdk import response

"""
Example Log Source:
https://docs.oracle.com/en-us/iaas/Content/Logging/Reference/details_for_emaildelivery.htm#details_for_emaildelivery
"""

# Use OCI Application or Function configurations to override these environment variable defaults.

api_endpoint = os.getenv('API_ENDPOINT', 'not-configured')
api_token = os.getenv('API_KEY', 'not-configured')
api_token_header = os.getenv('API_KEY_HEADER', 'not-configured')
api_account_id = os.getenv('ACCOUNT_ID', 'not-configured')
api_account_id_header = os.getenv('ACCOUNT_ID_HEADER', 'not-configured')
is_forwarding = eval(os.getenv('FORWARD_TO_ENDPOINT', "True"))

# Set all registered loggers to the configured log_level

logging_level = os.getenv('LOGGING_LEVEL', 'INFO')
loggers = [logging.getLogger()] + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
[logger.setLevel(logging.getLevelName(logging_level)) for logger in loggers]


def handler(ctx, data: io.BytesIO = None):
    """
    OCI Function Entry Point
    :param ctx: InvokeContext
    :param data: data payload
    :return: plain text response indicating success or error
    """

    preamble = "fn {} / events {} / logging level {} / forwarding {}"

    try:
        event_list = json.loads(data.getvalue())
        logging.getLogger().info(preamble.format(ctx.FnName(), len(event_list), logging_level, is_forwarding))
        send_to_datadog(event_list=event_list)

    except (Exception, ValueError) as ex:
        logging.getLogger().error('error handling logging payload: {}'.format(str(ex)))
        logging.getLogger().error(ex)


def send_to_datadog (event_list):
    """
    Sends each transformed event to DataDog Endpoint.
    :param event_list: list of events in DataDog format
    :return: None
    """

    if is_forwarding is False:
        logging.getLogger().debug("Forwarding is disabled - nothing sent to API endpoint")
        return

    # creating a session and adapter to avoid recreating
    # a new connection pool between each POST call

    session = requests.Session()
    try:
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
        session.mount('https://', adapter)

        for event in event_list:
            dd_headers = {'Content-type': 'application/json',
                          api_token_header: api_token,
                          api_account_id_header:
                              api_account_id}

            post_response = session.post(api_endpoint, data=json.dumps(event), headers=dd_headers)
            if post_response.status_code != 200:
                raise Exception('error POSTing API endpoint', post_response.text)

    finally:
        session.close()


def get_dictionary_value(dictionary: dict, target_key: str):
    """
    Recursive method to find value within a dictionary which may also have nested lists / dictionaries.
    :param dictionary: the dictionary to scan
    :param target_key: the key we are looking for
    :return: If a target_key exists multiple times in the dictionary, the first one found will be returned.
    """

    target_value = dictionary.get(target_key)
    if target_value:
        return target_value

    for key, value in dictionary.items():
        if isinstance(value, dict):
            target_value = get_dictionary_value(dictionary=value, target_key=target_key)
            if target_value:
                return target_value

        elif isinstance(value, list):
            for entry in value:
                if isinstance(entry, dict):
                    target_value = get_dictionary_value(dictionary=entry, target_key=target_key)
                    if target_value:
                        return target_value


def local_test_mode(filename):
    """
    This routine reads a local json CloudEvents file, converting the contents to DataDog format.
    :param filename: cloud events json file exported from OCI Logging UI or CLI.
    :return: None
    """

    logging.getLogger().info("testing {}".format(filename))

    with open(filename, 'r') as f:
        data = json.load(f)

        if not isinstance(data, list):
            data = [data]

        logging.getLogger().debug(json.dumps(data, indent=4))
        send_to_datadog(event_list=data)


"""
Local Debugging 
"""

if __name__ == "__main__":
    local_test_mode('test-data/hard-bounce.json')
    local_test_mode('test-data/soft-bounce.json')
    local_test_mode('test-data/successful-relay.json')
