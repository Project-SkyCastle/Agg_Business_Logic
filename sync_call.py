import util
import requests
import json


def sync_fetch(session, resource):
    url = resource["url"]
    print("Calling URL =", url)
    with session.get(url) as response:
        t = response.json()
        print("Returned URL =", url)
        result = {
            "resource": resource["resource"],
            "data": t
        }
        # print(result)
        util.response_order += [resource["resource"]]
    return result


def sync_aggregate(resources):
    with requests.session() as session:
        responses = [sync_fetch(session, res) for res in resources]
        full_result = {}
        for response in responses:
            full_result[response["resource"]] = response["data"]
    return full_result


def sync_request(resources):
    return sync_aggregate(resources)
