import asyncio
import aiohttp
import nest_asyncio
import json
import util

nest_asyncio.apply()


async def async_fetch(session, resource):
    url = resource["url"]
    print("Calling URL =", url)
    async with session.get(url) as response:
        t = await response.json()
        print("Returned URL =", url)
        result = {
            "resource": resource["resource"],
            "data": t
        }
        util.response_order += [resource["resource"]]
    return result


async def async_aggregate(resources):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.ensure_future(async_fetch(session, res)) for res in resources]
        responses = await asyncio.gather(*tasks)
        full_result = {}
        for response in responses:
            full_result[response["resource"]] = response["data"]
    return full_result


def async_request(resources):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(async_aggregate(resources))
