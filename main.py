from fastapi import FastAPI, Body, Response, status
import uvicorn
import time
import util
import async_call
import sync_call
import json
from pydantic import BaseModel
from typing import Optional
import requests
import pandas as pd
from pandas import json_normalize
import datetime

app = FastAPI()
util.init()

subscription_fields = [
    'subscription_id', 'user_id', 'analyst_id', 'report_id', 'subscription_date',
    'feedbacks', 'notifications', 'activity'
]


def get_full_subscription():
    res = requests.get(util.resources['subscription'] + 'full')
    subs_df = pd.concat([pd.DataFrame(item[0]) for item in res.json()], axis=1).transpose()
    subs_df.columns = subscription_fields
    return subs_df


@app.get("/")
async def root():
    return {
        "Aggregator for Business Logic"
    }


# noinspection DuplicatedCode
@app.post("/create_report")
async def create_subscription(data=Body(...), response=Response):
    return_dict = {}

    # POST Subscription
    subs_df = get_full_subscription()
    next_id = subs_df['subscription_id'].max() + 1
    post_subs_dict = {
        "subscription_id": next_id,
        "user_id": int(data['user_id']),
        "report_id": int(data['user_id']),
        "analyst_id": '',
        "subscription_date": datetime.datetime.now().strftime("%b %d, %Y"),
        "feedbacks": "",
        "notifications": "",
        "activity": ""
    }
    res = requests.post(util.resources['subscription'], json=post_subs_dict)
    return_dict['subscription_id'] = res.json()['Created']['subscription_id']
    return_dict['user_id'] = res.json()['Created']['user_id']
    print(" *** Subscription Created: subscription_id=" + str(res.json()['Created']['subscription_id']))
    print(
        " ****** User " + str(res.json()['Created']['user_id']) + ' is now subscribing to report ' +
        str(res.json()['Created']['report_id'])
    )
    print()

    return return_dict


# noinspection DuplicatedCode
@app.post("/create_subscription")
async def create_subscription(data=Body(...), response=Response):
    return_dict = {}

    # POST Subscription
    subs_df = get_full_subscription()
    next_id = subs_df['subscription_id'].max() + 1
    post_subs_dict = {
        "subscription_id": next_id,
        "user_id": int(data['user_id']),
        "report_id": int(data['user_id']),
        "analyst_id": '',
        "subscription_date": datetime.datetime.now().strftime("%b %d, %Y"),
        "feedbacks": "",
        "notifications": "",
        "activity": ""
    }
    res = requests.post(util.resources['subscription'], json=post_subs_dict)
    return_dict['subscription_id'] = res.json()['Created']['subscription_id']
    return_dict['user_id'] = res.json()['Created']['user_id']
    print(" *** Subscription Created: subscription_id=" + str(res.json()['Created']['subscription_id']))
    print(
        " ****** User " + str(res.json()['Created']['user_id']) + ' is now subscribing to report ' +
        str(res.json()['Created']['report_id'])
    )
    print()

    return return_dict


@app.post("/delete_subscription")
async def delete_subscription(data=Body(...), response=Response):
    return_dict = {}

    # POST Subscription
    subs_df = get_full_subscription()
    subs_df = subs_df[
        (subs_df['user_id'] == int(data['user_id']))
        & (subs_df['report_id'] == int(data['report_id']))
        ]
    return_dict['subscription_id'] = subs_df['subscription_id'].values
    print(" *** Found subscription: " + ', '.join(return_dict['subscription_id']))
    if len(subs_df) > 0:
        for subscription_id in return_dict['subscription_id']:
            res = requests.delete(util.resources['subscription'] + str(subscription_id))
            print(" ****** DELETED subscription " + str(subscription_id))
    else:
        print(" ****** No subscription found on (" + str(data['user_id']) + ", " + str(data['report_id']) + ")")
    print()

    return return_dict


@app.post("/create_report")
async def create_report(data=Body(...), response=Response):
    # Input:
    # {
    #     "title": "POST Report TEST",
    #     "analyst_id": "5",
    #     "content": "this is the new report I added for the video",
    #     "feedback": "very good video report"
    # }

    return_dict = {}

    # POST Report
    res = requests.post(util.resources['report'], json=data)
    if res.status_code != 201:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return res.json()
    created_report_id = res.json()['report_id']
    return_dict['report_id'] = created_report_id
    print(" *** Report Created: report_id=" + str(created_report_id))
    print(" ****** Report title '" + res.json()['title'] + "' by Analyst " + str(res.json()['analyst_id']))
    print()

    # POST Subscription
    subs_df = get_full_subscription()
    next_id = subs_df['subscription_id'].max() + 1
    post_subs_dict = {
        "subscription_id": next_id,
        "user_id": int(data['analyst_id']),
        "report_id": created_report_id,
        "analyst_id": int(data['analyst_id']),
        "subscription_date": datetime.datetime.now().strftime("%b %d, %Y"),
        "feedbacks": "test_feedbacks",
        "notifications": "test_notification2",
        "activity": "test_activity"
    }
    res = requests.post(util.resources['subscription'], json=post_subs_dict)
    return_dict['subscription_id'] = res.json()['Created']['subscription_id']
    return_dict['user_id'] = res.json()['Created']['user_id']
    print(" *** Subscription Created: subscription_id=" + str(res.json()['Created']['subscription_id']))
    print(
        " ****** User " + str(res.json()['Created']['user_id']) + ' is now subscribing to report ' +
        str(res.json()['Created']['report_id'])
    )
    print()

    return return_dict


@app.delete("/delete_report/{report_id}")
async def delete_report(report_id: int, response: Response):
    return_dict = {}

    # DELETE Report
    res = requests.delete(util.resources['report'] + str(report_id))

    if res.status_code != 200:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return res.json()
    return_dict['report_id'] = res.json()['report_id']
    print(" *** Report Deleted: report_id=" + str(return_dict['report_id']))
    print()

    # POST Subscription
    res = requests.delete(util.resources['subscription'] + 'report/' + str(return_dict['report_id']))
    return_dict['Subscription Deleted'] = res.json()
    print(" *** Subscription Deleted by report_id: report_id=" + str(return_dict['report_id']))
    for item in return_dict['Subscription Deleted']:
        print(
            " ****** Subscription " + str(item['Deleted subscription_id']) + ' is deleted for ' +
            str(item['user_id'])
        )
    print()
    return return_dict


@app.get("/user_subscription/{user_id}")
async def get_report_by_user(user_id: int, response: Response):
    return_dict = {}

    # GET List of report_id FROM /subscription BY user_id
    res = requests.get(util.resources['subscription'] + 'user_id/' + str(user_id))

    if res.status_code != 200:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return res.json()
    return_dict['report_id'] = res.json()['report_id']
    print(" *** Reports Subscribed: report_id=" + str(return_dict['report_id']))
    print()

    # GET List of report titles by List of report_id
    report_titles = []
    for report_id in return_dict['report_id']:
        res = requests.get(util.resources['report'] + str(report_id))
        report_titles += res.json()['title']
        print(' ***** Report ' + str(report_id) + ' : ' + res.json()['title'])
    return_dict['Report Titles'] = report_titles
    print()
    return return_dict


@app.get("/show_stat/")
async def show_stat(response: Response):
    return show_stat_custom('async')


@app.get("/show_stat/{sync_type}")
async def show_stat(sync_type: str, response: Response):
    return show_stat_custom(sync_type)


def show_stat_custom(sync_type: str):
    result = None

    # construct call list
    resource_paths = [
        # {"resource": "MS-3-Subscription", "url": util.resources['subscription'] + "full"},
        {"resource": "MS-1-User", "url": util.resources['user']},
        {"resource": "MS-2-Report", "url": util.resources['report']}
    ]

    s_time = time.time()
    print("=== Request Order:", [res["resource"] for res in resource_paths], "===")
    util.response_order = []
    if sync_type == 'sync':
        print(' *** Running Sync Calls')
        result = sync_call.sync_request(resource_paths)
    else:
        print(' *** Running Async Calls')
        result = async_call.async_request(resource_paths)
    return_dict = {k: len(v) for (k, v) in result.items()}
    print("Result: ", return_dict)
    print("=== Response Order:", util.response_order, "===")
    print("Time used: ", time.time() - s_time)
    print()

    return return_dict


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8012)
