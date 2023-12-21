import json

response_order = None
resources = None


def init():
    global response_order
    global resources
    response_order = None
    with open("./resources.json", 'r') as openfile:
        resources = json.load(openfile)
    print("Resources: ", resources)
