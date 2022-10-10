#!/usr/bin/env python
import json
import random 
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"

# function to calculate randomized value. for a dummy concept of "strength" of that purchased item
def generate_random_value(maximum_allowed = 10, minimum_allowed = 0, mean = 5, sigma = 4):
    random_value = int(random.normalvariate(mean,sigma))
    if random_value >= maximum_allowed:
        random_value = maximum_allowed
    elif random_value <= minimum_allowed:
        random_value = minimum_allowed
    return str(random_value)


@app.route("/purchase_a_sword")
def purchase_a_sword():
    purchase_sword_event = {'event_type': 'purchase_sword', 
                            'description': 'large sword',
                            'strength': generate_random_value()}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/purchase_an_axe")
def purchase_an_axe():
    purchase_axe_event = {'event_type': 'purchase_axe',
                          'description': 'large axe',
                          'strength': generate_random_value()}
    log_to_kafka('events', purchase_axe_event)
    return "Axe Purchased!\n"

@app.route("/purchase_a_shield")
def purchase_a_shield():
    purchase_shield_event = {'event_type': 'purchase_shield',
                             'description': 'heavy shield',
                             'strength': generate_random_value()}
    log_to_kafka('events', purchase_shield_event)
    return "Axe Purchased!\n"

@app.route("/join_a_guild")
def join_a_guild():
    join_guild_event = {'event_type': 'join_guild',
                        'description': 'cool guild'}
    log_to_kafka('events', join_guild_event)
    return "Guild Joined!\n"

@app.route("/declare_peace")
def declare_peace():
    declare_peace_event = {'event_type': 'declare_peace',
                           'description': 'lastful peace'}
    log_to_kafka('events', declare_peace_event)
    return "Peace declared!\n"

@app.route("/declare_a_war")
def declare_a_war():
    declare_war_event = {'event_type': 'declare_war',
                         'description': 'destructive war'}
    log_to_kafka('events', declare_war_event)
    return "War declared!\n"
