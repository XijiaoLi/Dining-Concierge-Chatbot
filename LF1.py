import json
import datetime
import time
import os
import logging
import re
import boto3
from botocore.exceptions import ClientError

queue_url = "https://sqs.us-east-1.amazonaws.com/121942057738/testqueue.fifo"


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    print('lambda')
    return dispatch(event)
    
    
def dispatch(intent_request):

    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'DiningSuggestionsIntent':
        return diningIntent(intent_request)
    elif intent_name == 'GreetingIntent':
        return greetingIntent()
    elif intent_name == 'ThankYouIntent':
        return thankYouIntent()

    raise Exception('Intent with name ' + intent_name + ' not supported')


def thankYouIntent():
    return {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
                "contentType": "PlainText",
                "content": "I am happy that I could assit you with this. :)"
                }
            }
    }

def greetingIntent():
    return {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
                "contentType": "PlainText",
                "content": "Hi there, how can I help?"
                }
            }
    }

def diningIntent(intent):
    
    slots = intent["currentIntent"]["slots"]
    source = intent["invocationSource"]
    print(source)
    
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    dining_date = slots["Date"]
    dining_time = slots["Time"]
    people = slots["PeopleNum"]
    phone = slots["Phone"]
    
    output_session_attributes = intent['sessionAttributes'] if intent['sessionAttributes'] is not None else {}
    
    if source == 'DialogCodeHook':
        validation_result = validate_dining(location, cuisine, dining_date, dining_time, people, phone)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                output_session_attributes,
                intent['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message'],
                ""
            )
        return delegate(output_session_attributes, slots)
        
    #if slots and location and cuisine and dining_date and dining_time and people and phone:
    push_message(intent)
    
    response = {
            "sessionAttributes":output_session_attributes,
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled",
                "message": {
                    "contentType": "PlainText",
                    "content": "You’re all set. Expect my recommendations shortly! Have a good day."
                }
            }
    }
    print("gh")
    return response


def push_message(intent):
    
    slots = intent["currentIntent"]["slots"]
    
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    dining_date = slots["Date"]
    dining_time = slots["Time"]
    people = slots["PeopleNum"]
    phone = slots["Phone"]
   
    message_attributes = {
        'Location': {
            'StringValue': location,
            'DataType': 'String.Location'
        },
        'Cuisine': {
            'StringValue': cuisine,
            'DataType': 'String.Cuisine'
        },
        'Date': {
            'StringValue': dining_date,
            'DataType': 'String.Date'
        },
        'DiningTime': {
            'StringValue': dining_time,
            'DataType': 'String.DiningTime'
        },
        'NumberOfPeople': {
            'StringValue': people,
            'DataType': 'Number.NumberOfPeople'
        },
        'PhoneNumber': {
            'StringValue': phone,
            'DataType': 'Number.PhoneNumber'
        }
    }
    
    sqs_client = boto3.client('sqs')
    
    try:
        msg = sqs_client.send_message(QueueUrl=queue_url,
                                      MessageBody='Dining Suggestions',
                                      MessageAttributes = message_attributes,
                                      MessageGroupId='userReq')
    except ClientError as e:
        logging.error(e)
        return None
    
    return msg
    
    
#-----------------------------HELPER FUNCTIONS-----------------------------
def build_validation_result(isValid, violatedSlot, msg):
    return {
        'isValid': isValid,
        'violatedSlot': violatedSlot,
        'message': {
            'contentType': 'PlainText', 
            'content': msg
        }
    }

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def validate_dining(location, cuisine, dining_date, dining_time, people, phone):
    
    res, err_slot, err_msg = False, None, None
    cuisineList = ["chinese", "american", "indian", "italian", "japanese", "korean", "mexican", "thai", "vegetarian"]
    locationList = ["new york", "manhattan", "brooklyn", "queens", "the bronx", "staten island"]
    
    if cuisine and cuisine.lower() not in cuisineList:
        err_slot = 'Cuisine'
        err_msg = 'Sorry, we currently do not support {} as a valid cuisine. Would you like to try a different one?'.format(cuisine)
    elif location and location.lower() not in locationList:
        err_slot = 'Location'
        err_msg = 'Sorry, We currently do not support {} as a valid location. Can you try a different one?'.format(location)
    elif dining_date and (dining_date < datetime.datetime.now().strftime('%Y-%m-%d')):
        err_slot = 'Date'
        err_msg = 'Sorry, the date has already passed. Can you try a different date?'
    elif dining_time and ((dining_date + "-" + dining_time) < datetime.datetime.now().strftime('%Y-%m-%d-%H:%M')):
        err_slot = 'Time'
        err_msg = 'Sorry, the time has already passed. Can you try a different time?'
    elif people and (not re.match(r'^\d+$', people)): 
        err_slot = 'PeopleNum'
        err_msg = 'Sorry, please provide a valid number.'
    elif phone and (not re.match(r'^\d{10}$', phone)): 
        err_slot = 'Phone'
        err_msg = 'Sorry, please provide valid U.S. phone number.'
    else:
        res = True
        
    return build_validation_result(res, err_slot, err_msg)