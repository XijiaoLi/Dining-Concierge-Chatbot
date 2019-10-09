import json
import time
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    
    client = boto3.client('lex-runtime')
    result = []
    
    lex_response = client.post_text(
        botName = 'Dining_Concierge_chatbot',
        botAlias = 'Koko',
        userId = event['body-json']['messages'][0]['unstructured']['id'],
        inputText = event['body-json']['messages'][0]['unstructured']['text']
    )
    #event['body-json']['messages'][0]['unstructured']['text']
    print(lex_response)
    
    current_time = time.localtime()
    now = time.strftime('%m-%d-%Y %H:%M:%S', current_time)
    msg = {
        "type": "string",
        "unstructured": {
            "id": event['body-json']['messages'][0]['unstructured']['id'],
            "text": lex_response,
            "timestamp": now
      }
    }
    
    result.append(msg)
    '''
    return {
        'statusCode':200,
        'body':{
        'messages': result}
    }
    '''
    
    a = json.dumps(event)
    b = json.loads(a)
    
    return {"messages":lex_response['message']}
        