import json
import base64
import datetime

def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        data_json = json.loads(payload)
        
        # Add processing timestamp
        processing_time = datetime.datetime.now(datetime.timezone.utc)
        
        # Create the partition key (YYYY-MM-DD format)
        partition_date = processing_time.strftime('%Y-%m-%d')
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((json.dumps(data_json) + '\n').encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'processing_date': partition_date
                }
            }
        }
        output.append(output_record)
    
    return {'records': output}