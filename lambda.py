import json
import boto3
from pprint import pprint
from extract import (
    extract_text,
    map_word_id,
    extract_table_info,
    get_key_map,
    get_value_map,
    get_kv_map,
)


def lambda_handler(event, context):
    
    #Create Textract method
    textract = boto3.client("textract")
    
    # when a file is uploaded to the S3 Bucket "vdp-documentclassfication" an event will be triggered
    # Trigger is configured in configuration section of the Lamba service.
    if event:
        file_obj = event["Records"][0]
        print("*** Event Triggered ***")
        print(event["Records"][0])
        bucketname = str(file_obj["s3"]["bucket"]["name"])
        filename = str(file_obj["s3"]["object"]["key"])

        print(f"S3 Bucket from file accessed : {bucketname} ; File Name: {filename}")
        
        # S3 Bucket and File Name to textract's Analyze Document
        response = textract.analyze_document(
            Document={
                "S3Object": {
                    "Bucket": bucketname,
                    "Name": filename,
                }
            },
            FeatureTypes=["FORMS", "TABLES"],
        )

        print(json.dumps(response))

        raw_text = extract_text(response, extract_by="LINE")
        word_map = map_word_id(response)
        table = extract_table_info(response, word_map)
        key_map = get_key_map(response, word_map)
        value_map = get_value_map(response, word_map)
        final_map = get_kv_map(key_map, value_map)
        
        print("*** Jsonify Extracted Table records from Image")
        print(json.dumps(table))
        
        print("*** Jsonify Extracted Key Value Pairs from Image")
        print(json.dumps(final_map))
        print(raw_text)

    return {"statusCode": 200, "body": json.dumps("Document Processed")}