import argparse, textwrap, boto3, time, json, logging

def parse_args():
    parser = argparse.ArgumentParser(
        description="Command-line arguments (both short and long styles):",
        epilog=textwrap.dedent('''\
            Examples:
              python consumer.py -rb usu-cs5250-blue-requests -wb usu-cs5250-blue-web
              python consumer.py --request-bucket=usu-cs5250-blue-requests --widget-bucket=usu-cs5250-blue-web
              python consumer.py -rb my-requests -dwt widgets
        '''),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--request-bucket", "-rb", type=str, required=True, metavar="BUCKET_NAME", help="Name of the bucket that contains widget requests.") # reading a single widget requests from bucket 2 in key order
    
    parser.add_argument("--widget-bucket", "-wb", type=str, metavar="BUCKET_NAME",
                        help="Name of the S3 bucket that holds the widgets")

    parser.add_argument("--dynamodb-widget-table", "-dwt", type=str, metavar="BUCKET_NAME",
                        help="Name of the DynamoDB table that holds widgets")  
    
    args = parser.parse_args()

    if not args.widget_bucket and not args.dynamodb_widget_table:
        parser.error("You must specify either --widget-bucket OR --dynamodb-widget-table")

    if args.widget_bucket and args.dynamodb_widget_table:
        parser.error("You cannot specify both --widget-bucket AND --dynamodb-widget-table")
    
    return args

def poll_requests(bucket_name):
    s3 =boto3.client('s3')

    while True:
        response = s3.list_objects_v2(Bucket=bucket_name)

        if "Contents" in response:
            obj = response["Contents"][0]
            key = obj["Key"]

            req_obj = s3.get_object(Bucket=bucket_name, Key=key)
            body = req_obj["Body"].read().decode("utf-8")

            try:
                request = json.loads(body)
                print("Processing request:", request)
                if args.widget_bucket:
                    print("Storing widget in S3 bucket:", args.widget_bucket)
                    widget_s3 = boto3.client('s3')

                    try:
                        widget_id = request.get("widgetId", "unknown")

                        owner = request.get("owner", "unknown_owner").replace(" ", "_")

                        new_key = f"widgets/{owner}/{widget_id}.json"

                        print(f"Uploading widget to s3://{args.widget_bucket}/{key}")

                        widget_s3.put_object(
                            Bucket=args.widget_bucket,
                            Key=new_key,
                            Body=json.dumps(request).encode("utf-8"),
                            ContentType='application/json'
                        )
                        print(f"Stored widget {widget_id} in S3 bucket {args.widget_bucket} with key {key}\n")
                    except Exception as e:
                        print("Error storing widget in S3:", e)
                    
                elif args.dynamodb_widget_table:
                    dynamodb = boto3.client('dynamodb')
                    try:
                        item = {}

                        if "widgetId" not in request:
                            print("Error: request missing widgetId, skipping")
                            return
                        item["id"] = {'S': str(request["widgetId"])}
                        if "owner" in request:
                            item["owner"] = {'S': str(request["owner"])}
                        if "label" in request:
                            item["label"] = {'S': str(request["label"])}
                        if "description" in request:    
                            item["description"] = {'S': str(request["description"])}

                        if "otherAttributes" in request:
                            for attribute in request["otherAttributes"]:
                                name = attribute.get("name")
                                val = attribute.get("value")
                                if not name or not val:
                                    print(f"Skipping invalid attribute: {attribute}")
                                    continue
                                item[name] = {'S': str(val)}

                        for k, v in request.items():
                            if v is None or k in ["widgetId", "owner", "label", "description", "otherAttributes"]:
                                continue
                            item[k] = {'S': str(v)}

                        print("Final item to store in DynamoDB:", item)
                        dynamodb.put_item(
                            TableName=args.dynamodb_widget_table,
                            Item = item
                        )
                        print(f"Stored widget {request.get('widgetId')} with {len(item)} attributes in DynamoDB table {args.dynamodb_widget_table}\n")
                    except Exception as e:
                        print("Error storing widget in DynamoDB:", e)
                    
            except json.JSONDecodeError:
                print("Error: Object was not valid JSON")
                print("Raw body:", body)

            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted {key} from {bucket_name}")
        else:
            print("No requests found. Waiting...")
            time.sleep(.01)



if __name__ == "__main__":
    args = parse_args()
    print("Request bucket:", args.request_bucket)
    if args.widget_bucket:
        print("Storing widgets in S3 bucket:", args.widget_bucket)
    if args.dynamodb_widget_table:
        print("Storing widgets in DynamoDB table:", args.dynamodb_widget_table)

    poll_requests(args.request_bucket)