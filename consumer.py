import argparse, textwrap, boto3, time, json, logging, sys

# I found documentation from argparse that was helpful in my implemenmtation since I've never done this before. https://docs.python.org/3/howto/argparse.html
# This website helped me with the logging: https://docs.python.org/3/howto/logging.html
# This website helps a lot with using boto3 to store the widgets: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html

logging.basicConfig(
    filename="consumer.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Command-line arguments (both short and long styles):",
        epilog=textwrap.dedent('''\
            Examples:
              python consumer.py -rb usu-cs5250-blue-requests -wb usu-cs5250-blue-web
              python consumer.py --request-bucket=usu-cs5250-blue-requests --widget-bucket=usu-cs5250-blue-web
              python consumer.py -rb my-requests -dwt widgets
                               
              add --region REGION if not using default region us-east-1
        '''),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--request-bucket", "-rb", type=str, required=True, metavar="BUCKET_NAME", help="Name of the bucket that contains widget requests.") # reading a single widget requests from bucket 2 in key order
    
    parser.add_argument("--widget-bucket", "-wb", type=str, metavar="BUCKET_NAME",
                        help="Name of the S3 bucket that holds the widgets")

    parser.add_argument("--dynamodb-widget-table", "-dwt", type=str, metavar="BUCKET_NAME",
                        help="Name of the DynamoDB table that holds widgets")  
    
    parser.add_argument("--region", "-r", type=str, default="us-east-1", help="AWS region (default: us-east-1)")
    
    args = parser.parse_args()

    # They also need to add args for either a s3 or dynamodb widget to store widget, but not both.
    if not args.widget_bucket and not args.dynamodb_widget_table:
        parser.error("You must specify either --widget-bucket OR --dynamodb-widget-table")

    if args.widget_bucket and args.dynamodb_widget_table:
        parser.error("You cannot specify both --widget-bucket AND --dynamodb-widget-table")
    
    return args

def fetch_widget_request(s3, bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    if "Contents" not in response:
            return None, None
    key = response["Contents"][0]["Key"]
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return key, body

def store_s3_widget(request, bucket_name, s3=None):
    s3 = s3 or boto3.client('s3', region_name=args.region)
    if "widgetId" not in request:
        logging.error("Request missing widgetId, skipping")
        return
    widget_id = request["widgetId"]
    owner = request.get("owner", "unknown_owner").replace(" ", "_")
    key = f"widgets/{owner}/{widget_id}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(request).encode("utf-8"),
        ContentType='application/json'
    )
    logging.info(f"Stored widget {widget_id} in S3 bucket {bucket_name} with key {key}")

def store_dynamodb_widget(request, table_name, region, dynamodb=None):
    dynamodb = dynamodb or boto3.client('dynamodb', region_name=region)
    item = {}
    if "widgetId" not in request:
        logging.error("Request missing widgetId, skipping")
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
                logging.warning(f"Skipping invalid attribute: {attribute}")
                continue
            item[name] = {'S': str(val)}
    for k, v in request.items():
        if v is None or k in ["widgetId", "owner", "label", "description", "otherAttributes"]:
            continue
        item[k] = {'S': str(v)}
    logging.info(f"Final item to store in DynamoDB: {item}")
    dynamodb.put_item(
        TableName=table_name,
        Item=item
    )
    logging.info(f"Stored widget {request.get('widgetId')} with {len(item)} attributes in DynamoDB table {table_name}")

    # I was struggling to know if it was actually writing to DynamoDB, so I added this verification step. This helps me know it actually wrote.
    resp = dynamodb.get_item(
        TableName=table_name,
            Key={"id": {"S": str(request["widgetId"])}}
    )
    logging.info(f"Verified DynamoDB insert: {resp.get('Item')}")

def poll_requests(bucket_name, args):
    s3 =boto3.client('s3')
    idle_timeout = 30      # seconds
    poll_interval = 1.0    # wait 1 s between empty checks
    last_activity = time.time()

    try:
        while True:
            key,body=fetch_widget_request(s3,bucket_name)
            if not key:
                if time.time() - last_activity > idle_timeout:
                    logging.info(f"No new requests for {idle_timeout} seconds. Stopping consumer.")
                    break
                logging.info("No requests found. Waiting...")
                time.sleep(poll_interval)
                continue
            try:
                request = json.loads(body)
                logging.info(f"Processing request: {request}")

                if args.widget_bucket:
                    logging.info(f"Storing widget in S3 bucket: {args.widget_bucket}")
                    store_s3_widget(request, args.widget_bucket)
                elif args.dynamodb_widget_table:
                    logging.info(f"Storing widget in DynamoDB table: {args.dynamodb_widget_table}")
                    store_dynamodb_widget(request, args.dynamodb_widget_table, args.region)
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON in {key}: {body}")
            
            s3.delete_object(Bucket=bucket_name, Key=key)
            logging.info(f"Deleted {key} from {bucket_name}")

    # I am having it so the only way it stops is when a user interrupts it.
    except KeyboardInterrupt:
        logging.info("Polling interrupted by user.")
        sys.exit(0)

if __name__ == "__main__":
    args = parse_args()
    logging.info("Starting consumer with request bucket: %s", args.request_bucket)
    poll_requests(args.request_bucket,args)