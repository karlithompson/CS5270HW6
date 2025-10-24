import argparse

# parser = argparse.ArgumentParser()
# parser.parse_args()

def parse_args():
    parser = argparse.ArgumentParser(description="Consumer Application for Widget Requests")

    parser.add_argument("--request-bucket", "-rb", type=str, required=True, metavar="BUCKET_NAME",
                        help="Name of the bucket that contains widget requests.") # reading a single widget requests from bucket 2 in key order
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    print("Request bucket:", args.request_bucket)
    if args.widget_bucket:
        print("Storing widgets in S3 bucket:", args.widget_bucket)
    if args.dynamodb_widget_table:
        print("Storing widgets in DynamoDB table:", args.dynamodb_widget_table)