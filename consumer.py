import argparse, textwrap, boto3, time, json, logging

# parser = argparse.ArgumentParser()
# parser.parse_args()

print("boto3 is ready!")

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

if __name__ == "__main__":
    args = parse_args()
    print("Request bucket:", args.request_bucket)
    if args.widget_bucket:
        print("Storing widgets in S3 bucket:", args.widget_bucket)
    if args.dynamodb_widget_table:
        print("Storing widgets in DynamoDB table:", args.dynamodb_widget_table)