import os, sys, json
import unittest
import consumer
from consumer import store_dynamodb_widget, store_s3_widget, delete_widget,update_widget

# this helped me get more familiar with unittest: https://docs.python.org/3/library/unittest.html

class dynamoDB:
    def __init__(self):
        self.items = {}
    # save to a fake items dictionary instead of real dynamodb
    def put_item(self, TableName, Item):
        self.items[(TableName, Item["id"]["S"])] = Item
    def get_item(self, TableName, Key):
        return {"Item": self.items.get((TableName, Key["id"]["S"]))}
    def delete_item(self, TableName, Key):
        return self.items.pop((TableName, Key["id"]["S"]), None)
    def update_item(self, TableName, Key, AttributeUpdates):
        item_key = (TableName, Key["id"]["S"])
        item = self.items.get(item_key)

        if not item:
            return
        
        for attribute, action in AttributeUpdates.items():
            value = action.get("Value", None)

            if action.get("Action") == "DELETE":
                item.pop(attribute, None)
            else:
                item[attribute] = value
        self.items[item_key] = item
        return {"Attributes": item}
        

class s3Client:
    def __init__(self):
        self.objects = {}
    # save to a fake s3 objects dictionary instead of real s3
    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentType": ContentType
        }
    def delete_object(self, Bucket, Key):
        return self.objects.pop((Bucket, Key), None)
    def update_object(self, Bucket, Key, Body, ContentType):
        if (Bucket, Key) in self.objects:
            self.objects[(Bucket, Key)] = {
                "Body": Body,
                "ContentType": ContentType
            }

class testConsumer(unittest.TestCase):

    def test_update_dyanmodb(self):
        """
        I will be testing updating dynamo db
        """
        fake_dynamodb = dynamoDB()
        fake_dynamodb.items[("widgets", "test-123")] = {
            "id": {"S": "test-123"},
            "owner": {"S": "karli"},
            "label": {"S": "Old Label"},
            "description": {"S": "Old Description"}
        }

        request = {
            "type": "update",
            "requestId": "req-11",
            "widgetId": "test-123",
            "owner": "karli",
            "label": "New Label",
            "description": "New Description"
        }
    
        class Args:
            def __init__(self):
                self.widget_bucket = None
                self.dynamodb_widget_table = "widgets"
                self.region = "us-east-1"

        args = Args()

        # the below helps it to use the fake dynamodb instead of real one, kept getting errors due to wanting to use real dynamoDB so this is what I could find that worked.

        original_store = consumer.store_dynamodb_widget

        def fake_store_dynamodb_widget(request, table_name, region, dynamodb=None):
            return original_store(request, table_name, region, dynamodb=fake_dynamodb)
        
        consumer.store_dynamodb_widget = fake_store_dynamodb_widget

        consumer.update_widget(request, args)

        key = ("widgets", "test-123")
        self.assertIn(key, fake_dynamodb.items)
        updated = fake_dynamodb.items[key]
        self.assertEqual(updated["label"]["S"], "New Label")
        self.assertEqual(updated["description"]["S"], "New Description")


    def test_delete_dynamodb(self):
        """
        Test that widget is deleted successfully in dynamodb
        """
        fake_dynamodb = dynamoDB()
        fake_dynamodb.items[("widgets", "test-123")] = {
            "id": {"S": "test-123"},
            "owner": {"S": "karli"}
        }
        request = {
            "type": "delete",
            "requestId": "req-11",
            "widgetId": "test-123",
            "owner": "karli"
        }

        class Args:
            def __init__(self):
                self.widget_bucket = None
                self.dynamodb_widget_table = "widgets"
                self.region = "us-east-1"

        args = Args()

        delete_widget(request, args, dynamodb=fake_dynamodb)

        self.assertNotIn(("widgets", "test-123"), fake_dynamodb.items)

    def test_delete_s3(self):
        """
        Test that widget is deleted successfully in s3
        """
        fake_s3 = s3Client()
        fake_s3.objects[("my-widget-bucket", "widgets/karli/test-123.json")] = {
            "Body": "test content",
            "ContentType": "application/json"
        }
        request = {
            "type": "delete",
            "requestId": "req-11",
            "widgetId": "test-123",
            "owner": "karli"
        }

        class Args:
            def __init__(self):
                self.widget_bucket = "my-widget-bucket"
                self.dynamodb_widget_table = None
                self.region = "us-east-1"

        args = Args()

        delete_widget(request, args, dynamodb=None, s3=fake_s3)

        self.assertNotIn(("widgets", "test-123"), fake_s3.objects)

    def test_update_s3(self):
        """
        Test that widget is updated successfully in s3
        """
        fake_s3 = s3Client()
        fake_s3.objects[("my-widget-bucket", "widgets/karli/test-123.json")] = {
            "Body": "test content",
            "ContentType": "application/json"
        }
        request = {
            "type": "update",
            "requestId": "req-11",
            "widgetId": "test-123",
            "owner": "karli",
            "Body": "updated content"
        }

        class Args:
            def __init__(self):
                self.widget_bucket = "my-widget-bucket"
                self.dynamodb_widget_table = None
                self.region = "us-east-1"

        args = Args()

        # kept getting errors about args not being set, so set it here

        consumer.args = args # this helps so that the region doesn't creash

        original_store = consumer.store_s3_widget # my actual function

        def fake_store_s3_widget(request, bucket_name, s3=None):
            return original_store(request, bucket_name, s3=fake_s3)

        consumer.store_s3_widget = fake_store_s3_widget

        update_widget(request, args, dynamodb=None, s3=fake_s3) # this can now use the fake request

        consumer.store_s3_widget = original_store

        key = ("my-widget-bucket", "widgets/karli/test-123.json")
        self.assertIn(key, fake_s3.objects)
        saved = fake_s3.objects[key]
        body = saved["Body"].decode("utf-8") 
        self.assertIn("updated content", body)

    
    def test_sqs_to_s3(self):
        """
        Test that widget is stored in s3 from sqs message
        """
        
        s3 = s3Client()

        # this is like the queue
        message_body = json.dumps({
            "type": "create",
            "widgetId": "789",
            "owner": "sqs_owner",
            "label": "SQS Widget",
            "description": "A widget from SQS",
            "otherAttributes": [
                {"name": "color", "value": "blue"},
                {"name": "size", "value": "222"}
            ],
        })

        request = json.loads(message_body)
        store_s3_widget(request, "sqs-widget-bucket", s3=s3)

        key = ("sqs-widget-bucket", "widgets/sqs_owner/789.json")

        self.assertIn(key, s3.objects)

        saved = s3.objects[key]
        self.assertEqual(saved["Body"].decode("utf-8"), '{"type": "create", "widgetId": "789", "owner": "sqs_owner", "label": "SQS Widget", "description": "A widget from SQS", "otherAttributes": [{"name": "color", "value": "blue"}, {"name": "size", "value": "222"}]}')

    def test_sqs_to_dynamodb(self):
        """
        Test that widget is stored in dynamo from sqs message
        """
        
        fake_dynamoDB = dynamoDB()

        # this is like the queue
        message_body = json.dumps({
            "type": "create",
            "widgetId": "789",
            "owner": "sqs_owner",
            "label": "SQS Widget",
            "description": "A widget from SQS",
            "otherAttributes": [
                {"name": "color", "value": "blue"},
                {"name": "size", "value": "222"}
            ],
        })

        request = json.loads(message_body)
        store_dynamodb_widget(request, "sqs-widget-bucket", region = "us-east-1", dynamodb=fake_dynamoDB)

        key = ("sqs-widget-bucket", "789")

        self.assertIn(key, fake_dynamoDB.items)

        saved = fake_dynamoDB.items[key]
        self.assertEqual(saved["owner"]["S"], "sqs_owner")
        self.assertEqual(saved["label"]["S"], "SQS Widget")
        self.assertEqual(saved["description"]["S"], "A widget from SQS")
        self.assertEqual(saved["color"]["S"], "blue")
        self.assertEqual(saved["size"]["S"], "222")



    def test_Dynamodb(self):
        """
        Test that widget is made successfully
        """
        fake_dynamodb = dynamoDB()
        request = {
            "widgetId": "123",
            "owner": "test_owner",
            "label": "Test Widget",
            "description": "A widget for testing",
            "otherAttributes": [
                {"name": "color", "value": "red"},
                {"name": "size", "value": "111"}
            ],
        }

        # call my function to store in fake dynamodb
        store_dynamodb_widget(request, "widgets", region="us-east-1",dynamodb=fake_dynamodb)

        # verify that the item was stored correctly
        self.assertIn(("widgets", "123"), fake_dynamodb.items)

        saved_item = fake_dynamodb.items[("widgets", "123")]
        self.assertEqual(saved_item["owner"]["S"], "test_owner")
        self.assertEqual(saved_item["label"]["S"], "Test Widget")
        self.assertEqual(saved_item["description"]["S"], "A widget for testing")
        self.assertEqual(saved_item["color"]["S"], "red")
        self.assertEqual(saved_item["size"]["S"], "111")

    def test_Dynamodb_emptyListAttributes(self):
        """
        Tests that if otherAttributes is an empty list, no extra attributes are added
        """
        fake_dynamodb = dynamoDB()
        request = {
            "widgetId": "123",
            "owner": "test_owner",
            "label": "Test Widget",
            "description": "A widget for testing",
            "otherAttributes": [
            ],
        }

        store_dynamodb_widget(request, "widgets", region="us-east-1", dynamodb=fake_dynamodb)

        self.assertIn(("widgets", "123"), fake_dynamodb.items)

        saved_item = fake_dynamodb.items[("widgets", "123")]
        self.assertEqual(saved_item["owner"]["S"], "test_owner")
        self.assertEqual(saved_item["label"]["S"], "Test Widget")
        self.assertEqual(saved_item["description"]["S"], "A widget for testing")
        self.assertNotIn("color", saved_item)
        self.assertNotIn("size", saved_item)

    def test_invalid_dynamoDB(self):
        """
        Test that widget isn't made if no widgetId is provided
        """
        fake_dynamodb = dynamoDB()
        request = {
            "owner": "another_owner",
            "label": "Another Widget",
            "description": "Another widget for testing",
        }

        store_dynamodb_widget(request, "widgets", region="us-east-1", dynamodb=fake_dynamodb)

        self.assertEqual(len(fake_dynamodb.items), 0)

    def test_Dynamodb_badAttribute(self):
        """
        Test that invalid attributes in otherAttributes are skipped becasue they don't have a name
        """
        fake_dynamodb = dynamoDB()
        request = {
            "widgetId": "123",
            "owner": "test_owner",
            "label": "Test Widget",
            "description": "A widget for testing",
            "otherAttributes": [
                {"name": "color", "value": "red"},
                {"value": "111"}
            ],
        }

        store_dynamodb_widget(request, "widgets", region="us-east-1", dynamodb=fake_dynamodb)

        self.assertIn(("widgets", "123"), fake_dynamodb.items)

        saved_item = fake_dynamodb.items[("widgets", "123")]
        self.assertEqual(saved_item["owner"]["S"], "test_owner")
        self.assertEqual(saved_item["label"]["S"], "Test Widget")
        self.assertEqual(saved_item["description"]["S"], "A widget for testing")
        self.assertEqual(saved_item["color"]["S"], "red")
        self.assertNotIn("size", saved_item) 

    def test_s3(self):
        """
        Test that widget is made successfully
        """
        s3 = s3Client()

        request = {
            "widgetId": "456",
            "owner": "s3_owner",
            "label": "S3 Widget",
            "description": "A widget for S3 testing",
            "otherAttributes": [
                {"name": "color", "value": "red"},
                {"name": "size", "value": "111"}
            ],
        }

        store_s3_widget(request, "my-widget-bucket", s3=s3)

        self.assertIn(("my-widget-bucket", "widgets/s3_owner/456.json"), s3.objects)

        saved = s3.objects[("my-widget-bucket", "widgets/s3_owner/456.json")]
        self.assertEqual(saved["Body"].decode("utf-8"), '{"widgetId": "456", "owner": "s3_owner", "label": "S3 Widget", "description": "A widget for S3 testing", "otherAttributes": [{"name": "color", "value": "red"}, {"name": "size", "value": "111"}]}')

    def test_invalid_s3(self):
        """
        Test that widget isn't made if no widgetId is provided
        """
        s3 = s3Client()

        request = {
            "owner": "no_id_owner",
            "label": "No ID Widget",
            "description": "A widget without ID",
        }

        store_s3_widget(request, "my-widget-bucket", s3=s3)

        self.assertEqual(len(s3.objects), 0)

    

if __name__ == "__main__":
    unittest.main(verbosity=2)