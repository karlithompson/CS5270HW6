import os, sys, json
import unittest
from consumer import store_dynamodb_widget, store_s3_widget

# this helped me get more familiar with unittest: https://docs.python.org/3/library/unittest.html

class dynamoDB:
    def __init__(self):
        self.items = {}
    # save to a fake items dictionary instead of real dynamodb
    def put_item(self, TableName, Item):
        self.items[(TableName, Item["id"]["S"])] = Item
    def get_item(self, TableName, Key):
        return {"Item": self.items.get((TableName, Key["id"]["S"]))}

class s3Client:
    def __init__(self):
        self.objects = {}
    # save to a fake s3 objects dictionary instead of real s3
    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentType": ContentType
        }

class testConsumer(unittest.TestCase):
    
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