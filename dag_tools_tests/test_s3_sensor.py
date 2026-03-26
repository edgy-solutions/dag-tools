import unittest
from unittest.mock import MagicMock, patch
from dagster_aws.s3 import S3Resource
from dag_tools.resources.s3 import S3ResourceConfig, S3SensorResource

class TestS3SensorResource(unittest.TestCase):
    def setUp(self):
        self.s3_resource = S3Resource(
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1"
        )
        self.config = S3ResourceConfig(
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_resource=self.s3_resource,
            s3_filter=".*\\.parquet"
        )
        self.resource = S3SensorResource(config=self.config)

    def test_get_client(self):
        with patch.object(S3Resource, "get_client") as mock_get_client:
            self.resource.get_client()
            mock_get_client.assert_called_once()

    def test_apply_filter_directories(self):
        self.assertFalse(self.resource.apply_filter("some/directory/"))

    def test_apply_filter_metadata(self):
        self.assertFalse(self.resource.apply_filter("some/file.metadata"))
        self.assertFalse(self.resource.apply_filter("some/_SUCCESS"))

    def test_apply_filter_regex(self):
        # Should match .parquet based on setup
        self.assertTrue(self.resource.apply_filter("data/file.parquet"))
        # Should NOT match .txt
        self.assertFalse(self.resource.apply_filter("data/file.txt"))

    def test_apply_filter_no_regex(self):
        config_no_filter = S3ResourceConfig(
            s3_bucket="test-bucket",
            s3_prefix="test-prefix",
            s3_resource=self.s3_resource
        )
        resource_no_filter = S3SensorResource(config=config_no_filter)
        self.assertTrue(resource_no_filter.apply_filter("data/file.txt"))

if __name__ == "__main__":
    unittest.main()
