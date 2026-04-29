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

from dag_tools.components.s3_sensor.sensor_component import S3SensorComponent
from dagster.components import ComponentLoadContext

class TestS3SensorComponent(unittest.TestCase):
    def test_default_sensor_name(self):
        component = S3SensorComponent(
            bucket="test-bucket",
            prefix="test-prefix",
            partition_name="test-partition",
            target_job="test_job",
            target_op="test_op"
        )
        # Mock ComponentLoadContext
        context = MagicMock(spec=ComponentLoadContext)
        defs = component.build_defs(context)
        
        # Verify the default name logic
        sensors = list(defs.sensors)
        self.assertEqual(len(sensors), 1)
        self.assertEqual(sensors[0].name, "test_prefix_s3_sensor")

    def test_custom_sensor_name(self):
        component = S3SensorComponent(
            bucket="test-bucket",
            prefix="",
            name="custom_sensor_name",
            partition_name="test-partition",
            target_job="test_job",
            target_op="test_op"
        )
        context = MagicMock(spec=ComponentLoadContext)
        defs = component.build_defs(context)
        
        # Verify the custom name overrides prefix
        sensors = list(defs.sensors)
        self.assertEqual(len(sensors), 1)
        self.assertEqual(sensors[0].name, "custom_sensor_name")

    def test_empty_prefix_default_name(self):
        component = S3SensorComponent(
            bucket="test-bucket",
            prefix="",
            partition_name="test-partition",
            target_job="test_job",
            target_op="test_op"
        )
        context = MagicMock(spec=ComponentLoadContext)
        defs = component.build_defs(context)
        
        # Verify empty prefix falls back to "s3_sensor"
        sensors = list(defs.sensors)
        self.assertEqual(len(sensors), 1)
        self.assertEqual(sensors[0].name, "s3_sensor")

    def test_custom_sensor_name_with_hyphens(self):
        component = S3SensorComponent(
            bucket="test-bucket",
            prefix="",
            name="custom-sensor-name",
            partition_name="test-partition",
            target_job="test_job",
            target_op="test_op"
        )
        context = MagicMock(spec=ComponentLoadContext)
        defs = component.build_defs(context)
        
        # Verify the custom name cleans hyphens
        sensors = list(defs.sensors)
        self.assertEqual(len(sensors), 1)
        self.assertEqual(sensors[0].name, "custom_sensor_name")

    @patch("dag_tools.components.s3_sensor.sensor_component.get_s3_keys")
    def test_sensor_partition_key_is_full_path(self, mock_get_s3_keys):
        from dagster import build_sensor_context
        
        component = S3SensorComponent(
            bucket="test-bucket",
            prefix="test-prefix",
            partition_name="test-partition",
            target_job="test_job",
            target_op="test_op"
        )
        context = MagicMock(spec=ComponentLoadContext)
        defs = component.build_defs(context)
        
        sensors = list(defs.sensors)
        self.assertEqual(len(sensors), 1)
        sensor_def = sensors[0]
        
        # Mock get_s3_keys to return a dummy file
        mock_get_s3_keys.return_value = {
            "test-prefix/folder/file.txt": {
                "Key": "test-prefix/folder/file.txt",
                "ETag": "dummy-etag"
            }
        }
        
        # Setup resources
        resource_key = "test_prefix_s3_sensor_resource"
        mock_resource = MagicMock()
        mock_resource.apply_filter.return_value = True
        
        sensor_context = build_sensor_context(
            resources={resource_key: mock_resource}
        )
        
        result = sensor_def._raw_fn(sensor_context)
        
        # Verify partition key is full path
        self.assertEqual(len(result.run_requests), 1)
        run_request = result.run_requests[0]
        self.assertEqual(run_request.partition_key, "test-prefix/folder/file.txt")
        
        # Verify dynamic partitions request
        self.assertEqual(len(result.dynamic_partitions_requests), 1)
        self.assertEqual(
            result.dynamic_partitions_requests[0].partition_keys,
            ["test-prefix/folder/file.txt"]
        )

if __name__ == "__main__":
    unittest.main()
