import json
import unittest
from unittest.mock import MagicMock, patch

from omni_pro_base.exceptions import NotFoundError
from omni_pro_redis.redis import FakeRedisServer, RedisCache, RedisConnection, RedisManager


class TestRedis(unittest.TestCase):

    def setUp(self):
        self.host = "localhost"
        self.port = 6379
        self.db = 0
        self.ssl = False
        self.redis_connection = RedisConnection(self.host, self.port, self.db, self.ssl)
        self.redis_manager = RedisManager(self.host, self.port, self.db, self.ssl)
        self.redis_cache = RedisCache(self.host, self.port, self.db, self.ssl)

    @patch("redis.StrictRedis")
    def test_redis_connection(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        with self.redis_connection as redis_client:
            self.assertEqual(redis_client, mock_redis_client)
            mock_redis.assert_called_once_with(host="localhost", port=6379, db=0, decode_responses=True, ssl=False)
            mock_redis_client.close.assert_not_called()

    @patch("redis.StrictRedis")
    def test_redis_connection_close(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        with self.redis_connection as redis_client:
            pass

        mock_redis_client.close.assert_called_once()

    @patch("omni_pro_redis.redis.RedisConnection")
    def test_get_connection_redis(self, mock_redis_connection):
        mock_redis_client = MagicMock()
        mock_redis_connection.return_value = mock_redis_client

        redis_manager = RedisManager(self.host, self.port, self.db, self.ssl)
        connection = redis_manager.get_connection()

        mock_redis_connection.assert_called_once_with(host=self.host, port=self.port, db=self.db, redis_ssl=self.ssl)
        self.assertEqual(connection, mock_redis_client)

    @patch("redis.StrictRedis")
    def test_set_json_redis(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        key = "key"
        json_obj = {"key": "value"}

        result = self.redis_manager.set_json(key, json_obj)

        self.assertEqual(result, mock_redis_client.json().set.return_value)
        mock_redis_client.json().set.assert_called_once_with(key, "$", json_obj)

    @patch("redis.StrictRedis")
    def test_set_json_isinstance_str(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        key = "key"
        json_obj = '{"key": "value"}'

        result = self.redis_manager.set_json(key, json_obj)

        self.assertEqual(result, mock_redis_client.json().set.return_value)
        mock_redis_client.json().set.assert_called_once_with(key, "$", json.loads(json_obj))

    @patch("redis.StrictRedis")
    def test_get_json_raise_not_found_error(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        key = "key"
        mock_redis_client.json().get.return_value = None

        with self.assertRaises(NotFoundError):
            self.redis_manager.get_json(key)

        mock_redis_client.json().get.assert_called_once_with(key, no_escape=False)

    @patch("redis.StrictRedis")
    def test_get_json(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        key = "key"
        json_obj = {"key": "value"}
        mock_redis_client.json().get.return_value = json_obj

        result = self.redis_manager.get_json(key)

        self.assertEqual(result, json_obj)
        mock_redis_client.json().get.assert_called_once_with(key, no_escape=False)

    @patch("redis.StrictRedis")
    def test_get_resource_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant_code = "tenant_code"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_resource_config(service_id, tenant_code)

        expected = {
            **config["resources"][service_id],
            **config["aws"],
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant_code)

    @patch("redis.StrictRedis")
    def test_get_aws_cognito_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant_code = "tenant_code"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_aws_cognito_config(service_id, tenant_code)

        expected = {
            "region_name": None,
            "aws_access_key_id": None,
            "aws_secret_access_key": None,
            "client_id": None,
            "user_pool_id": None,
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant_code)

    @patch("redis.StrictRedis")
    def test_get_aws_s3_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant_code = "tenant_code"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_aws_s3_config(service_id, tenant_code)

        expected = {
            "region_name": None,
            "aws_access_key_id": None,
            "aws_secret_access_key": None,
            "bucket_name": None,
            "allowed_files": [],
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant_code)

    @patch("redis.StrictRedis")
    def test_get_mongodb_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant_code = "tenant_code"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_mongodb_config(service_id, tenant_code)

        expected = {
            "host": None,
            "port": None,
            "user": None,
            "password": None,
            "name": None,
            "complement": None,
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant_code)

    @patch("redis.StrictRedis")
    def test_get_postgres_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant_code = "tenant_code"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_postgres_config(service_id, tenant_code)

        expected = {
            "host": None,
            "port": None,
            "user": None,
            "password": None,
            "name": None,
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant_code)

    @patch("redis.StrictRedis")
    def test_get_tenant_codes(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        pattern = "*"
        excludes_keys = ["SETTINGS"]
        mock_redis_client.keys.return_value = ["key"]

        result = self.redis_manager.get_tenant_codes(pattern, excludes_keys)

        expected = ["key"]
        self.assertEqual(result, expected)

    @patch("redis.StrictRedis")
    def test_get_user_admin(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        tenant = "tenant"
        user_admin = {"user_admin": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=user_admin)

        result = self.redis_manager.get_user_admin(tenant)

        expected = user_admin["user_admin"]

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant)

    @patch("redis.StrictRedis")
    def test_get_load_balancer_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant = "tenant"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_load_balancer_config(service_id, tenant)

        expected = {
            "host": None,
            "port": None,
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant)

    @patch("redis.StrictRedis")
    def test_get_airflow_config(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant = "tenant"
        config = {"resources": {service_id: {"key": "value"}}, "aws": {"key": "value"}}
        self.redis_manager.get_json = MagicMock(return_value=config)

        result = self.redis_manager.get_airflow_config(service_id, tenant)

        expected = {
            "host": None,
            "username": None,
            "password": None,
        }

        self.assertEqual(result, expected)
        self.redis_manager.get_json.assert_called_once_with(tenant)

    @patch("redis.StrictRedis")
    def test_get_load_balancer_name(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        service_id = "service_id"
        tenant = "tenant"
        self.redis_manager.get_load_balancer_config = MagicMock(return_value={"host": "host", "port": "port"})

        result = self.redis_manager.get_load_balancer_name(service_id, tenant)

        expected = "host:port"

        self.assertEqual(result, expected)
        self.redis_manager.get_load_balancer_config.assert_called_once_with(service_id, tenant)

    @patch("fakeredis.FakeServer")
    def test_fake_get_instance(self, mock_fake_server):
        server_instance = FakeRedisServer._create_instance()
        mock_fake_server.assert_called_once()
        self.assertEqual(server_instance, mock_fake_server.return_value)

    def test_get_instance_returns_singleton(self):
        instance1 = FakeRedisServer.get_instance()
        instance2 = FakeRedisServer.get_instance()
        self.assertEqual(instance1, instance2)

    @patch("omni_pro_redis.redis.RedisConnection")
    def test_redis_cache_get_connection(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        redis_cache = RedisCache(self.host, self.port, self.db, self.ssl)
        connection = redis_cache.get_connection()

        mock_redis.assert_called_once_with(host=self.host, port=self.port, db=self.db, redis_ssl=self.ssl)
        self.assertEqual(connection, mock_redis_client)

    @patch("redis.StrictRedis")
    def test_redis_chache_save_cache_expire(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        hash_key = "hash_key"
        data = {"key": "value"}
        expire = True

        result = self.redis_cache.save_cache(hash_key, data, expire)

        self.assertEqual(result, mock_redis_client.json().set.return_value)
        mock_redis_client.json().set.assert_called_once_with(hash_key, "$", obj=data)

    @patch("redis.StrictRedis")
    def test_redis_cache_save_cache_no_expire(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        hash_key = "hash_key"
        data = {"key": "value"}
        expire = False

        result = self.redis_cache.save_cache(hash_key, data, expire)

        self.assertEqual(result, mock_redis_client.json().set.return_value)
        mock_redis_client.json().set.assert_called_once_with(hash_key, "$", obj=data)

    @patch("redis.StrictRedis")
    def test_redis_cache_get_cache(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        hash_key = "hash_key"
        data = {"key": "value"}
        mock_redis_client.json().get.return_value = data

        result = self.redis_cache.get_cache(hash_key)

        self.assertEqual(result, data)
        mock_redis_client.json().get.assert_called_once_with(hash_key)

    @patch("redis.StrictRedis")
    def test_redis_cache_get_cache_not_result(self, mock_redis):
        mock_redis_client = MagicMock()
        mock_redis.return_value = mock_redis_client

        hash_key = None
        data = None
        mock_redis_client.json().get.return_value = data

        result = self.redis_cache.get_cache(hash_key)
        self.assertEqual(result, data)
        mock_redis_client.json().get.assert_called_once_with(hash_key)


if __name__ == "__main__":
    unittest.main()
