import json
import logging
from contextlib import contextmanager

import fakeredis
import redis
from omni_pro_base.config import Config
from omni_pro_base.exceptions import NotFoundError
from omni_pro_base.util import nested

logger = logging.getLogger(__name__)


class RedisConnection:
    def __init__(self, host: str, port: int, db: int, redis_ssl: bool) -> None:
        self.host = host
        self.port = int(port)
        self.db = db
        self.redis_ssl = redis_ssl
        self.redis_client: redis.StrictRedis = None

    def __enter__(self) -> redis.StrictRedis:
        if not self.redis_client:
            self.redis_client = redis.StrictRedis(
                host=self.host, port=self.port, db=self.db, decode_responses=True, ssl=self.redis_ssl
            )
        return self.redis_client

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if not self.redis_client:
            self.redis_client.close()
            self.redis_client = None

    def change_db(self, new_db: int) -> None:
        self.db = new_db
        if self.redis_client:
            self.redis_client.select(new_db)


class RedisManager(object):
    def __init__(self, host: str, port: int, db: int, redis_ssl: bool) -> None:
        self._connection = RedisConnection(host=host, port=int(port), db=db, redis_ssl=redis_ssl)

    def get_connection(self) -> RedisConnection:
        return self._connection

    @contextmanager
    def use_db(self, new_db: int):
        original_db = self._connection.db
        try:
            if new_db != original_db:
                self._connection.change_db(new_db)
            yield self._connection
        finally:
            self._connection.change_db(original_db)

    def change_db(self, new_db: int):
        self._connection.change_db(new_db)

    def set_connection(self, connection: RedisConnection) -> None:
        self._connection = connection

    def set_json(self, key, json_obj):
        with self.get_connection() as rc:
            if isinstance(json_obj, str):
                json_obj = json.loads(json_obj)
            return rc.json().set(key, "$", json_obj)

    def get_json(self, key, *args, no_escape=False):
        with self.get_connection() as rc:
            result = rc.json().get(key, *args, no_escape=no_escape)
            if result is None:
                raise NotFoundError(f"Key {key} not found")
            return result

    def get_resource_config(self, service_id: str, tenant_code: str) -> dict:
        config = self.get_json(tenant_code)
        return {
            **nested(config, f"resources.{service_id}", {}),
            **nested(config, "aws", {}),
        }

    def get_aws_cognito_config(self, service_id: str, tenant_code: str) -> dict:
        config = self.get_resource_config(service_id, tenant_code)
        return {
            "region_name": nested(config, "aws.cognito.region"),
            "aws_access_key_id": config.get("aws_access_key_id"),
            "aws_secret_access_key": config.get("aws_secret_access_key"),
            "user_pool_id": nested(config, "aws.cognito.user_pool_id"),
            "client_id": nested(config, "aws.cognito.client_id"),
        }

    def get_aws_s3_config(self, service_id: str, tenant_code: str) -> dict:
        """
        Retrieves the configuration settings for an AWS S3 service based on a given service ID and tenant code.

        :param service_id: str
        The unique identifier for the service.

        :param tenant_code: str
        The code representing the tenant for which the configuration is required.

        :return: dict
        Returns a dictionary containing the S3 configuration, including region name, access key ID, secret access key, and bucket name.

        The method retrieves the configuration using `get_resource_config` and extracts S3-specific settings such as the region, access keys, and bucket name.
        """
        config = self.get_resource_config(service_id, tenant_code)
        return {
            "region_name": nested(config, "aws.s3.region"),
            "aws_access_key_id": config.get("aws_access_key_id"),
            "aws_secret_access_key": config.get("aws_secret_access_key"),
            "bucket_name": nested(config, "aws.s3.bucket_name"),
            "allowed_files": nested(config, "aws.s3.allowed_files") or [],
        }

    def get_mongodb_config(self, service_id: str, tenant_code: str) -> dict:
        config = self.get_resource_config(service_id, tenant_code)
        return {
            "host": nested(config, "dbs.mongodb.host"),
            "port": nested(config, "dbs.mongodb.port"),
            "user": nested(config, "dbs.mongodb.user"),
            "password": nested(config, "dbs.mongodb.pass"),
            "name": nested(config, "dbs.mongodb.name"),
            "complement": nested(config, "dbs.mongodb.complement"),
        }

    def get_postgres_config(self, service_id: str, tenant_code: str) -> dict:
        config = self.get_resource_config(service_id, tenant_code)
        return {
            "host": nested(config, "dbs.postgres.host"),
            "port": nested(config, "dbs.postgres.port"),
            "user": nested(config, "dbs.postgres.user"),
            "password": nested(config, "dbs.postgres.pass"),
            "name": nested(config, "dbs.postgres.name"),
        }

    def get_tenant_codes(self, pattern="*", exlcudes_keys=["SETTINGS"]) -> list:
        with self.get_connection() as rc:
            if self._connection.redis_ssl is False:
                return [key for key in rc.keys(pattern=pattern) if key not in exlcudes_keys]
            cursor = "0"
            keys = []
            while cursor != 0:
                cursor, next_keys = rc.scan(cursor=cursor, match=pattern)
                keys.extend(next_keys)
            return [key for key in keys if key not in exlcudes_keys]

    def get_user_admin(self, tenant):
        tenant_obj = self.get_json(tenant)
        return tenant_obj.get("user_admin") or {}

    def get_load_balancer_config(self, service_id, tennat):
        config = self.get_resource_config(service_id, tennat)
        return {
            "host": nested(config, "load_balancer"),
            "port": nested(config, "port"),
        }

    def get_airflow_config(self, service_id, tennat):
        config = self.get_resource_config(service_id, tennat)
        return {
            "host": nested(config, "airflow_host"),
            "username": nested(config, "username"),
            "password": nested(config, "password"),
        }

    def get_load_balancer_name(self, service_id, tennat):
        config = self.get_load_balancer_config(service_id, tennat)
        return f"{config.get('host')}:{config.get('port')}"

    def get_set(self, key: str):
        with self.get_connection() as rc:
            return rc.smembers(key)

    def set_set(self, key: str, *members):
        with self.get_connection() as rc:
            rc.delete(key)
            return rc.sadd(key, *members)

    def save_cache(self, hash_key: str, data: dict, expire=True) -> bool:
        """
        Save data to the Redis cache.

        Args:
            hash_key (str): The key to store the data in the cache.
            data (dict): The data to be stored in the cache.

        Returns:
            bool: True if the data was successfully saved to the cache, False otherwise.
        """
        with self.get_connection() as rc:
            ttl_before = rc.ttl(hash_key)
            response = rc.json().set(hash_key, "$", obj=data)
            ttl_after = rc.ttl(hash_key)
            if expire:
                rc.expire(hash_key, Config.EXPIRE_CACHE)
            elif ttl_after < 0:
                rc.expire(hash_key, ttl_before)
            return response

    def get_cache(self, hash_key: str) -> dict:
        """
        Retrieves the value associated with the given hash key from the cache.

        Args:
            hash_key (str): The key used to retrieve the value from the cache.

        Returns:
            dict: The value associated with the hash key, or None if the key is not found in the cache.
        """
        with self.get_connection() as rc:
            result = rc.json().get(hash_key)
            if result is None:
                return None
            return result

    def get_keys_with_prefix(self, prefix: str) -> list:
        """
        Retrieve all keys from Redis that match a given prefix and their associated data.
        This method uses the SCAN command to efficiently iterate through the keys in the Redis database
        that match the specified prefix. For each key found, it retrieves the associated data using the
        JSON.GET command and appends it to the result list if the data is not None.
        Args:
            prefix (str): The prefix to match keys against.
        Returns:
            list: A list of dictionaries where each dictionary contains a key and its associated data.
        """
        with self.get_connection() as rc:
            # Utilizamos SCAN para obtener las keys de forma eficiente
            cursor = "0"
            while cursor != 0:
                cursor, keys = rc.scan(cursor=cursor, match=f"{prefix}*")
                keys_with_data = [{key: rc.json().get(key)} for key in keys if rc.json().get(key) is not None]
        return keys_with_data

    def get_hashall(self, name: str):
        with self.get_connection() as rc:
            return rc.hgetall(name)

    def set_hast(self, name: str, key: str = None, value: str = None, mapping: dict = None, items: list = None):
        with self.get_connection() as rc:
            rc.delete(name)
            return rc.hset(name, key, value, mapping, items)

    def get_hash(self, name: str, key: str):
        with self.get_connection() as rc:
            return rc.hget(name, key)

    def get_multi_hash(self, name: str, keys: list, *args):
        with self.get_connection() as rc:
            return rc.hmget(name, keys, *args)

    def delete_hash(self, name: str):
        with self.get_connection() as rc:
            return rc.delete(name)


class FakeRedisServer:
    _instance = None

    @classmethod
    def get_instance(cls) -> fakeredis.FakeServer:
        if not cls._instance:
            cls._instance = cls._create_instance()
        return cls._instance

    @classmethod
    def _create_instance(cls) -> fakeredis.FakeServer:
        server = fakeredis.FakeServer()
        return server


# TODO: Remover las importaciones de RedisCache y RedisUsers
RedisCache = RedisManager
RedisUsers = RedisManager
