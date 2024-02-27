import json
import logging
import fakeredis
import redis
from omni_pro_base.exceptions import NotFoundError
from omni_pro_base.util import nested

logger = logging.getLogger(__name__)


class RedisConnection:
    def __init__(self, host: str, port: int, db: int, redis_ssl: bool) -> None:
        self.host = host
        self.port = int(port)
        self.db = db
        self.redis_ssl = redis_ssl

    def __enter__(self) -> redis.StrictRedis:
        self.redis_client = redis.StrictRedis(
            host=self.host, port=self.port, db=self.db, decode_responses=True, ssl=self.redis_ssl
        )
        return self.redis_client

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.redis_client.close()


class RedisManager(object):
    def __init__(self, host: str, port: int, db: int, redis_ssl: bool) -> None:
        self.host = host
        self.port = int(port)
        self.db = db
        self.redis_ssl = redis_ssl
        self._connection = RedisConnection(host=self.host, port=self.port, db=self.db, redis_ssl=self.redis_ssl)
        logger.info(f"RedisManager: {self.host}:{self.port}/{self.db}")
        with self._connection as rc:
            logger.info(f"RedisManager")
            logger.info(f"Redis PING: {rc.ping()}")

    def get_connection(self) -> RedisConnection:
        # if Config.TESTING:
        #     return fakeredis.FakeStrictRedis(
        #         server=FakeRedisServer.get_instance(),
        #         charset="utf-8",
        #         decode_responses=True,
        #     )
        return self._connection

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
        # logger.info(f"Redis config", extra={"deb_config": config})
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
            logger.info(f"RedisManager get_tenant_codes")
            if self.redis_ssl is False:
                logger.info(f"RedisManager redis_ssl is False")
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
