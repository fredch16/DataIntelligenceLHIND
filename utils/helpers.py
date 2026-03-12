import os
import yaml

class LufthansaClient:
	def __init__(self, scope_name="lufthansa_scope"):
		self.base_url = "https://lh-proxy.onrender.com"

		self.client_secret = self._get_credentials(scope_name)

		self.base_volume = self._get_base_volume()

		self.headers = {"password": self.client_secret}

	def _get_credentials(self, scope):
		if "DATABRICKS_RUNTIME_VERSION" in os.environ:
			from pyspark.sql import SparkSession
			from pyspark.dbutils import DBUtils

			spark = SparkSession.builder.getOrCreate()
			dbutils = DBUtils(spark)
			proxy_pass = dbutils.secrets.get(scope="lufthansa_scope", key="client_secret")
			return proxy_pass
		else:
			script_dir = os.path.dirname(os.path.abspath(__file__))
			project_root = os.path.dirname(os.path.dirname(script_dir))
			config_path = os.path.join(project_root, "config.yaml")
			with open(config_path, 'r') as f:
				config = yaml.safe_load(f)
			return config["password"]
		
	def _get_base_volume(self):
		if "DATABRICKS_RUNTIME_VERSION" in os.environ:
			return "/Volumes/main/lufthansa/landing_zone"
		script_dir = os.path.dirname(os.path.abspath(__file__))
		project_root = os.path.dirname(os.path.dirname(script_dir))
		return os.path.join(project_root, "outputs")
