import os

# Setting Environment Variables
# os.environ["exec_env"] = "TEST"
os.environ["exec_env"] = "PROD"
os.environ["postgres_username"] = "sparkuser"
os.environ["postgres_password"] = "user123"

# Getting Environment Variables
exec_env = os.environ["exec_env"]
postgres_username = os.environ["postgres_username"]
postgres_password = os.environ["postgres_password"]

# Set Other Variables
appName = "USAMedPrescriberDataPipeline"
current_path = os.getcwd()
# staging_dim_city = current_path + '\..\staging\dimension_city'
# staging_fact = current_path + '\..\staging\\fact'

header = True
inferSchema = True

staging_city = "prescriber_pipeline/staging/city_dimension"
staging_presc = "prescriber_pipeline/staging/prescriber_fact"

output_city="prescriber_pipeline/output/city_dimension"
output_presc="prescriber_pipeline/output/prescriber_fact"

postgres_jdbc_url="jdbc:postgresql://localhost:6432/prescriber"
