from cryptography.hazmat.primitives import serialization

with open("/home/pranav_shirali/airflow-project/keys/snowflake_private_key.pem", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
    )

print(private_key)  # Should NOT be None
