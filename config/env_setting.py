import subprocess

# Run hostname -I to get the container's IP address
result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)
output = result.stdout.strip()

# If multiple IP addresses are returned, take the first one
db_host = output.split()[0]

# Or run ifconfig, filter the output to get the line containing "192",
# then extract the IP address

# result = subprocess.run(['ifconfig'], capture_output=True, text=True)
# output_lines = result.stdout.split('\n')
# for line in output_lines:
#     if 'inet 192' in line:
#         db_host = line.split()[1]
#         break
# else:
#     db_host = ''

# Define the content of the .env file
env_content = f"""\
DB_USER=user123
DB_PASSWORD=pass123
DB_HOST={db_host}
DB_PORT=5432
DB_NAME=user123
PGADMIN_PORT=5050
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
"""

# Write content to .env file
with open('../.env', 'w') as env_file:
    env_file.write(env_content)

print(".env file has been created with DB_HOST set to: ", db_host)
