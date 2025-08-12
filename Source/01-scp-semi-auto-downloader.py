# 1-scp-semi-auto-downloader.py
# This .py file is aiming at downloading files from remote server, since the relevant resources stored there.
# If you have all the dependencies saved locally, feel safe to ignore.
# Jeff He @ Apr. 8

import paramiko
from scp import SCPClient

host = 'host_addr'
port = port
username = 'usr_name'
password = 'passwd'


remote = 'remote_addr'
local = r"local_addr"

def create_ssh_client(hostname, port, usr, pwd):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, port, usr, pwd)
    return client

def download_files(client, remote_path, local_path):
    with SCPClient(client.get_transport()) as scp:
        for year in range(2014, 2015):
            for month in range(1, 13):
                strmonth = f"{month:02d}"
                remote_file = f"{remote_path}{year}-{strmonth}.csv"
                local_file = f"{local_path}{year}-{strmonth}.csv"
                try:
                    scp.get(remote_file, local_path=local_file)
                    print(f"Downloaded {remote_file} to {local_file}")
                except Exception as e:
                    print(f"Failed in downloading {remote_file}: {e}")

def main():
    ssh = create_ssh_client(host, port, username, password)
    download_files(ssh, remote, local)
    ssh.close()

if __name__ == '__main__':
    main()