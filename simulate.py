import boto3
from botocore.config import Config
import json
import os
import subprocess
import logging
import time
from pathlib import Path
import shutil

S3_CLIENT_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"} 
)

def clean_download_dir(download_path, keep_dirs):
    for item in os.listdir(download_path):
        item_path = os.path.join(download_path, item)
        if item in keep_dirs:
            continue
        try:
            if os.path.isdir(item_path):
                logging.info(f"🧹 Removing directory: {item_path}")
                shutil.rmtree(item_path)
            else:
                logging.info(f"🧽 Removing file: {item_path}")
                os.remove(item_path)
        except Exception as e:
            logging.warning(f"⚠️ Failed to remove {item_path}: {e}")

def download_snapshot(s3, bucket, full_prefix, local_base_dir):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            s3_key = obj['Key']
            relative_path = s3_key[len(full_prefix):]
            if not relative_path:  # skip directory itself
                continue
            local_path = os.path.join(local_base_dir, relative_path)
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, s3_key, local_path)

def simulate_snapshot(snapshot_dir, first_slot, name, log_dir, repo_path, test_name):
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"{first_slot}_{test_name}.log" if test_name else f"{first_slot}.log"
    log_file_path = os.path.join(log_dir, log_filename)
    ledger_tool_path = os.path.join(repo_path, 'target', 'release', 'agave-ledger-tool')

    cmd = [
        ledger_tool_path,
        '-l', snapshot_dir,
        'simulate-block-production',
        '--first-simulated-slot', str(first_slot)
    ]

    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = f"{env.get('LD_LIBRARY_PATH', '')}:{os.path.join(repo_path, 'target/release')}"

    logging.info(f'Running simulation for {name}... Logging to {log_file_path}')

    with open(log_file_path, 'w') as log_file:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=None,
            env=env,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        try:
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                if "Sleeping a bit before signaling exit" in line:
                    logging.info(f"🟡 Detected shutdown log in {name}")
                    logging.info(f"🔴 Terminating {name} due to exit signal...")
                    time.sleep(2)
                    process.terminate()
                    process.wait()
                    break

            exit_code = process.wait()
            if exit_code == 0:
                logging.info(f"✅ Simulation completed for {name}")
            else:
                logging.error(f"❌ Simulation failed for {name} with exit code {exit_code}")

        except Exception as e:
            logging.error(f"❌ Error while running simulation for {name}: {e}")
            process.terminate()
            process.wait()

    # Cleanup
    try:
        cleanup_cmd = ["rm", "-rf", "accounts", "ledger_tool", "snapshot", "banking_retrace"]
        subprocess.run(cleanup_cmd, cwd=snapshot_dir)
        logging.info(f"🧹 Cleaned up snapshot directory for {name}")
    except Exception as e:
        logging.error(f"⚠️ Cleanup failed for {name}: {e}")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    
    with open("config.json") as f:
        config = json.load(f)

    session = boto3.Session()
    s3 = session.client('s3', config=S3_CLIENT_CONFIG)
    bucket = config['bucket']
    prefix = config['prefix']
    download_path = config['download_path']
    repo_path = config['test_repo_path']
    test_name = config.get('test_name', '')
    log_dir = os.path.join(repo_path, 'simulation_logs')

    keep_dirs = [entry["name"] for entry in config["directories"]]
    clean_download_dir(download_path, keep_dirs)
    
    for entry in config['directories']:
        name = entry['name']
        first_slot = entry['first_simulated_slot']
        full_prefix = prefix + name + "/"
        local_dir = os.path.join(download_path, name)

        if os.path.exists(local_dir):
            logging.info(f"⏭️ Skipping {name}: already exists at {local_dir}")
        else:
            logging.info(f"⬇️ Downloading snapshot {name} from S3...")
            download_snapshot(s3, bucket, full_prefix, local_dir)

        simulate_snapshot(local_dir, first_slot, name, log_dir, repo_path, test_name)

if __name__ == "__main__":
    main()
