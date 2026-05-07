#!/usr/bin/env python3
import os
import sys
import json
import subprocess
from pathlib import Path

import yaml

CONFIG_PATH = Path(os.getenv("DBT_COMPILE_CONFIG", "dbt_compile_config.yaml"))

# Initialize defaults
config_data = {}
if CONFIG_PATH.exists():
    try:
        with open(CONFIG_PATH, 'r') as f:
            config_data = yaml.safe_load(f) or {}
            print(f"📖 Loaded configuration from {CONFIG_PATH}")
    except Exception as e:
        print(f"⚠️ Error loading {CONFIG_PATH}: {e}")
        sys.exit(1)
else:
    print(f"⚠️ Warning: Configuration file {CONFIG_PATH} not found. Skipping dynamic compilation.")
    sys.exit(0)

# --- Configuration Resolution (Env Var > YAML Config > Default) ---
DBT_ASSETS_FILE = os.getenv(
    "DBT_ASSETS_FILE", 
    config_data.get("dbt_assets_file", "orch/assets/dbt_assets.py")
)
MANIFEST_PATH = Path(os.getenv(
    "DBT_MANIFEST_PATH", 
    config_data.get("manifest_path", "target/manifest.json")
))
ENVIRONMENTS = config_data.get("environments", [])

if not ENVIRONMENTS:
    print("⚠️ No environments defined in config. Skipping compilation.")
    sys.exit(0)

def compile_dbt_environments():
    """Iterates through the environment configs and compiles the dbt project."""
    print("="*60)
    print("🚀 STARTING DYNAMIC DBT COMPILATION")
    print("="*60)

    for env_config in ENVIRONMENTS:
        env_name = env_config["name"]
        print(f"\n📦 Packaging dbt manifest for: {env_name}")
        
        # Copy the current system environment and inject the target-specific vars
        run_env = os.environ.copy()
        run_env.update(env_config["env_vars"])
        
        # Ensure we are running from the directory containing the dbt project
        # (Assuming the script is run from the project root)
        cmd = [
            "dagster-dbt", 
            "project", 
            "prepare-and-package", 
            "--file", 
            DBT_ASSETS_FILE
        ]
        
        try:
            # Execute the compilation
            result = subprocess.run(cmd, env=run_env, check=True, text=True, capture_output=True)
            print(f"✅ Successfully packaged {env_name}.")
        except subprocess.CalledProcessError as e:
            print(f"❌ COMPILATION FAILED FOR {env_name}")
            print(f"Error Output:\n{e.stderr}")
            sys.exit(1)

def validate_ontology_tags():
    """Parses the generated manifest.json to enforce semantic tagging."""
    print("\n" + "="*60)
    print("🛡️ ENFORCING ENTERPRISE ONTOLOGY COMPLIANCE")
    print("="*60)

    if not MANIFEST_PATH.exists():
        print(f"❌ BUILD FAILED: manifest.json not found at {MANIFEST_PATH}")
        sys.exit(1)

    with open(MANIFEST_PATH, 'r') as f:
        manifest = json.load(f)

    missing_tags = []
    
    for node_id, node_data in manifest.get("nodes", {}).items():
        if node_data.get("resource_type") == "model":
            meta = node_data.get("meta", {})
            if "ontology_uri" not in meta:
                missing_tags.append(node_data.get("name"))

    if missing_tags:
        print("🚨 COMPLIANCE VIOLATION 🚨")
        print("The following dbt models are missing the 'ontology_uri' meta tag:")
        for model in missing_tags:
            print(f"  - {model}")
        print("\nAll production analytics models must be mapped to the Enterprise Ontology.")
        print("Please add 'meta: { ontology_uri: <URI> }' to your schema.yml files.")
        sys.exit(1)

    print("✅ Ontology compliance check passed. All models are semantically bound.")

if __name__ == "__main__":
    compile_dbt_environments()
    # We only need to validate the manifest once, as the models/tags are 
    # identical across environments; only the target DB changes.
    validate_ontology_tags()
    sys.exit(0)
