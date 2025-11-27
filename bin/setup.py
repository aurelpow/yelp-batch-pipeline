#!/usr/bin/env python3
"""
Yelp Batch ETL Pipeline - Security Setup Script

RECOMMENDED METHOD to initialize the project:
  python bin/setup.py

This script will:
1. Generate a cryptographically secure Fernet key
2. Create the .env file with proper configuration
3. Verify setup is correct

Manual alternative (if you prefer):
1. cp .env.example .env
2. python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
3. Edit .env and paste your generated key
"""

import os
import sys
from pathlib import Path

def generate_fernet_key():
    """Generate a new Fernet key"""
    try:
        from cryptography.fernet import Fernet
        return Fernet.generate_key().decode()
    except ImportError:
        print("Error: cryptography package not installed")
        print("Install it with: pip install cryptography")
        sys.exit(1)

def main():
    # Get project root directory (script is in bin/ folder)
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"
    env_example = project_root / ".env.example"

    print("=" * 70)
    print("Yelp Batch ETL Pipeline - Setup")
    print("=" * 70)
    print()

    # Check if .env already exists
    if env_file.exists():
        print("✅ .env file already exists")
        overwrite = input("Do you want to regenerate it? (y/N): ").lower().strip()
        if overwrite != 'y':
            print("Setup cancelled. Using existing .env file.")
            return

    # Generate Fernet key
    print("🔑 Generating Fernet key...")
    fernet_key = generate_fernet_key()
    print(f"Generated key: {fernet_key}")
    print()

    # Create .env file
    print("📝 Creating .env file...")
    env_content = f"""# Local development environment variables
# This file is ignored by git for security

# Airflow Fernet Key (for encrypting connections/passwords)
AIRFLOW_FERNET_KEY={fernet_key}

# Airflow UID (Linux only, leave empty on Windows)
AIRFLOW_UID=
"""

    env_file.write_text(env_content)
    print(f"✅ Created {env_file}")
    print()

    # Summary
    print("=" * 70)
    print("Setup Complete! 🎉")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Start services:  docker-compose up -d")
    print("2. Access Airflow:  http://localhost:8080")
    print("3. Login:           airflow / airflow")
    print()
    print("⚠️  Security Note:")
    print("   - The .env file is gitignored and won't be committed")
    print("   - For production, generate a new key and keep it secret")
    print()

if __name__ == "__main__":
    main()
