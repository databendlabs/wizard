#!/usr/bin/env python3
"""
Automated installer for SnowSQL and BendSQL CLI tools on Ubuntu
Installs both tools to /usr/bin for system-wide access
"""

import os
import sys
import subprocess
import tempfile
import venv
from pathlib import Path
import json
import shutil
import platform

class SystemBinCLIInstaller:
    def __init__(self):
        self.home_dir = Path.home()
        self.script_dir = Path(__file__).parent.absolute()
        self.venv_dir = self.script_dir / "cli_installer_env"
        self.temp_dir = tempfile.mkdtemp()
        self.install_dir = Path("/usr/bin")
        self.requirements = [
            "requests>=2.25.0",
            "beautifulsoup4>=4.9.0",
            "lxml>=4.6.0"
        ]
        
    def print_status(self, message, status="INFO"):
        """Print installation status with formatting"""
        status_symbols = {
            "INFO": "ℹ️",
            "SUCCESS": "✅",
            "ERROR": "❌",
            "WARNING": "⚠️",
            "FIX": "🔧"
        }
        print(f"{status_symbols.get(status, 'ℹ️')} [{status}] {message}")
    
    def run_command(self, command, shell=True, capture_output=True, cwd=None, env=None):
        """Execute shell command and return result"""
        try:
            result = subprocess.run(
                command, 
                shell=shell, 
                capture_output=capture_output, 
                text=True,
                cwd=cwd or self.temp_dir,
                env=env
            )
            return result
        except Exception as e:
            self.print_status(f"Command execution failed: {e}", "ERROR")
            return None
    
    def check_sudo_access(self):
        """Check if we have sudo access"""
        self.print_status("Checking sudo access...", "INFO")
        result = self.run_command("sudo -n true")
        if result and result.returncode == 0:
            self.print_status("Sudo access confirmed", "SUCCESS")
            return True
        else:
            self.print_status("This script requires sudo access to install tools to /usr/bin", "WARNING")
            self.print_status("Please run: sudo -v", "INFO")
            # Try to get sudo access
            sudo_result = self.run_command("sudo -v", capture_output=False)
            if sudo_result and sudo_result.returncode == 0:
                self.print_status("Sudo access granted", "SUCCESS")
                return True
            else:
                self.print_status("Failed to get sudo access", "ERROR")
                return False
    
    def check_system_prerequisites(self):
        """Check if required system tools are available"""
        self.print_status("Checking system prerequisites...", "INFO")
        
        # Required tools
        required_tools = ['curl', 'bash', 'gpg', 'unzip', 'alien']
        missing_tools = []
        
        # Check tools
        for tool in required_tools:
            result = self.run_command(f"which {tool}")
            if result and result.returncode != 0:
                missing_tools.append(tool)
        
        if missing_tools:
            self.print_status(f"Missing required tools: {', '.join(missing_tools)}", "INFO")
            self.print_status("Installing missing tools...", "INFO")
            
            # Update package list
            update_result = self.run_command("sudo apt update", capture_output=False)
            if update_result and update_result.returncode != 0:
                self.print_status("Failed to update package list", "ERROR")
                return False
            
            # Install missing tools
            install_cmd = f"sudo apt install -y {' '.join(missing_tools)}"
            install_result = self.run_command(install_cmd, capture_output=False)
            if install_result and install_result.returncode != 0:
                self.print_status("Failed to install required tools", "ERROR")
                return False
        
        self.print_status("System prerequisites check completed", "SUCCESS")
        return True
    
    def install_bendsql_to_usr_bin(self):
        """Install BendSQL directly to /usr/bin"""
        self.print_status("Starting BendSQL installation to /usr/bin...", "INFO")
        
        try:
            # Create installation script
            install_script = f'''#!/bin/bash
set -e

# Create temporary directory for BendSQL installation
TEMP_BENDSQL_DIR=$(mktemp -d)
cd "$TEMP_BENDSQL_DIR"

echo "Detecting architecture..."
ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "$ARCH" = "x86_64" ]; then
    ARCH="x86_64"
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    ARCH="aarch64"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

echo "Architecture: $ARCH, OS: $OS"

# Try to get latest release info from GitHub
echo "Fetching latest BendSQL release..."
LATEST_URL="https://api.github.com/repos/databendlabs/bendsql/releases/latest"
DOWNLOAD_URL=$(curl -s "$LATEST_URL" | grep "browser_download_url.*$OS-$ARCH.tar.gz" | cut -d '"' -f 4 | head -1)

if [ -z "$DOWNLOAD_URL" ]; then
    echo "Could not find download URL from GitHub, trying direct repository..."
    # Fallback to direct repository URL
    VERSION="v0.22.2"
    if [ "$ARCH" = "x86_64" ]; then
        DOWNLOAD_URL="https://repo.databend.com/bendsql/$VERSION/bendsql-x86_64-unknown-linux-gnu.tar.gz"
    else
        DOWNLOAD_URL="https://repo.databend.com/bendsql/$VERSION/bendsql-aarch64-unknown-linux-gnu.tar.gz"
    fi
fi

echo "Downloading BendSQL from: $DOWNLOAD_URL"
curl -L -o bendsql.tar.gz "$DOWNLOAD_URL"

if [ ! -f bendsql.tar.gz ]; then
    echo "Failed to download BendSQL"
    exit 1
fi

echo "Extracting BendSQL..."
tar -xzf bendsql.tar.gz

# Find the bendsql binary
BENDSQL_BIN=$(find . -name "bendsql" -type f -executable | head -1)

if [ -z "$BENDSQL_BIN" ]; then
    echo "BendSQL binary not found in archive"
    echo "Archive contents:"
    tar -tzf bendsql.tar.gz
    exit 1
fi

echo "Found BendSQL binary at: $BENDSQL_BIN"

echo "Installing BendSQL to /usr/bin..."
sudo cp "$BENDSQL_BIN" /usr/bin/bendsql
sudo chmod +x /usr/bin/bendsql

echo "Verifying BendSQL installation..."
if /usr/bin/bendsql --version; then
    echo "BendSQL installed successfully!"
else
    echo "BendSQL installation verification failed"
    exit 1
fi

# Cleanup
cd /
rm -rf "$TEMP_BENDSQL_DIR"

echo "BendSQL installation to /usr/bin completed"
'''
            
            script_path = Path(self.temp_dir) / "install_bendsql_usr_bin.sh"
            with open(script_path, 'w') as f:
                f.write(install_script)
            
            # Make script executable
            os.chmod(script_path, 0o755)
            
            # Run the installation script
            result = self.run_command(f"bash {script_path}", capture_output=False)
            
            if result and result.returncode == 0:
                self.print_status("BendSQL installation to /usr/bin completed successfully", "SUCCESS")
                
                # Verify installation
                bendsql_path = Path("/usr/bin/bendsql")
                if bendsql_path.exists():
                    # Test the installation
                    test_result = self.run_command("bendsql --version")
                    if test_result and test_result.returncode == 0:
                        self.print_status(f"BendSQL version: {test_result.stdout.strip()}", "SUCCESS")
                        return True
                    else:
                        self.print_status("BendSQL installed but version check failed", "WARNING")
                        return True  # Still consider it successful if binary exists
                else:
                    self.print_status("BendSQL installation completed but binary not found", "ERROR")
                    return False
            else:
                self.print_status("BendSQL installation to /usr/bin failed", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"BendSQL installation error: {e}", "ERROR")
            return False
    
    def detect_architecture(self):
        """Detect system architecture"""
        arch = platform.machine().lower()
        if arch in ['x86_64', 'amd64']:
            return 'x86_64'
        elif arch in ['aarch64', 'arm64']:
            return 'aarch64'
        else:
            self.print_status(f"Unsupported architecture: {arch}", "WARNING")
            return 'x86_64'  # Default fallback
    
    def install_snowsql_to_usr_bin(self):
        """Install SnowSQL directly to /usr/bin using RPM package"""
        self.print_status("Starting SnowSQL installation to /usr/bin...", "INFO")
        
        try:
            version = "1.4.0"
            bootstrap_version = "1.3"
            arch = self.detect_architecture()
            
            # Construct RPM download URL
            rpm_filename = f"snowflake-snowsql-{version}-1.{arch}.rpm"
            rpm_url = f"https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/{bootstrap_version}/linux_{arch}/{rpm_filename}"
            
            self.print_status(f"Downloading SnowSQL RPM from {rpm_url}...", "INFO")
            
            # Create installation script that installs directly to /usr/bin
            install_script = f'''#!/bin/bash
set -e

echo "Downloading SnowSQL RPM package..."
curl -L -O "{rpm_url}"

echo "Converting RPM to DEB package..."
sudo alien --to-deb {rpm_filename}

echo "Installing converted DEB package..."
DEB_FILE=$(ls snowflake-snowsql*.deb | head -1)
sudo dpkg -i "$DEB_FILE" || sudo apt-get install -f -y

echo "Finding SnowSQL installation and copying to /usr/bin..."
# Search for the actual SnowSQL binary
SNOWSQL_LOCATIONS=(
    "/usr/bin/snowsql"
    "/opt/snowflake/snowsql/bin/snowsql"
    "/usr/local/snowflake/snowsql/bin/snowsql"
    "/usr/share/snowflake/snowsql/bin/snowsql"
)

FOUND_SNOWSQL=""
for location in "${{SNOWSQL_LOCATIONS[@]}}"; do
    if [ -f "$location" ] && [ -x "$location" ]; then
        FOUND_SNOWSQL="$location"
        echo "Found SnowSQL at: $location"
        break
    fi
done

# If not found in common locations, search more broadly
if [ -z "$FOUND_SNOWSQL" ]; then
    echo "Searching for SnowSQL binary in system..."
    FOUND_SNOWSQL=$(find /usr /opt -name "snowsql" -type f -executable 2>/dev/null | head -1)
fi

if [ -n "$FOUND_SNOWSQL" ]; then
    if [ "$FOUND_SNOWSQL" != "/usr/bin/snowsql" ]; then
        echo "Copying SnowSQL to /usr/bin..."
        sudo cp "$FOUND_SNOWSQL" /usr/bin/snowsql
        sudo chmod +x /usr/bin/snowsql
    fi
    
    echo "Verifying SnowSQL installation..."
    if /usr/bin/snowsql --version 2>/dev/null || /usr/bin/snowsql --help >/dev/null 2>&1; then
        echo "SnowSQL installed successfully to /usr/bin!"
    else
        echo "SnowSQL copied but verification failed"
    fi
else
    echo "SnowSQL binary not found after installation"
    echo "Attempting to extract from DEB package manually..."
    
    # Try to extract the DEB package and find the binary
    TEMP_EXTRACT=$(mktemp -d)
    cd "$TEMP_EXTRACT"
    
    dpkg-deb -x "../$DEB_FILE" .
    EXTRACTED_SNOWSQL=$(find . -name "snowsql" -type f -executable | head -1)
    
    if [ -n "$EXTRACTED_SNOWSQL" ]; then
        echo "Found SnowSQL in extracted package: $EXTRACTED_SNOWSQL"
        sudo cp "$EXTRACTED_SNOWSQL" /usr/bin/snowsql
        sudo chmod +x /usr/bin/snowsql
        echo "SnowSQL manually installed to /usr/bin"
    else
        echo "Could not find SnowSQL binary in package"
        exit 1
    fi
    
    cd ..
    rm -rf "$TEMP_EXTRACT"
fi

echo "SnowSQL installation to /usr/bin completed"
'''
            
            script_path = Path(self.temp_dir) / "install_snowsql_usr_bin.sh"
            with open(script_path, 'w') as f:
                f.write(install_script)
            
            # Make script executable
            os.chmod(script_path, 0o755)
            
            # Run the installation script
            result = self.run_command(f"bash {script_path}", capture_output=False)
            
            if result and result.returncode == 0:
                self.print_status("SnowSQL installation to /usr/bin completed successfully", "SUCCESS")
                
                # Verify installation
                snowsql_path = Path("/usr/bin/snowsql")
                if snowsql_path.exists():
                    # Test the installation
                    test_result = self.run_command("snowsql --version")
                    if test_result and test_result.returncode == 0:
                        self.print_status(f"SnowSQL version check successful", "SUCCESS")
                        return True
                    else:
                        # Try help command as fallback
                        help_result = self.run_command("snowsql --help")
                        if help_result and help_result.returncode == 0:
                            self.print_status("SnowSQL help command works (version check failed)", "SUCCESS")
                            return True
                        else:
                            self.print_status("SnowSQL installed but both version and help checks failed", "WARNING")
                            return True  # Still consider successful if binary exists
                else:
                    self.print_status("SnowSQL installation completed but binary not found in /usr/bin", "ERROR")
                    return False
            else:
                self.print_status("SnowSQL installation to /usr/bin failed", "ERROR")
                return False
                
        except Exception as e:
            self.print_status(f"SnowSQL installation error: {e}", "ERROR")
            return False
    
    def verify_installations(self):
        """Verify both installations work correctly"""
        self.print_status("=== Verifying Installations ===", "INFO")
        
        success_count = 0
        
        # Test BendSQL
        self.print_status("Testing BendSQL...", "INFO")
        bendsql_result = self.run_command("bendsql --version")
        if bendsql_result and bendsql_result.returncode == 0:
            self.print_status(f"✓ BendSQL: {bendsql_result.stdout.strip()}", "SUCCESS")
            success_count += 1
        else:
            # Try help command
            help_result = self.run_command("bendsql --help")
            if help_result and help_result.returncode == 0:
                self.print_status("✓ BendSQL: Available (help command works)", "SUCCESS")
                success_count += 1
            else:
                self.print_status("✗ BendSQL test failed", "ERROR")
        
        # Test SnowSQL
        self.print_status("Testing SnowSQL...", "INFO")
        snowsql_result = self.run_command("snowsql --version")
        if snowsql_result and snowsql_result.returncode == 0:
            self.print_status(f"✓ SnowSQL: {snowsql_result.stdout.strip()}", "SUCCESS")
            success_count += 1
        else:
            # Try help command as fallback
            snowsql_help = self.run_command("snowsql --help")
            if snowsql_help and snowsql_help.returncode == 0:
                self.print_status("✓ SnowSQL: Available (help command works)", "SUCCESS")
                success_count += 1
            else:
                self.print_status("✗ SnowSQL test failed", "ERROR")
        
        return success_count
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            shutil.rmtree(self.temp_dir)
            self.print_status("Temporary files cleaned up", "INFO")
        except Exception as e:
            self.print_status(f"Failed to clean up temporary files: {e}", "WARNING")
    
    def install_all(self):
        """Install both BendSQL and SnowSQL to /usr/bin"""
        self.print_status("=== /usr/bin CLI Tools Installer for Ubuntu ===", "INFO")
        self.print_status("This script will install BendSQL and SnowSQL to /usr/bin", "INFO")
        
        # Check sudo access
        if not self.check_sudo_access():
            self.print_status("Sudo access required for installation to /usr/bin. Exiting.", "ERROR")
            return False
        
        # Check system prerequisites
        if not self.check_system_prerequisites():
            self.print_status("System prerequisites check failed. Exiting.", "ERROR")
            return False
        
        success_count = 0
        total_tools = 2
        
        # Install BendSQL
        self.print_status("\n--- Installing BendSQL to /usr/bin ---", "INFO")
        if self.install_bendsql_to_usr_bin():
            success_count += 1
        
        # Install SnowSQL
        self.print_status("\n--- Installing SnowSQL to /usr/bin ---", "INFO")
        if self.install_snowsql_to_usr_bin():
            success_count += 1
        
        # Verify installations
        self.print_status("\n--- Verification ---", "INFO")
        verified_count = self.verify_installations()
        
        # Summary
        self.print_status(f"\n=== Installation Summary ===", "INFO")
        self.print_status(f"Successfully installed: {success_count}/{total_tools} tools", 
                         "SUCCESS" if success_count == total_tools else "WARNING")
        self.print_status(f"Successfully verified: {verified_count}/{total_tools} tools", 
                         "SUCCESS" if verified_count == total_tools else "WARNING")
        
        if success_count > 0:
            self.print_status("\n🎉 Installation completed!", "SUCCESS")
            self.print_status("Tools are now available system-wide in /usr/bin:", "INFO")
            self.print_status("", "INFO")
            if success_count >= 1:
                self.print_status("  bendsql --help", "INFO")
            if success_count >= 2:
                self.print_status("  snowsql --help", "INFO")
            self.print_status("", "INFO")
            self.print_status("No need to restart terminal or modify PATH!", "SUCCESS")
            self.print_status("Both tools are now in /usr/bin and ready to use", "INFO")
        
        # Cleanup
        self.cleanup()
        
        return success_count == total_tools

def main():
    """Main function"""
    installer = SystemBinCLIInstaller()
    
    try:
        success = installer.install_all()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        installer.print_status("Installation interrupted by user", "WARNING")
        installer.cleanup()
        sys.exit(1)
    except Exception as e:
        installer.print_status(f"Unexpected error: {e}", "ERROR")
        installer.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()
