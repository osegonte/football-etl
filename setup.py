"""
Setup script for the football data ETL pipeline.
Creates necessary directories and installs dependencies.
"""

import os
import subprocess
import sys
from pathlib import Path


def create_directories():
    """Create necessary directories for the pipeline."""
    dirs = [
        "data",
        "data/raw",
        "data/processed",
        "data/output",
        "logs",
        "scrapers",
        "processors",
        "utils"
    ]
    
    print("Creating directory structure...")
    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ Created {directory}/")


def install_dependencies():
    """Install required dependencies."""
    print("\nInstalling dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("  ✓ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Error installing dependencies: {e}")
        return False
    
    return True


def create_init_files():
    """Create __init__.py files for Python packages."""
    package_dirs = [
        "scrapers",
        "processors",
        "utils"
    ]
    
    print("\nCreating package __init__.py files...")
    for directory in package_dirs:
        init_file = os.path.join(directory, "__init__.py")
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                module_name = os.path.basename(directory)
                f.write(f'"""Package {module_name}."""\n')
            print(f"  ✓ Created {init_file}")


def check_chrome_driver():
    """Check if ChromeDriver is installed."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        print("\nChecking ChromeDriver installation...")
        driver_path = ChromeDriverManager().install()
        print(f"  ✓ ChromeDriver available at: {driver_path}")
        
        # Create a test driver to verify installation
        print("  Testing ChromeDriver...")
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver.quit()
        print("  ✓ ChromeDriver working correctly")
        
    except Exception as e:
        print(f"  ✗ ChromeDriver check failed: {e}")
        print("  ⚠ You may need to install Chrome browser manually")
        return False
    
    return True


def main():
    """Main setup function."""
    print("=" * 60)
    print("Football Data ETL Pipeline Setup")
    print("=" * 60)
    
    create_directories()
    deps_ok = install_dependencies()
    
    if deps_ok:
        create_init_files()
        driver_ok = check_chrome_driver()
        
        if driver_ok:
            print("\n✅ Setup completed successfully!")
            print("\nYou can now run the pipeline with:")
            print("  python pipeline.py")
        else:
            print("\n⚠ Setup completed with warnings.")
            print("Please check the ChromeDriver issues before running the pipeline.")
    else:
        print("\n❌ Setup failed to install dependencies.")
        print("Please check the errors and try again.")
    
    print("=" * 60)


if __name__ == "__main__":
    main()