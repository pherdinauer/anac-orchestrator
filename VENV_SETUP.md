# Virtual Environment Setup

This project now uses a Python virtual environment to avoid conflicts with system-managed Python packages.

## Quick Setup

### Linux/macOS
```bash
./setup_venv.sh
```

### Windows
```cmd
setup_venv.bat
```

## Manual Setup

If the automated setup doesn't work, you can create the virtual environment manually:

### Linux/macOS
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

### Windows
```cmd
# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate.bat

# Upgrade pip
python -m pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

## Usage

After setup, the startup scripts (`avvio.sh` or `avvio.bat`) will automatically:
1. Check if a virtual environment exists
2. Create one if it doesn't exist
3. Activate the virtual environment
4. Install/update dependencies
5. Run the application

## Deactivating

To deactivate the virtual environment:
- Linux/macOS: `deactivate`
- Windows: `deactivate`

## Troubleshooting

### "externally-managed-environment" Error
This error occurs when trying to install packages globally. The virtual environment setup resolves this by creating an isolated Python environment.

### Permission Issues
If you encounter permission issues:
- Linux/macOS: Make sure the setup script is executable: `chmod +x setup_venv.sh`
- Windows: Run the command prompt as administrator if needed

### Virtual Environment Not Found
If the startup script can't find the virtual environment:
1. Run the setup script manually
2. Ensure you're in the correct project directory
3. Check that the `venv` directory was created successfully
