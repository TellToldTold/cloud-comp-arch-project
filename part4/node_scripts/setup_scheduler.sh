#!/bin/bash

# Exit on error
set -e

# Function to echo messages in a visually distinct way
print_section() {
    echo "===================================================================="
    echo "$1"
    echo "===================================================================="
}

# Install Python dependencies
install_python_deps() {
    print_section "Installing Python dependencies"
    
    # Update package lists
    sudo apt-get update
    
    # Install Python packages via apt instead of pip
    # This avoids issues with externally-managed-environment errors in newer Ubuntu versions
    sudo apt-get install -y \
        python3-psutil \
        python3-docker \
        python3-netaddr \
        screen
    
    echo "Python dependencies installed successfully."
}

# Install Docker if not installed
install_docker() {
    print_section "Checking for Docker"
    
    if command -v docker &> /dev/null; then
        echo "Docker is already installed."
    else
        print_section "Installing Docker"
        
        # Update package lists
        sudo apt-get update
        
        # Install dependencies
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
            apt-transport-https \
            ca-certificates \
            curl \
            gnupg \
            lsb-release
        
        # Add Docker's official GPG key
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
        
        # Set up the stable repository
        echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
            $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        
        # Set dpkg to keep existing config files
        echo 'Dpkg::Options {"--force-confold"};' | sudo tee /etc/apt/apt.conf.d/local > /dev/null
        
        # Install Docker Engine in noninteractive mode to prevent prompts
        sudo DEBIAN_FRONTEND=noninteractive apt-get update
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce docker-ce-cli containerd.io
        
        # Add current user to docker group to run docker without sudo
        sudo usermod -aG docker $USER
        
        echo "Docker installed successfully. You may need to log out and back in for group changes to take effect."
    fi
}

# Install netcat (for memcached stats)
install_netcat() {
    print_section "Installing netcat"
    # Install netcat-openbsd instead of the virtual netcat package
    sudo apt-get install -y netcat-openbsd
    echo "Netcat installed successfully."
}


# Main function
main() {
    print_section "Setting up scheduler environment"
    
    install_python_deps
    install_docker
    install_netcat
    
}

# Run the main function
main 