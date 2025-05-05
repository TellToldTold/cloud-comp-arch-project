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
    
    # Install pip if not installed
    if ! command -v pip3 &> /dev/null; then
        sudo apt-get install -y python3-pip
    fi
    
    # Install required Python packages
    pip3 install --user psutil docker netaddr
    
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
        sudo apt-get install -y \
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
        
        # Install Docker Engine
        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io
        
        # Add current user to docker group to run docker without sudo
        sudo usermod -aG docker $USER
        
        echo "Docker installed successfully. You may need to log out and back in for group changes to take effect."
    fi
}

# Install netcat (for memcached stats)
install_netcat() {
    print_section "Installing netcat"
    sudo apt-get install -y netcat
    echo "Netcat installed successfully."
}

# Make scripts executable
make_executable() {
    print_section "Making scripts executable"
    chmod +x scheduler_controller.py
    chmod +x test_scheduler.py
    echo "Scripts are now executable."
}

# Print usage information
print_usage() {
    print_section "Setup Complete! Here's how to use the scheduler:"
    
    echo "To run tests:"
    echo "  ./test_scheduler.py --memcached-ip <MEMCACHED_IP>"
    echo
    echo "To run the scheduler controller:"
    echo "  ./scheduler_controller.py --memcached-ip <MEMCACHED_IP>"
    echo
    echo "Replace <MEMCACHED_IP> with the internal IP address of your memcached server."
    echo
    echo "Note: If you're running this for the first time after installing Docker,"
    echo "you may need to log out and back in for the docker group changes to take effect."
    echo "Alternatively, you can run the following to use Docker without logging out:"
    echo "  newgrp docker"
}

# Main function
main() {
    print_section "Setting up scheduler environment"
    
    install_python_deps
    install_docker
    install_netcat
    make_executable
    
    print_usage
}

# Run the main function
main 