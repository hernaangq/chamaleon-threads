# 1. Update package list
sudo apt update

# 2. Install essential build tools and libraries
sudo apt install -y build-essential cmake libomp-dev

# 3. Install BLAKE3 (from source)
git clone https://github.com/BLAKE3-team/BLAKE3.git
cd BLAKE3
git submodule update --init
cd c
make               # Builds libblake3.a and blake3.h
sudo cp blake3.h /usr/local/include/
sudo cp libblake3.a /usr/local/lib/
cd ../..