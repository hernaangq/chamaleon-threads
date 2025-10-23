# 1. Install build tools
sudo apt update
sudo apt install -y build-essential git cmake libomp-dev

# 2. Clone and build BLAKE3 (C version)
git clone https://github.com/BLAKE3-team/BLAKE3.git
cd BLAKE3
git submodule update --init
cd c

# This is the key: use cmake, NOT make directly
mkdir build && cd build
cmake .. -DBUILD_SHARED_LIBS=OFF
cmake --build . --config Release

# Copy header and static lib
sudo cp ../blake3.h /usr/local/include/
sudo cp libblake3.a /usr/local/lib/