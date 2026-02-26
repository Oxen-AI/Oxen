
# Install the raw deps
sudo apt update
sudo apt install -y fish
sudo apt install -y apt-utils

# Install docker
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo chmod 666 /var/run/docker.sock

# SSH Keys
ssh-keygen -t ed25519 -C "g@oxen.ai"
ssh-add -K ~/.ssh/id_ed25519
# Copy into deploy keys on github
cat ~/.ssh/id_ed25519.pub

# Clone the repo
mkdir Code
cd Code/
git clone git@github.com:Oxen-AI/Oxen.git
cd Oxen/

# Build docker
docker build -t oxen/server:0.1.0 .

# Run in docker
docker run -d -v /var/oxen/data:/var/oxen/data -p 3000:3001 --name oxen oxen/server:0.1.0

# Or docker compose
docker compose up -d reverse-proxy
docker compose up -d --scale oxen=4 --no-recreate

# Build actual binary

sudo apt install -y libclang-dev
sudo apt install -y build-essential
sudo apt install -y libssl-dev
sudo apt install -y pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo install cargo-build-deps

# Build the server
cargo build --workspace --release
ln -s /path/to/release/build/oxen /usr/local/bin/oxen

# Run the server with a user
mkdir -p /home/ubuntu/Data/sync/
sudo env SYNC_DIR=/home/ubuntu/Data/sync/ ./target/release/oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
sudo env SYNC_DIR=/home/ubuntu/Data/sync/ ./target/release/oxen-server start -p 80
