#!/bin/bash
# Run ON THE COLLECTOR HOST (the always-on machine running the poller, with
# sudo) to create a restricted user for remote access to this project only
# -- no docker/sudo group.
#
#   sudo bash collector-access-setup.sh
#
# Must run with bash, not `sudo sh ...` -- dash doesn't support pipefail.

if [ -z "${BASH_VERSION:-}" ]; then
    echo "ERROR: this script must be run with bash, not sh/dash." >&2
    echo "Run:  sudo bash $0" >&2
    exit 1
fi

# Set to your collector host's actual hostname -- this guard exists because
# the script creates a user and grants filesystem ACLs, and should refuse
# to run anywhere else.
COLLECTOR_HOSTNAME="${COLLECTOR_HOSTNAME:-prodesk}"

if [ "$(hostname)" != "$COLLECTOR_HOSTNAME" ]; then
    echo "ERROR: this script creates a user and grants filesystem ACLs -- it must" >&2
    echo "run on the collector host ($COLLECTOR_HOSTNAME), not on $(hostname). Refusing to continue." >&2
    exit 1
fi

set -euo pipefail

TTC_CODE_DIR="/home/gopi/ttc_project/ttc-platform"
TTC_DATA_DIR="/mnt/skyhawk/data/ttc-platform"

PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAPw/2YxG2lTiXbNjq1QyTTJ7m2nhtC39WX8w2tA6ucx ttcbot@collector-access"

sudo useradd -m -s /bin/bash ttcbot
sudo passwd -l ttcbot

sudo -u ttcbot mkdir -p /home/ttcbot/.ssh
echo "$PUBKEY" | sudo -u ttcbot tee /home/ttcbot/.ssh/authorized_keys > /dev/null
sudo chmod 700 /home/ttcbot/.ssh
sudo chmod 600 /home/ttcbot/.ssh/authorized_keys

sudo apt-get install -y acl 2>/dev/null || true

sudo setfacl -R -m u:ttcbot:rwx "$TTC_CODE_DIR"
sudo setfacl -R -d -m u:ttcbot:rwx "$TTC_CODE_DIR"

sudo setfacl -R -m u:ttcbot:rx "$TTC_DATA_DIR"
sudo setfacl -R -d -m u:ttcbot:rx "$TTC_DATA_DIR"

grant_traversal() {
    local dir
    dir="$(dirname "$1")"
    while [ "$dir" != "/" ] && [ "$dir" != "." ]; do
        sudo setfacl -m u:ttcbot:x "$dir" 2>/dev/null || true
        dir="$(dirname "$dir")"
    done
}
grant_traversal "$TTC_CODE_DIR"
grant_traversal "$TTC_DATA_DIR"

echo "Done. Test from your compute host with:"
echo "  ssh <your-collector-host-alias> 'whoami && ls $TTC_CODE_DIR && ls $TTC_DATA_DIR'"
echo ""
echo "Confirm ttcbot CANNOT do these (expected to fail):"
echo "  ssh <your-collector-host-alias> 'docker ps'      # should fail: not in docker group"
echo "  ssh <your-collector-host-alias> 'sudo whoami'    # should fail: not in sudoers"
