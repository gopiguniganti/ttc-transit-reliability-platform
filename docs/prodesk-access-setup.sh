#!/bin/bash
# Run ON PRODESK (as gopi, with sudo) to create a restricted user for
# remote access to this project only -- no docker/sudo group.
#
#   sudo bash prodesk-access-setup.sh
#
# Must run with bash, not `sudo sh ...` -- dash doesn't support pipefail.

if [ -z "${BASH_VERSION:-}" ]; then
    echo "ERROR: this script must be run with bash, not sh/dash." >&2
    echo "Run:  sudo bash $0" >&2
    exit 1
fi

if [ "$(hostname)" != "prodesk" ]; then
    echo "ERROR: this script creates a user and grants filesystem ACLs -- it must" >&2
    echo "run ON PRODESK, not on $(hostname). Refusing to continue." >&2
    exit 1
fi

set -euo pipefail

TTC_CODE_DIR="/home/gopi/ttc_project/ttc-platform"
TTC_DATA_DIR="/mnt/skyhawk/data/ttc-platform"

PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAPw/2YxG2lTiXbNjq1QyTTJ7m2nhtC39WX8w2tA6ucx ttcbot@beast-claude-assist"

# 1. Create the user: no password login (key-only), no docker/sudo group.
sudo useradd -m -s /bin/bash ttcbot
sudo passwd -l ttcbot   # locks password auth -- SSH key is the only way in

# 2. Install the public key.
sudo -u ttcbot mkdir -p /home/ttcbot/.ssh
echo "$PUBKEY" | sudo -u ttcbot tee /home/ttcbot/.ssh/authorized_keys > /dev/null
sudo chmod 700 /home/ttcbot/.ssh
sudo chmod 600 /home/ttcbot/.ssh/authorized_keys

# 3. Grant access to EXACTLY these two paths via ACLs (not group membership,
#    not chmod -- so nothing else on the system is affected).
sudo apt-get install -y acl 2>/dev/null || true   # `setfacl` sometimes needs this package

sudo setfacl -R -m u:ttcbot:rwx "$TTC_CODE_DIR"
sudo setfacl -R -d -m u:ttcbot:rwx "$TTC_CODE_DIR"   # default ACL: new files inherit access too

sudo setfacl -R -m u:ttcbot:rx "$TTC_DATA_DIR"       # read + list only, no write -- it's collected data
sudo setfacl -R -d -m u:ttcbot:rx "$TTC_DATA_DIR"

# Also needs execute on each parent directory to traverse down to these paths
# (ACLs don't grant traversal through ancestor directories automatically).
# NOTE: derived from the two paths above, NOT $HOME -- under `sudo`, $HOME
# resolves to root's home (/root), not gopi's, which would silently grant
# the wrong directory.
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

echo "Done. Test from Beast with:"
echo "  ssh prodesk-ttc 'whoami && ls $TTC_CODE_DIR && ls $TTC_DATA_DIR'"
echo ""
echo "Confirm ttcbot CANNOT do these (expected to fail):"
echo "  ssh prodesk-ttc 'docker ps'      # should fail: not in docker group"
echo "  ssh prodesk-ttc 'sudo whoami'    # should fail: not in sudoers"
