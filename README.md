# DeltaSync

Syncs a folder from one machine to multiple clients over TCP. Only transfers changed chunks.

## Setup

pip install -r requirements.txt

## Run

**Host** (machine with the folder to sync):
python host.py 8000 /path/to/folder

**Client** (machine to sync to):
python client.py <host-ip> 8000

Synced files appear in `./received/` by default. To change:
python client.py <host-ip> 8000 /path/to/output

## Stop

`Ctrl+C` on either side."# csse432-final-project" 
