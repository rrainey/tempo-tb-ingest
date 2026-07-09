# Deployment (Linux workstation)

One-time setup on the always-on dropzone workstation:

```bash
# 1. environment (from the repo checkout)
cd /home/riley/src/tempo-tb-ingest
uv sync                          # runtime deps
(cd dashboard && npm ci && npm run build)   # dashboard → dashboard/dist

# 2. config
sudo cp deploy/tempo-tb-ingest.example.toml /etc/tempo-tb-ingest.toml
sudo $EDITOR /etc/tempo-tb-ingest.toml      # paths, thresholds, static_dir

# 3. data dir (owned by the service user)
sudo mkdir -p /var/lib/tempo-tb-ingest
sudo chown riley:riley /var/lib/tempo-tb-ingest

# 4. service
sudo cp deploy/tempo-tb-ingest.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/tempo-tb-ingest.service
sudo systemctl daemon-reload
sudo systemctl enable --now tempo-tb-ingest
```

Operations:

- Health: `curl -s localhost:8080/healthz` · dashboard: `http://<host>:8080/`
- Logs: `journalctl -u tempo-tb-ingest -f`
- The daemon is watchdog-supervised (`Type=notify`, `WatchdogSec=60`; the
  daemon feeds every 5 s) and restarts on failure. Exit code 3 =
  another instance holds the data_dir lock.
- Event recordings accumulate under `<data_dir>/events/YYYYMMDD.jsonl` —
  useful for replay (`tempo-tb-ingest replay <file> --listen …
  --static dashboard/dist`) and diagnosis.
- `device-owners.json` lives in the staging root (edit at the start of a
  jump day); `tempo-tb-ingest promote` is run manually after harvesting.

Porting notes for other hosts: adjust `User=`, `ExecStart=` (venv path), and
`Documentation=` in the unit; everything else is config. For the Windows
laptop scenario see `docs/windows-options.md`.
