# Gmail Inbox Cleanup

[![Personal Gmail tool – not Google verified](https://img.shields.io/badge/Personal%20Gmail%20tool-Not%20Google%20verified-orange)](#)

CLI utility to safely reduce your Gmail inbox size while keeping purchase/receipt emails. It:
- Archives INBOX mail between **7 years** and **3 days** old (removes the INBOX label)
- Trashes (or permanently deletes) INBOX mail older than **7 years**
- Preserves receipts and purchases via configurable keep rules
- Uses checkpointing, batching, and logging to survive interruptions and respect Gmail limits

## Safety first
- Always run a dry run before deleting: `python gmail_cleanup.py --dry-run`
- Default behavior sends old mail to Trash; add `--permanent-delete` only when you are sure
- Checkpointing tracks processed message IDs so reruns skip already handled items; reset via `--reset-*` flags if needed
- Gmail imposes rate limits; start with small batches (e.g., `--batch-size 10 --base-sleep 1.0`) and increase slowly

## Prerequisites
- Python 3.8+
- A Google Cloud project with the Gmail API enabled
- OAuth consent screen set to **Testing**
- Your Google account added as a **Test User**
- A **Desktop** OAuth client; download the resulting `credentials.json`

> Keep OAuth secrets out of git. Place `credentials.json` next to the script locally, but it is ignored by `.gitignore`. Never commit it. A stub `credentials.example.json` is provided as a placeholder only.

## Installation
1) (Optional) create a virtual environment
   - Windows: `python -m venv .venv; .\.venv\Scripts\activate`
   - macOS/Linux: `python -m venv .venv && source .venv/bin/activate`
2) Install dependencies: `pip install -r requirements.txt`

## Usage
Run commands from this folder (after activating your venv if you use one).

- Dry run (no changes):
  ```bash
  python gmail_cleanup.py --dry-run
  ```

- Real run with defaults (trash >7y, archive 7y-3d):
  ```bash
  python gmail_cleanup.py
  ```

- Reduce batch size / slow down to avoid rate limits:
  ```bash
  python gmail_cleanup.py --batch-size 10 --base-sleep 1.0
  ```

- Log to a file (stdout still receives logs):
  ```bash
  python gmail_cleanup.py --log-file cleanup.log --log-level INFO
  ```

- Add keep rules (banks, Amazon, starred/important) while keeping defaults:
  ```bash
  python gmail_cleanup.py --keep-query "from:(amazon.com)" --keep-query "label:important"
  ```

- Disable default keep filter and provide your own:
  ```bash
  python gmail_cleanup.py --no-default-keep --keep-query "from:(mybank.com OR paypal.com)"
  ```

- Reset checkpoints if you want to reprocess everything:
  ```bash
  python gmail_cleanup.py --reset-all
  ```

- Permanently delete instead of trashing (>7y bucket only):
  ```bash
  python gmail_cleanup.py --permanent-delete
  ```

## Runtime files
- `token.json` (OAuth token; created after first auth flow)
- `gmail_cleanup_checkpoint.sqlite3` (+ WAL/SHM sidecars) for processed message tracking
- Log files if `--log-file` is supplied

## Troubleshooting
- **credentials.json not found**: ensure it is downloaded from your Desktop OAuth client and placed beside the script.
- **Access blocked / app not verified**: confirm OAuth consent screen is **Testing** and your account is listed under Test Users.
- **Too many concurrent requests / rate limit errors**: lower `--batch-size`, increase `--base-sleep`, and retry; checkpointing prevents duplicate processing.

## Design notes
- **Batching vs sequential**: requests are grouped to reduce HTTP overhead but kept small to stay under Gmail rate limits.
- **Exponential backoff**: retries on rate limit errors add jitter and optionally shrink batch sizes when Gmail pushes back.
- **Checkpointing**: processed message IDs are stored in SQLite so you can safely rerun after interruptions or tuning changes.

## Secrets and safety
- Do **not** commit `credentials.json` or `token.json`; they are ignored via `.gitignore`.
- Store these files locally alongside the script only for personal use.
- Review logs and dry-run output before enabling permanent deletes. You are responsible for data loss from deletes.

## License
MIT License. See `LICENSE` for details.
