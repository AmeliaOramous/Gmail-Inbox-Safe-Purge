# Changelog

## v1.0 - 2026-01-02
- Initial release of Gmail Inbox Cleanup CLI.
- Archives INBOX mail between 7 years and 3 days old while keeping receipts/purchases.
- Trashes (or permanently deletes) INBOX mail older than 7 years with optional dry-run.
- Includes checkpointing (SQLite), logging, rate-limit-friendly batching, and configurable keep rules.
