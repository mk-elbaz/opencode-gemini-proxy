# Security Policy

The web dashboard binds to `localhost`/loopback by default and is not
exposed to the network unless you explicitly change that.

API keys live only in your local `.env` file (or are added via the
dashboard, which rewrites `.env`) — they are never committed, and never
logged unmasked; logs and the dashboard only show masked/truncated keys.

If you find a security issue, please do not open a public GitHub issue.
Instead use GitHub's private vulnerability reporting (the "Security" tab of
this repository, then "Report a vulnerability") with details and, if possible,
steps to reproduce. We'll acknowledge and follow up as soon as we can.
