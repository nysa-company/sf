# Git flow

- Use short-lived branches from the protected default branch. Never push directly to it.
- Use the `branch_pattern` declared in `.agents/repo-standard.json`.
- Keep commits small and explain why the change exists.
- Run the declared verification command, `scripts/repo-check`, and `scripts/secret-scan` before review.
- Open a draft PR while work or evidence is incomplete; mark it ready only when deterministic gates pass.
- Prefer squash merge, then delete the merged branch.
- Configure branch protection to require review and the deterministic baseline CI job.
