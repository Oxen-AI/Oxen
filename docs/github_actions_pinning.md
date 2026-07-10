# GitHub Actions Pinning Policy

Canonical policy for pinning GitHub Actions across all Oxen-AI repositories. When adding or updating an action in any repo's `.github/workflows/`, pin it according to its trust tier.

## Trust tiers

- **First-party and high-trust vendor orgs** pin to a **version tag** (`@v6`, `@v2.1.5`) — these publishers are trusted to honor their tags. The base trusted set is:
  - First-party: `actions/*` (GitHub's own) and `Oxen-AI` (this org's own).
  - High-trust vendors: `aws-actions`, `docker`, `slackapi`, `hashicorp`.
- **All other (third-party) actions** pin to a **full-length commit SHA**, with the human-readable version in a trailing comment:

  ```yaml
  uses: machulav/ec2-github-runner@343a1b2ae682e681c3cec9a235d882da17ff04ef # v2.6.1
  ```

  A moving tag from an unvetted publisher is a supply-chain risk; the SHA is immutable, and the trailing comment keeps it legible.

## Adding an action from a new org

Default it to the SHA tier. Promote an org to the version-tag tier only by a deliberate team decision, then add it to the base list above so the promotion applies uniformly across all repositories.
