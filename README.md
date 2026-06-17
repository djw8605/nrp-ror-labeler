ROR Labeler for the NRP
=======================

The [Research Organization Registry (ROR)](https://ror.org/) is a global, community-led registry of open
persistent identifiers for research organizations. The ROR labeler for the NRP labels nodes given a
table of nodes and their ROR IDs.  It also adds a special attribute for the OSG institutional ID.

## Labeling Nodes

To label a new set of nodes, provide the hostnames to your AI assistant (Claude Code, Codex, OpenCode, etc.). The assistant will:

1. Check `node-institution.csv` — if the node is already there, it reports it as already labeled.
2. Look for existing nodes from the same domain and copy their institution, ROR ID, and OSG IID.
3. If no domain siblings exist, query the [OSG topology API](https://topology-institutions.osg-htc.org/api/institution_ids) and [ROR API](https://api.ror.org/v2/organizations) to look up the institution.
4. Append new rows to `node-institution.csv` and print a summary table of all actions taken.

Any nodes where the OSG IID could not be determined automatically will be flagged with `NEEDS_MANUAL_ENTRY` for you to fill in.

Full instructions for the AI are in [AGENTS.md](AGENTS.md) (Codex/OpenCode) and [CLAUDE.md](CLAUDE.md) (Claude Code).

## Attributes

- `nautilus.io/RORInstitutionID`: The ROR ID of the institution.
- `nautilus.io/OSGInstitutionID`: The OSG ID of the institution (derived from ROR value).
- `nautilus.io/Institution`: The institution name, sanitized for Kubernetes label-value rules.
