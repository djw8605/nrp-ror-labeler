# Agent Instructions — NRP Node Labeler

## Label Nodes Workflow

When the user provides a list of nodes to label, check and update `node-institution.csv` by following the steps below.

### CSV Format

`node-institution.csv` columns:
```
OSG Identifier,Institution Name,ROR Value,OSG Value
```

Example row:
```
epic001.clemson.edu,Clemson University,https://ror.org/037s24f05,https://osg-htc.org/iid/ricyf18amt49
```

### Process (execute in order for each node)

**Step 1 — Check if already in CSV**

Read `node-institution.csv` and check whether the node hostname appears in the `OSG Identifier` column (exact match on the full hostname).

- **Already present**: record "already labeled" and skip to the next node.

**Step 2 — Check for domain siblings**

Extract the domain from the node (everything after the first `.`). Scan `node-institution.csv` for any existing row whose hostname shares that same domain suffix.

- **Domain match found**: copy `Institution Name`, `ROR Value`, and `OSG Value` from the matching row. Append the new row to the CSV. Record "copied from domain sibling `<sibling-hostname>`".

**Step 3 — Look up institution (no domain siblings)**

When no sibling exists, identify the institution for the node:

- **3a — Query OSG topology first**: Fetch `https://topology-institutions.osg-htc.org/api/institution_ids` (JSON list). Search for the institution by name. If found, extract both the ROR ID and OSG IID from this response.
- **3b — Query ROR if needed**: If not in the OSG topology response, fetch `https://api.ror.org/v2/organizations?query=<institution-name>`. Use the `id` field from the best match as the ROR Value (already a full URL like `https://ror.org/XXXXXXX`).
- **3c — OSG IID**: If the OSG IID was not found in the topology API response, set `OSG Value` to `NEEDS_MANUAL_ENTRY`. Never invent an OSG IID.

Append the new row to the CSV. Record "queried ROR/OSG for institution".

### Appending rows

Preserve all existing content. New rows go at the end of the file:
```
<hostname>,<Institution Name>,<ROR Value>,<OSG Value>
```
Wrap `Institution Name` in double-quotes if it contains a comma.

### Summary output

After processing all nodes, print a Markdown summary table:

| Node | Action | Institution | ROR Value | OSG Value |
|------|--------|-------------|-----------|-----------|
| ... | Already labeled / Copied from `<sibling>` / Queried ROR+OSG | ... | ... | ... |

List any `NEEDS_MANUAL_ENTRY` nodes below the table with a prompt for the user to fill in the OSG IID.

### Rules

- Never modify existing rows.
- If a fetch fails, report the error and leave the node unlabeled rather than guessing.
- Prefer the OSG topology API over the ROR API when it has the needed data.
