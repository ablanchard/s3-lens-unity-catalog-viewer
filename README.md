# S3 Lens - Unity Catalog Viewer

A Chrome extension that resolves Databricks Unity Catalog UUID storage paths on AWS S3 Storage Lens dashboard pages into human-readable `catalog.schema.table` names.

## The problem

S3 Storage Lens shows Unity Catalog storage paths as opaque UUIDs:

```
my-bucket/__unitystorage/schemas/d3eaf7c7-0e77-4d45-970b-5b9e69dbae5b/tables/02001227-e40c-4d05-b79a-9dcab2964056
```

This extension adds inline badges that show the actual names:

```
my-bucket/__unitystorage/schemas/.../tables/...  [my_catalog.my_schema.my_table]
```

Badges are color-coded by type:
- **Blue** — table (`catalog.schema.table`)
- **Amber** — schema (`catalog.schema`)
- **Purple** — catalog (`catalog`)

## How it works

1. A content script scans the S3 Storage Lens page for `__unitystorage` paths
2. UUIDs are extracted and sent to the background service worker
3. The background queries Databricks `system.information_schema.tables` via the SQL Statement API to resolve UUIDs to names
4. Results are cached locally for 24 hours
5. Badges are injected next to the original paths in the page

### UUID resolution strategy

| Path type | SQL approach |
|-----------|-------------|
| `tables/<uuid>` | `WHERE storage_sub_directory IN ('tables/<uuid>', ...)` (batched) |
| `schemas/<uuid>` | `WHERE storage_path LIKE '%/schemas/<uuid>/%' LIMIT 1` |
| `catalogs/<uuid>` | `WHERE storage_path LIKE '%/catalogs/<uuid>/%' LIMIT 1` |

## Installation

1. Clone or download this repository
2. Open `chrome://extensions` in Chrome
3. Enable **Developer mode** (toggle in the top right)
4. Click **Load unpacked** and select the `s3-lens-unity-viewer/` directory

## Configuration

Click the extension icon to open the popup and configure:

- **Workspace URL** — Your Databricks workspace (e.g. `https://my-workspace.cloud.databricks.com`)
- **PAT Token** — A Databricks personal access token with access to `system.information_schema`
- **SQL Warehouse ID** — The ID of a SQL warehouse to execute queries against

Click **Save**, then **Test Connection** to verify.

## Permissions

- `storage` — Persist configuration and UUID cache locally
- `https://*.cloud.databricks.com/*` — Call the Databricks SQL Statement API

The content script runs on `https://eu-west-1.console.aws.amazon.com/*` to match S3 Storage Lens pages.

## Files

```
manifest.json   — Extension manifest (Manifest V3)
background.js   — Service worker: SQL API calls, UUID resolution, caching
content.js      — Content script: DOM scanning, UUID extraction, badge injection
content.css     — Badge styles
popup.html      — Configuration popup
popup.js        — Popup logic
popup.css       — Popup styles
```
