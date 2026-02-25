// ============================================================
// background.js — Service worker for S3 Lens Unity Viewer
// Handles Databricks SQL Statement API calls, UUID resolution,
// and local caching.
// ============================================================

console.log("[S3 Lens BG] Service worker started");

const DEFAULTS = {
  workspaceUrl: "",
  warehouseId: "",
  patToken: "",
};

const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

// --------------- Storage Helpers ---------------

async function getStorage(keys) {
  return chrome.storage.local.get(keys);
}

async function setStorage(data) {
  return chrome.storage.local.set(data);
}

async function getConfig() {
  const stored = await getStorage([
    "workspaceUrl",
    "warehouseId",
    "patToken",
    "uuidCache",
    "cacheUpdatedAt",
  ]);
  return {
    workspaceUrl: stored.workspaceUrl || DEFAULTS.workspaceUrl,
    warehouseId: stored.warehouseId || DEFAULTS.warehouseId,
    patToken: stored.patToken || DEFAULTS.patToken,
    uuidCache: stored.uuidCache || {},
    cacheUpdatedAt: stored.cacheUpdatedAt || null,
  };
}

// --------------- Databricks SQL Statement API ---------------

async function executeSql(workspaceUrl, patToken, warehouseId, sql) {
  const url = `${workspaceUrl.replace(/\/+$/, "")}/api/2.0/sql/statements`;

  console.log("[S3 Lens BG] Executing SQL:", sql.trim().substring(0, 300));

  const submitResp = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${patToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement: sql,
      wait_timeout: "30s",
      disposition: "INLINE",
      format: "JSON_ARRAY",
    }),
  });

  if (!submitResp.ok) {
    const text = await submitResp.text();
    console.error("[S3 Lens BG] SQL submit HTTP error:", submitResp.status, text);
    throw new Error(`SQL submit failed (${submitResp.status}): ${text}`);
  }

  let data = await submitResp.json();
  console.log("[S3 Lens BG] SQL response state:", data.status?.state);

  // Poll if still running
  while (data.status && (data.status.state === "PENDING" || data.status.state === "RUNNING")) {
    await new Promise((r) => setTimeout(r, 1000));
    const pollResp = await fetch(`${url}/${data.statement_id}`, {
      headers: { Authorization: `Bearer ${patToken}` },
    });
    if (!pollResp.ok) {
      const text = await pollResp.text();
      throw new Error(`SQL poll failed (${pollResp.status}): ${text}`);
    }
    data = await pollResp.json();
    console.log("[S3 Lens BG] SQL poll state:", data.status?.state);
  }

  if (data.status && data.status.state === "FAILED") {
    const errMsg = data.status.error?.message || JSON.stringify(data.status);
    console.error("[S3 Lens BG] SQL FAILED:", errMsg);
    throw new Error(`SQL failed: ${errMsg}`);
  }

  const rowCount = data.result?.data_array?.length || 0;
  console.log("[S3 Lens BG] SQL succeeded, rows:", rowCount);
  return data;
}

// --------------- UUID Resolution ---------------

/**
 * Resolve typed UUIDs using system.information_schema.tables.storage_path.
 *
 * Strategy:
 * - Table UUIDs:   storage_sub_directory = 'tables/<uuid>'  -> exact match
 * - Schema UUIDs:  storage_path LIKE '%/schemas/<uuid>/%'   -> extract catalog.schema
 * - Catalog UUIDs: storage_path LIKE '%/catalogs/<uuid>/%'  -> extract catalog
 *
 * Input: array of { uuid, type } where type is "table"|"schema"|"catalog"
 * Returns: map of uuid -> { type, fullName }
 */
async function resolveUuids(typedUuids, config) {
  const { workspaceUrl, patToken, warehouseId } = config;
  const results = {};

  if (typedUuids.length === 0) return results;

  // Validate UUIDs to prevent SQL injection (belt-and-suspenders, content.js also validates)
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  const validUuids = typedUuids.filter((u) => UUID_RE.test(u.uuid));
  if (validUuids.length === 0) return results;

  console.log("[S3 Lens BG] Resolving", validUuids.length, "typed UUIDs:", validUuids);

  // Group by type
  const tableUuids = validUuids.filter((u) => u.type === "table");
  const schemaUuids = validUuids.filter((u) => u.type === "schema");
  const catalogUuids = validUuids.filter((u) => u.type === "catalog");

  // --- Resolve table UUIDs ---
  if (tableUuids.length > 0) {
    const conditions = tableUuids
      .map((u) => `'tables/${u.uuid}'`)
      .join(", ");

    try {
      const sql = `
        SELECT table_catalog, table_schema, table_name, storage_sub_directory
        FROM system.information_schema.tables
        WHERE storage_sub_directory IN (${conditions})
      `;
      const result = await executeSql(workspaceUrl, patToken, warehouseId, sql);

      if (result.result?.data_array) {
        for (const row of result.result.data_array) {
          const [catalog, schema, table, subDir] = row;
          // Extract UUID from "tables/<uuid>"
          const uuid = subDir.replace("tables/", "").toLowerCase();
          results[uuid] = {
            type: "table",
            fullName: `${catalog}.${schema}.${table}`,
          };
        }
      }
      console.log("[S3 Lens BG] Table UUIDs resolved:", Object.keys(results).length, "/", tableUuids.length);
    } catch (err) {
      console.error("[S3 Lens BG] Table lookup failed:", err.message);
    }
  }

  // --- Resolve schema UUIDs ---
  if (schemaUuids.length > 0) {
    for (const { uuid } of schemaUuids) {
      try {
        const sql = `
          SELECT DISTINCT table_catalog, table_schema
          FROM system.information_schema.tables
          WHERE storage_path LIKE '%/schemas/${uuid}/%'
          LIMIT 1
        `;
        const result = await executeSql(workspaceUrl, patToken, warehouseId, sql);

        if (result.result?.data_array?.length > 0) {
          const [catalog, schema] = result.result.data_array[0];
          results[uuid] = {
            type: "schema",
            fullName: `${catalog}.${schema}`,
          };
        }
      } catch (err) {
        console.error("[S3 Lens BG] Schema lookup failed for", uuid, ":", err.message);
      }
    }
    console.log("[S3 Lens BG] After schema resolution, total resolved:", Object.keys(results).length);
  }

  // --- Resolve catalog UUIDs ---
  if (catalogUuids.length > 0) {
    for (const { uuid } of catalogUuids) {
      try {
        const sql = `
          SELECT DISTINCT table_catalog
          FROM system.information_schema.tables
          WHERE storage_path LIKE '%/catalogs/${uuid}/%'
          LIMIT 1
        `;
        const result = await executeSql(workspaceUrl, patToken, warehouseId, sql);

        if (result.result?.data_array?.length > 0) {
          const [catalog] = result.result.data_array[0];
          results[uuid] = {
            type: "catalog",
            fullName: catalog,
          };
        }
      } catch (err) {
        console.error("[S3 Lens BG] Catalog lookup failed for", uuid, ":", err.message);
      }
    }
    console.log("[S3 Lens BG] After catalog resolution, total resolved:", Object.keys(results).length);
  }

  console.log("[S3 Lens BG] Final resolved results:", results);
  return results;
}

// --------------- Cache Management ---------------

async function getCachedResults(typedUuids) {
  const { uuidCache } = await getConfig();
  const now = Date.now();
  const cached = {};
  const uncached = [];

  for (const item of typedUuids) {
    const key = item.uuid.toLowerCase();
    const entry = uuidCache[key];
    if (entry && now - entry.cachedAt < CACHE_TTL_MS) {
      cached[key] = entry.data;
    } else {
      uncached.push(item);
    }
  }

  console.log(`[S3 Lens BG] Cache: ${Object.keys(cached).length} hit, ${uncached.length} miss`);
  return { cached, uncached };
}

async function updateCache(newResults) {
  const { uuidCache } = await getConfig();
  const now = Date.now();

  for (const [uuid, data] of Object.entries(newResults)) {
    uuidCache[uuid] = { data, cachedAt: now };
  }

  await setStorage({ uuidCache, cacheUpdatedAt: now });
}

// --------------- Test Connection ---------------

async function testConnection(config) {
  const result = await executeSql(
    config.workspaceUrl,
    config.patToken,
    config.warehouseId,
    "SELECT 1 AS ok"
  );

  if (
    result.status?.state === "SUCCEEDED" &&
    result.result?.data_array?.[0]?.[0] === "1"
  ) {
    return { success: true };
  }

  throw new Error(`Unexpected response: ${JSON.stringify(result.status)}`);
}

// --------------- Message Handler ---------------

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  console.log("[S3 Lens BG] Message:", message.action);
  handleMessage(message)
    .then(sendResponse)
    .catch((err) => {
      console.error("[S3 Lens BG] Handler error:", err.message);
      sendResponse({ error: err.message });
    });
  return true;
});

async function handleMessage(message) {
  const { action } = message;

  switch (action) {
    case "getConfig": {
      const config = await getConfig();
      const cacheSize = Object.keys(config.uuidCache).length;
      return {
        workspaceUrl: config.workspaceUrl,
        warehouseId: config.warehouseId,
        hasToken: !!config.patToken,
        cacheSize,
        cacheUpdatedAt: config.cacheUpdatedAt,
      };
    }

    case "saveConfig": {
      const updates = {};
      if (message.workspaceUrl !== undefined)
        updates.workspaceUrl = message.workspaceUrl.replace(/\/+$/, "");
      if (message.warehouseId !== undefined)
        updates.warehouseId = message.warehouseId;
      if (message.patToken !== undefined) updates.patToken = message.patToken;
      await setStorage(updates);
      return { success: true };
    }

    case "testConnection": {
      const config = await getConfig();
      return await testConnection(config);
    }

    case "clearCache": {
      await setStorage({ uuidCache: {}, cacheUpdatedAt: null });
      return { success: true };
    }

    case "lookupUuids": {
      // message.uuids is now an array of { uuid, type }
      const typedUuids = (message.uuids || []).map((u) => ({
        uuid: u.uuid.toLowerCase(),
        type: u.type,
      }));
      if (typedUuids.length === 0) return { matches: {} };

      console.log("[S3 Lens BG] lookupUuids:", typedUuids);

      // Check cache
      const { cached, uncached } = await getCachedResults(typedUuids);

      // Resolve uncached
      let freshResults = {};
      if (uncached.length > 0) {
        const config = await getConfig();
        if (!config.patToken) {
          return { matches: cached, error: "No PAT token configured" };
        }
        try {
          freshResults = await resolveUuids(uncached, config);
          await updateCache(freshResults);
        } catch (err) {
          console.error("[S3 Lens BG] Resolution failed:", err.message);
          return { matches: cached, error: err.message };
        }
      }

      const allMatches = { ...cached, ...freshResults };
      console.log("[S3 Lens BG] Returning", Object.keys(allMatches).length, "matches");
      return { matches: allMatches };
    }

    default:
      throw new Error(`Unknown action: ${action}`);
  }
}
