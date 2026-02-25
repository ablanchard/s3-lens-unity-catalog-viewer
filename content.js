// ============================================================
// content.js — Content script for S3 Storage Lens pages
// Scans for __unitystorage paths in the prefix table,
// resolves UUIDs to catalog.schema.table names via background,
// and injects human-readable badges inline.
// ============================================================

console.log("[S3 Lens Unity] Content script loaded on:", window.location.href);

const ANNOTATED_ATTR = "data-s3-lens-annotated";
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * Parse a __unitystorage path and extract typed UUIDs.
 *
 * Path patterns seen on the page:
 *   __unitystorage/schemas/<schema_uuid>/tables/<table_uuid>  -> schema + table UUIDs
 *   __unitystorage/catalogs/<catalog_uuid>/tables/<table_uuid> -> catalog + table UUIDs
 *   __unitystorage/catalogs/<catalog_uuid>                     -> catalog UUID only
 *   __unitystorage/schemas/<schema_uuid>/tables                -> schema UUID only (no table)
 *   __unitystorage/schemas                                     -> no UUID, skip
 *   __unitystorage/catalogs                                    -> no UUID, skip
 *
 * Returns array of { uuid, type } or null.
 */
function parseUnityPath(text) {
  const idx = text.indexOf("__unitystorage/");
  if (idx === -1) return null;

  const pathPart = text.substring(idx);
  const segments = pathPart.split("/");
  // segments[0] = "__unitystorage"
  // segments[1] = "schemas"|"catalogs"
  // segments[2] = <uuid> (maybe)
  // segments[3] = "tables" (maybe)
  // segments[4] = <uuid> (maybe)
  const results = [];

  for (let i = 1; i < segments.length; i += 2) {
    const kind = segments[i];
    const uuid = segments[i + 1];
    if (!uuid) continue;

    const cleanUuid = uuid.replace(/[^0-9a-f-]/gi, "").toLowerCase();
    if (!UUID_PATTERN.test(cleanUuid)) continue;

    let type;
    if (kind === "tables") type = "table";
    else if (kind === "schemas") type = "schema";
    else if (kind === "catalogs") type = "catalog";
    else continue;

    results.push({ uuid: cleanUuid, type });
  }

  return results.length > 0 ? results : null;
}

/**
 * Find all span.s3-util-word-break-all elements containing __unitystorage paths.
 * Returns { elements, typedUuids } where typedUuids is a Map<uuid, type>.
 */
function findUnityElements() {
  const elements = [];
  const typedUuids = new Map(); // uuid -> type

  const spans = document.querySelectorAll("span.s3-util-word-break-all");
  console.log(`[S3 Lens Unity] Found ${spans.length} s3-util-word-break-all spans`);

  let unityCount = 0;
  for (const span of spans) {
    if (span.hasAttribute(ANNOTATED_ATTR)) continue;

    const text = span.textContent || "";
    if (!text.includes("__unitystorage")) continue;
    unityCount++;

    const parsed = parseUnityPath(text);
    if (!parsed) continue;

    for (const { uuid, type } of parsed) {
      // Keep the most specific type if UUID appears multiple times
      const existing = typedUuids.get(uuid);
      if (!existing || typePriority(type) > typePriority(existing)) {
        typedUuids.set(uuid, type);
      }
    }

    elements.push({ el: span, parsed });
  }

  console.log(
    `[S3 Lens Unity] ${unityCount} contain __unitystorage, ${elements.length} have resolvable UUIDs, ${typedUuids.size} unique UUIDs`
  );

  return { elements, typedUuids };
}

function createBadge(info) {
  const badge = document.createElement("span");
  badge.className = "s3-lens-badge";
  badge.dataset.type = info.type;
  badge.textContent = info.fullName;
  badge.title = `Unity Catalog ${info.type}: ${info.fullName}`;
  return badge;
}

/**
 * Inject badges next to resolved UUIDs in the found elements.
 */
function annotateElements(elements, matchMap) {
  let annotatedCount = 0;

  for (const { el, parsed } of elements) {
    if (el.hasAttribute(ANNOTATED_ATTR)) continue;

    // Find the best (most specific) resolved match.
    let bestMatch = null;
    for (const { uuid } of parsed) {
      const info = matchMap[uuid];
      if (!info) continue;
      if (!bestMatch || typePriority(info.type) > typePriority(bestMatch.type)) {
        bestMatch = info;
      }
    }

    if (bestMatch) {
      const badge = createBadge(bestMatch);

      // Insert badge after the drill-down-menu-popover wrapper, not inside the button
      const button = el.closest("button");
      const popoverSpan = el.closest("span.drill-down-menu-popover") || (button && button.parentElement);

      if (popoverSpan && popoverSpan.parentNode) {
        popoverSpan.parentNode.insertBefore(badge, popoverSpan.nextSibling);
      } else if (button && button.parentNode) {
        button.parentNode.insertBefore(badge, button.nextSibling);
      } else {
        el.parentNode.insertBefore(badge, el.nextSibling);
      }

      el.setAttribute(ANNOTATED_ATTR, "true");
      annotatedCount++;
    }
  }

  console.log(`[S3 Lens Unity] Annotated ${annotatedCount} elements`);
}

function typePriority(type) {
  if (type === "table") return 3;
  if (type === "schema") return 2;
  if (type === "catalog") return 1;
  return 0;
}

/**
 * Main scan: find unity paths, resolve UUIDs, annotate.
 */
let scanInProgress = false;

async function scan() {
  if (scanInProgress) return;
  scanInProgress = true;

  try {
    const { elements, typedUuids } = findUnityElements();
    if (elements.length === 0 || typedUuids.size === 0) {
      console.log("[S3 Lens Unity] No unresolved unity elements found, skipping lookup");
      return;
    }

    // Convert Map to array of {uuid, type} for the background message
    const uuidsWithTypes = Array.from(typedUuids.entries()).map(
      ([uuid, type]) => ({ uuid, type })
    );

    console.log(`[S3 Lens Unity] Sending ${uuidsWithTypes.length} typed UUIDs to background`);

    const response = await chrome.runtime.sendMessage({
      action: "lookupUuids",
      uuids: uuidsWithTypes,
    });

    console.log("[S3 Lens Unity] Background response:", response);

    if (response && response.matches) {
      const matchCount = Object.keys(response.matches).length;
      console.log(`[S3 Lens Unity] Got ${matchCount} resolved matches`);
      annotateElements(elements, response.matches);
    }

    if (response && response.error) {
      console.warn("[S3 Lens Unity] Lookup warning:", response.error);
    }
  } catch (err) {
    console.error("[S3 Lens Unity] Scan error:", err.message, err);
  } finally {
    scanInProgress = false;
  }
}

// --------------- Initialization ---------------

console.log("[S3 Lens Unity] Running initial scan...");
scan();

let scanTimeout = null;
const observer = new MutationObserver(() => {
  clearTimeout(scanTimeout);
  scanTimeout = setTimeout(scan, 500);
});

observer.observe(document.body, {
  childList: true,
  subtree: true,
});

console.log("[S3 Lens Unity] MutationObserver active");
