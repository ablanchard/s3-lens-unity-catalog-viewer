// ============================================================
// popup.js — Popup UI logic for S3 Lens Unity Viewer
// ============================================================

const $ = (sel) => document.querySelector(sel);

const workspaceInput = $("#workspace-url");
const patTokenInput = $("#pat-token");
const warehouseInput = $("#warehouse-id");
const saveBtn = $("#save-btn");
const testBtn = $("#test-btn");
const cacheStats = $("#cache-stats");
const cacheUpdated = $("#cache-updated");
const clearCacheBtn = $("#clear-cache-btn");
const messageDiv = $("#message");

// --------------- Helpers ---------------

function showMessage(text, type = "info") {
  messageDiv.textContent = text;
  messageDiv.className = `message ${type}`;
  messageDiv.classList.remove("hidden");
  setTimeout(() => messageDiv.classList.add("hidden"), 4000);
}

function sendMessage(msg) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(msg, resolve);
  });
}

function formatTimestamp(ts) {
  if (!ts) return "Never";
  return new Date(ts).toLocaleString();
}

// --------------- UI Update ---------------

async function updateUI() {
  const config = await sendMessage({ action: "getConfig" });
  if (config.error) {
    showMessage(config.error, "error");
    return;
  }

  workspaceInput.value = config.workspaceUrl || "";
  warehouseInput.value = config.warehouseId || "";
  // Don't populate PAT in the field (security), but show placeholder hint
  patTokenInput.placeholder = config.hasToken ? "********** (saved)" : "dapi...";

  cacheStats.textContent = `${config.cacheSize} resolved UUIDs cached`;
  cacheUpdated.textContent = config.cacheUpdatedAt
    ? `Last updated: ${formatTimestamp(config.cacheUpdatedAt)}`
    : "";
}

// --------------- Event Handlers ---------------

saveBtn.addEventListener("click", async () => {
  const updates = {};
  const wsUrl = workspaceInput.value.trim();
  const warehouse = warehouseInput.value.trim();
  const pat = patTokenInput.value.trim();

  if (!wsUrl) {
    showMessage("Workspace URL is required", "error");
    return;
  }
  if (!warehouse) {
    showMessage("Warehouse ID is required", "error");
    return;
  }

  updates.workspaceUrl = wsUrl;
  updates.warehouseId = warehouse;
  // Only update PAT if user actually typed something new
  if (pat) {
    updates.patToken = pat;
  }

  const result = await sendMessage({ action: "saveConfig", ...updates });
  if (result.error) {
    showMessage(result.error, "error");
  } else {
    showMessage("Configuration saved", "success");
    patTokenInput.value = "";
    await updateUI();
  }
});

testBtn.addEventListener("click", async () => {
  testBtn.disabled = true;
  testBtn.textContent = "Testing...";

  const result = await sendMessage({ action: "testConnection" });

  testBtn.disabled = false;
  testBtn.textContent = "Test Connection";

  if (result.error) {
    showMessage(`Connection failed: ${result.error}`, "error");
  } else {
    showMessage("Connection successful!", "success");
  }
});

clearCacheBtn.addEventListener("click", async () => {
  const result = await sendMessage({ action: "clearCache" });
  if (result.error) {
    showMessage(result.error, "error");
  } else {
    showMessage("Cache cleared", "success");
    await updateUI();
  }
});

// --------------- Init ---------------
updateUI();
