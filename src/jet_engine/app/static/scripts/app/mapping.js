const appState = {
    datasetId: null,
    fieldsMeta: null,
    mappingState: {}
};

const continueBtn = document.getElementById("continueBtn");
continueBtn.addEventListener("click", () => {
    window.location.href = "/app/validate";
});


document.addEventListener("DOMContentLoaded", async () => {
    try {
        const datasetMeta = await loadDatasetMeta();
        appState.datasetId = datasetMeta.id;

        await loadTable();
        appState.fieldsMeta = await loadFields();

        initDragAndDrop();
        buildLoadBtnLogic();
        bindFocusButton();

        await applySuggestedMapping();

        updateMappingProgress();
        initUploadVisuals();
    } catch (err) {
        console.error("Initialization failed:", err);
    }
});

function bindFocusButton() {
    const focusBtn = document.querySelector(".focus-btn");

    focusBtn.addEventListener("click", () => {
        if (focusMode === "all") {
            focusMode = "unmapped";
            focusBtn.textContent = "Focus: Unmapped";
        } else {
            focusMode = "all";
            focusBtn.textContent = "Focus: All";
        }

        applyColumnFocus();
    });
}
const backBtn = document.getElementById("backBtn");
backBtn.addEventListener("click", () => {
    window.location.href = "/app/upload";
});

async function loadFields() {
    const response = await fetch("/api/meta/fields");
    const fields = await response.json();

    const container = document.querySelector(".fields-container");
    container.innerHTML = "";


    // Define group order
    const groupOrder = [
        "identification",
        "date",
        "monetary",
        "account",
        "workflow",
        "classification",
        "metadata"
    ];

    // Group fields
    const grouped = {};
    fields.forEach(field => {
        if (!grouped[field.group]) {
            grouped[field.group] = [];
        }
        grouped[field.group].push(field);
    });

    // Render groups in order
    groupOrder.forEach(groupName => {
        if (!grouped[groupName]) return;

        const groupFields = grouped[groupName];

        // Sort: mandatory first
        groupFields.sort((a, b) => b.is_mandatory - a.is_mandatory);

        const groupDiv = document.createElement("div");
        groupDiv.className = "field-group";

        // Title
        const title = document.createElement("div");
        title.className = "group-title";
        title.textContent = capitalize(groupName);

        groupDiv.appendChild(title);

        // Fields
        groupFields.forEach(field => {
            groupDiv.appendChild(createField(field));
        });

        container.appendChild(groupDiv);
    });

    // Add Next button back
    const button = document.createElement("button");
    button.id = "uploadBtn"
    button.classList.add("next-btn", "primary", "animation-loader-wrapper");
    button.disabled = true;
    button.textContent = "Load";

    container.appendChild(button);

    // Add message field
    const msgSpan = document.createElement("span");
    msgSpan.id = "uploadMsg"
    msgSpan.classList.add("hidden");
    msgSpan.textContent = "No message";

    container.appendChild(msgSpan);

    return fields
}

function createField(field) {
    const fieldDiv = document.createElement("div");

    fieldDiv.className = "mapping-field";
    if (field.is_mandatory) fieldDiv.classList.add("mandatory");

    fieldDiv.dataset.canonicalName = field.canonical_name;
    fieldDiv.dataset.dtype = field.dtype;
    fieldDiv.dataset.required = field.is_mandatory;

    fieldDiv.title = field.description;

    const meta = document.createElement("div");
    meta.className = "field-meta";

    const metaSm = document.createElement("div");
    metaSm.className = "field-meta-sm";

    const iconBox = document.createElement("span");
    iconBox.className = "dtype-icon-box";
    iconBox.innerHTML = getIcon(field.dtype);

    const name = document.createElement("span");
    name.className = "field-name";
    name.textContent = field.display_name;

    metaSm.appendChild(iconBox);
    metaSm.appendChild(name);

    const badge = document.createElement("span");
    badge.className = field.is_mandatory ? "required-badge" : "optional-badge";
    badge.textContent = field.is_mandatory ? "Required" : "Optional";

    meta.appendChild(metaSm);
    meta.appendChild(badge);

    // ✅ DROP ZONE WRAPPER (important for positioning X)
    const dropWrapper = document.createElement("div");
    dropWrapper.className = "drop-wrapper";

    const dropZone = document.createElement("div");
    dropZone.className = "drop-zone";
    dropZone.textContent = "Drop column here";

    const checkmark = document.createElement("span");
    checkmark.className = "mapped-check";
    checkmark.innerHTML = "✔";
    checkmark.style.display = "none";

    // REMOVE BUTTON (hidden by default)
    const removeBtn = document.createElement("span");
    removeBtn.className = "remove-mapping";
    removeBtn.innerHTML = "✕";
    removeBtn.style.display = "none";

    // Click handler
    removeBtn.addEventListener("click", () => {
        removeMapping(fieldDiv);
    });

    dropWrapper.appendChild(dropZone);
    dropWrapper.appendChild(checkmark);
    dropWrapper.appendChild(removeBtn);

    fieldDiv.appendChild(meta);
    fieldDiv.appendChild(dropWrapper);

    return fieldDiv;
}

function getIcon(dtype) {
    switch (dtype) {
        case "integer":
            return `
                <svg viewBox="0 0 24 24">
                    <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle">123</text>
                </svg>
            `;
        case "float":
            return `
                <svg viewBox="0 0 24 24">
                    <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle">1.23</text>
                </svg>
            `;
        case "str":
            return `
                <svg viewBox="0 0 24 24">
                    <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle">ABC</text>
                </svg>
            `;
        case "date":
            return `
                <svg viewBox="0 0 24 24">
                    <rect x="4" y="5" width="16" height="14" rx="2" stroke="currentColor" fill="none"/>
                    <line x1="4" y1="9" x2="20" y2="9" stroke="currentColor"/>
                </svg>
            `
        case "datetime":
            return `
                <svg viewBox="0 0 24 24">
                    <circle cx="12" cy="12" r="8" stroke="currentColor" fill="none"/>
                    <line x1="12" y1="12" x2="12" y2="8" stroke="currentColor"/>
                    <line x1="12" y1="12" x2="15" y2="12" stroke="currentColor"/>
                </svg>
            `
        default:
            return `
                <svg viewBox="0 0 24 24">
                    <text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle">ABC</text>
                </svg>
            `;
    }
}

function capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

async function loadDatasetMeta() {
    const response = await fetch(`/api/datasets/session/latest`);
    return await response.json();
}


function buildFieldIdMap(fieldsMeta) {
    const map = {};
    fieldsMeta.forEach(f => {
        map[f.id] = f;
    });
    return map;
}

async function applySuggestedMapping() {
    const response = await fetch(`/api/datasets/${appState.datasetId}/suggested-mapping`);
    const suggested = await response.json();
    console.log(suggested);

    if (!Object.keys(suggested).length) {
        return;
    }

    const fieldIdMap = buildFieldIdMap(appState.fieldsMeta);
    console.log(fieldIdMap);

    Object.entries(suggested).forEach(([columnName, fieldId]) => {
        const field = fieldIdMap[fieldId];
        if (!field) return;

        const fieldEl = document.querySelector(
            `[data-canonical-name="${field.canonical_name}"]`
        );
        if (!fieldEl) return;

        const header = document.querySelector(
            `th[data-column-name="${columnName}"]`
        );
        if (!header) return;

        const columnType = header.dataset.dtype;

        applyMapping(fieldEl, columnName);
    });

    validateMapping();
    updateMappingProgress();
    applyColumnFocus();
}

function applyMapping(fieldEl, columnName) {
    const fieldName = fieldEl.dataset.canonicalName;

    removeExistingMapping(columnName);

    appState.mappingState[fieldName] = columnName;

    updateFieldUI(fieldEl, columnName);
}

async function loadTable() {
    const response = await fetch(`/api/datasets/${appState.datasetId}/data?limit=50&offset=0`);
    if (!response.ok) {
        return;
    }

    const result = await response.json();

    const table = document.querySelector(".mapping-table");
    const thead = table.querySelector("thead tr");
    const tbody = table.querySelector("tbody");

    // Clear existing
    thead.innerHTML = "";
    tbody.innerHTML = "";

    // =========================
    // HEADER
    // =========================

    // Sticky index column
    const thIndex = document.createElement("th");
    thIndex.className = "sticky-col";
    thIndex.textContent = "#";
    thead.appendChild(thIndex);

    // Data columns
    result.columns.forEach(col => {
        const th = document.createElement("th");
        th.setAttribute("draggable", "true");

        // 🔥 Store metadata
        th.dataset.columnName = col.name;
        th.dataset.dtype = col.dtype;

        const headerDiv = document.createElement("div");
        headerDiv.className = "column-header";

        const nameSpan = document.createElement("span");
        nameSpan.className = "col-name";
        nameSpan.textContent = col.name;

        const iconBox = document.createElement("span");
        iconBox.className = "dtype-icon-box";
        iconBox.innerHTML = getIcon(col.dtype);

        headerDiv.appendChild(nameSpan);
        headerDiv.appendChild(iconBox);
        th.appendChild(headerDiv);

        thead.appendChild(th);
    });

    // =========================
    // BODY
    // =========================

    result.data.forEach((row, index) => {
        const tr = document.createElement("tr");

        // Row index
        const tdIndex = document.createElement("td");
        tdIndex.className = "sticky-col";
        tdIndex.textContent = result.offset + index + 1;
        tr.appendChild(tdIndex);

        // Data cells
        result.columns.forEach(col => {
            const td = document.createElement("td");
            td.textContent = row[col.name];
            tr.appendChild(td);
        });

        tbody.appendChild(tr);
    });
}

function enableColumnDrag() {
    const headers = document.querySelectorAll(".mapping-table th[draggable='true']");

    headers.forEach(th => {
        th.addEventListener("dragstart", (e) => {
            const columnName = th.dataset.columnName;
            const dtype = th.dataset.dtype;

            e.dataTransfer.setData("columnName", columnName);
            e.dataTransfer.setData("dtype", dtype);

            e.dataTransfer.effectAllowed = "move";
        });
    });
}

function enableFieldDrop() {
    const fields = document.querySelectorAll(".mapping-field");

    fields.forEach(field => {

        // Allow drop
        field.addEventListener("dragover", (e) => {
            e.preventDefault();
            field.classList.add("drag-over");
        });

        field.addEventListener("dragleave", () => {
            field.classList.remove("drag-over");
        });

        // Handle drop
        field.addEventListener("drop", (e) => {
            e.preventDefault();
            field.classList.remove("drag-over");

            const columnName = e.dataTransfer.getData("columnName");
            const dtype = e.dataTransfer.getData("dtype");

            handleDrop(field, columnName, dtype);
        });
    });
}

function removeMapping(fieldEl) {
    const fieldName = fieldEl.dataset.canonicalName;

    delete appState.mappingState[fieldName];

    resetFieldUI(fieldEl);

    validateMapping();
    applyColumnFocus();
    updateMappingProgress();
    resetUploadVisuals();
}


function handleDrop(fieldEl, columnName, columnDtype) {
    const fieldDtype = fieldEl.dataset.dtype;

    applyMapping(fieldEl, columnName);

    validateMapping();
    updateMappingProgress();
    applyColumnFocus();
}

function isCompatible(fieldType, columnType) {
    if (fieldType === columnType) return true;

    // Allow some flexibility
    if (fieldType === "float" && columnType === "integer") return true;
    if (fieldType === "datetime" && columnType === "str") return true;

    return false;
}

function removeExistingMapping(columnName) {
    for (const key in appState.mappingState) {
        if (appState.mappingState[key] === columnName) {
            delete appState.mappingState[key];

            const field = document.querySelector(`[data-canonical-name="${key}"]`);
            resetFieldUI(field);
        }
    }
}

function updateFieldUI(fieldEl, columnName) {
    const dropZone = fieldEl.querySelector(".drop-zone");
    const removeBtn = fieldEl.querySelector(".remove-mapping");
    const checkmark = fieldEl.querySelector(".mapped-check");

    dropZone.textContent = columnName;
    fieldEl.classList.add("mapped");

    removeBtn.style.display = "block";
    removeBtn.title = "Remove mapping";
    checkmark.style.display = "inline";

    applyColumnFocus();

    updateMappingProgress();
    resetUploadVisuals();
}

function resetFieldUI(fieldEl) {
    const dropZone = fieldEl.querySelector(".drop-zone");
    const removeBtn = fieldEl.querySelector(".remove-mapping");
    const checkmark = fieldEl.querySelector(".mapped-check");

    dropZone.textContent = "Drop column here";
    fieldEl.classList.remove("mapped");

    removeBtn.style.display = "none";
    checkmark.style.display = "none";

    applyColumnFocus();

    updateMappingProgress();
}

function validateMapping() {
    const requiredFields = document.querySelectorAll(".mapping-field[data-required='true']");
    const nextBtn = document.querySelector(".next-btn");

    let allMapped = true;

    requiredFields.forEach(field => {
        const key = field.dataset.canonicalName;
        if (!appState.mappingState[key]) {
            allMapped = false;
        }
    });

    nextBtn.disabled = !allMapped;
}

function buildMappingPayload() {
    const mapping = {};

    Object.entries(appState.mappingState).forEach(([fieldCanonical, columnName]) => {
        const field = appState.fieldsMeta.find(
            f => f.canonical_name === fieldCanonical
        );

        if (field && columnName) {
            mapping[columnName] = field.id;
        }
    });

    return { mapping };
}

function initDragAndDrop() {
    enableColumnDrag();
    enableFieldDrop();
}

function buildLoadBtnLogic() {
    const nextBtn = document.querySelector(".next-btn");

    nextBtn.addEventListener("click", async () => {
        try {
            showUploadingVisuals();

            const payload = buildMappingPayload();

            const response = await fetch(
                `/api/datasets/${appState.datasetId}/save-mapping`,
                {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload)
                }
            );

            if (!response.ok) {
                const err = await response.json();
                showUploadError(err.detail);
                return;
            }

            showUploadSuccess();

        } catch (err) {
            showUploadError(err.message);
        }
    });
}


let focusMode = "all"; // "all" | "unmapped"

function getMappedColumns() {
    return Object.values(appState.mappingState);
}

function applyColumnFocus() {
    const mappedColumns = getMappedColumns();

    const table = document.querySelector(".mapping-table");
    const headers = table.querySelectorAll("thead th");
    const rows = table.querySelectorAll("tbody tr");

    headers.forEach((th, colIndex) => {
        if (colIndex === 0) return; // skip index column

        const columnName = th.dataset.columnName;
        const isMapped = mappedColumns.includes(columnName);

        let visible = true;
        let dimmed = false;

        if (focusMode === "unmapped") {
            visible = !isMapped;
        } else if (focusMode === "all") {
            dimmed = isMapped;
        }

        // HEADER
        th.style.display = visible ? "" : "none";
        th.classList.toggle("dimmed", dimmed);

        // ROW CELLS
        rows.forEach(tr => {
            const td = tr.children[colIndex];
            if (!td) return;

            td.style.display = visible ? "" : "none";
            td.classList.toggle("dimmed", dimmed);
        });
    });
};

const focusBtn = document.querySelector(".focus-btn");

focusBtn.addEventListener("click", () => {
    if (focusMode === "all") {
        focusMode = "unmapped";
        focusBtn.textContent = "Focus: Unmapped";
    } else {
        focusMode = "all";
        focusBtn.textContent = "Focus: All";
    }

    applyColumnFocus();
});

function allRequiredFieldsMapped() {
    const requiredFields = document.querySelectorAll(
        ".mapping-field[data-required='true']"
    );

    let mappedCount = 0;

    requiredFields.forEach(field => {
        const key = field.dataset.canonicalName;

        if (appState.mappingState[key]) {
            mappedCount++;
        }
    });

    const total = requiredFields.length;

    return total === mappedCount;
}

function updateMappingProgress() {
    const requiredFields = document.querySelectorAll(
        ".mapping-field[data-required='true']"
    );

    let mappedCount = 0;

    requiredFields.forEach(field => {
        const key = field.dataset.canonicalName;

        if (appState.mappingState[key]) {
            mappedCount++;
        }
    });

    const total = requiredFields.length;

    const progressEl = document.querySelector(".mapping-progress");
    progressEl.textContent = `${mappedCount}/${total}`;

    // Optional: visual feedback
    progressEl.classList.toggle("complete", mappedCount === total);
};

function showUploadingVisuals() {
    const uploadBtn = document.getElementById("uploadBtn")
    const loadingHtml = 'Uploading... <span class="animation-loader"></span>'
    uploadBtn.innerHTML = loadingHtml;
    uploadBtn.disabled = true;
}

function initUploadVisuals() {
    if (allRequiredFieldsMapped()) {
        showUploadSuccess();
    } else {
        resetUploadVisuals();
    }
}

function resetUploadVisuals() {
    const uploadBtn = document.getElementById("uploadBtn");
    const uploadMsg = document.getElementById("uploadMsg");

    uploadMsg.classList.add("hidden");
    uploadMsg.classList.remove("success", "error");
    uploadMsg.textContent = "No message"

    uploadBtn.innerHTML = 'Upload';

    if (allRequiredFieldsMapped()) {
        uploadBtn.disabled = false;
    } else {
        uploadBtn.disabled = true;
    }

    continueBtn.disabled = true;
}

function showUploadSuccess() {
    const uploadBtn = document.getElementById("uploadBtn");
    const uploadMsg = document.getElementById("uploadMsg");

    uploadBtn.disabled = true;
    uploadBtn.innerHTML = `
        Uploaded
        <span class="checkmark">
            <svg viewBox="0 0 52 52">
                <circle class="checkmark-circle" cx="26" cy="26" r="25"/>
                <path class="checkmark-check" d="M14 27l7 7 16-16"/>
            </svg>
        </span>
    `;
    uploadBtn.classList.add("success");

    uploadMsg.textContent = "Mapping successfully uploaded.";
    uploadMsg.classList.add("success")
    uploadMsg.classList.remove("hidden");

    continueBtn.disabled = false;
}

function showUploadError(message) {
    const uploadBtn = document.getElementById("uploadBtn");
    const uploadMsg = document.getElementById("uploadMsg");

    uploadBtn.disabled = true;
    uploadBtn.innerHTML = `
        Upload failed
        <span class="error-icon">
            <svg viewBox="0 0 52 52">
                <circle class="error-circle" cx="26" cy="26" r="25"/>
                <path class="error-cross" d="M16 16 L36 36 M36 16 L16 36"/>
            </svg>
        </span>
    `;
    uploadBtn.classList.add("error");

    uploadMsg.textContent = message;
    uploadMsg.classList.add("error");
    uploadMsg.classList.remove("hidden");

    continueBtn.disabled = true;
}