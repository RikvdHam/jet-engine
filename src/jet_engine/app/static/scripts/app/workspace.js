const appState = {
    datasetId: null,
    fieldsMeta: null,
    mappingState: {}
};

document.addEventListener("DOMContentLoaded", async () => {
    try {
        const datasetMeta = await loadDatasetMeta();
        appState.datasetId = datasetMeta.id;

        appState.fieldsMeta = await loadFields();
    } catch (err) {
        console.error("Initialization failed:", err);
    }
});

async function loadDatasetMeta() {
    const response = await fetch(`/api/datasets/session/latest`);
    return await response.json();
}

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

    return fields
}

function createField(field) {
    const fieldDiv = document.createElement("div");

    fieldDiv.className = "field";
    fieldDiv.draggable = true;

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
    meta.appendChild(metaSm);
    fieldDiv.appendChild(meta);

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