// Track the current state
let selectedFile = null;
let status = 'idle'

const companyInput = document.getElementById("companyName");
const fiscalSelect = document.getElementById("fiscalYear");
const uploadBtn = document.getElementById("uploadBtn");
const previewStatus = document.getElementById("previewStatus");
const continueBtn = document.getElementById("continueBtn");
const errorMsg = document.getElementById("errorMessage");

populateFiscalYears()


// Enable button if all conditions are met
function updateUploadButtonState() {
    const companyFilled = companyInput.value.trim() !== "";
    const fiscalFilled = fiscalSelect.value !== "Select year";
    uploadBtn.disabled = !(selectedFile && companyFilled && fiscalFilled);
}

// Watch for changes in file, company name, or fiscal year
companyInput.addEventListener("input", updateUploadButtonState);
fiscalSelect.addEventListener("change", updateUploadButtonState);

// Dropzone/file input
dropzone.onclick = () => fileInput.click();
dropzone.ondragover = e => { e.preventDefault(); dropzone.classList.add("dragover"); };
dropzone.ondragleave = () => dropzone.classList.remove("dragover");
dropzone.ondrop = e => {
    e.preventDefault();
    dropzone.classList.remove("dragover");
    selectedFile = e.dataTransfer.files[0];
    updateDropzoneLabel(selectedFile);
    resetUploadVisuals();
    updateUploadButtonState();
};
fileInput.onchange = () => {
    selectedFile = fileInput.files[0];
    updateDropzoneLabel(selectedFile);
    resetUploadVisuals();
    updateUploadButtonState();
};

// Optional: allow removing file
function removeSelectedFile() {
    selectedFile = null;
    document.getElementById("dropTitle").textContent = "Drag & drop your Excel file";
    document.getElementById("dropText").textContent = "or click to browse";
    updateUploadButtonState();
}

function resetUploadVisuals() {
    continueBtn.disabled = true;

    errorMsg.textContent = "No Errors";
    errorMsg.classList.add("hidden")

    uploadBtn.innerHTML = 'Upload';
    uploadBtn.disabled = false;

    progressFill.style.width = "0%";

    fileInfo.innerHTML = '';

    previewStatus.classList.remove("hidden");

    resetPreviewTable();
}

function updateDropzoneLabel(file) {
    if(file) {
        document.getElementById("dropTitle").textContent = file.name;
        document.getElementById("dropText").textContent = "Waiting for upload...";
    } else {
        document.getElementById("dropTitle").textContent = "Drag & drop your Excel file";
        document.getElementById("dropText").textContent = "or click to browse";
    }
}

function enableUploadingVisuals() {
    const loadingHtml = 'Uploading... <span class="animation-loader"></span>'

    uploadBtn.innerHTML = loadingHtml;
    uploadBtn.disabled = true;

    dropzone.classList.add("disabled");

    document.getElementById("dropText").textContent = 'Uploading...';
}

function showUploadSuccess(fileName, fileSize, rowCount, dataColumns, dataPreview) {
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

    dropzone.classList.remove("disabled");

    document.getElementById("dropText").textContent = "Upload complete";

    fileInfo.innerHTML = `
        <strong>${fileName}</strong> (${formatBytes(fileSize)})<br>
        Number of detected rows: <strong>rowCount</strong>
    `;

    previewStatus.classList.add("hidden");

    addPreviewRows(dataColumns, dataPreview);

    continueBtn.disabled = false;
}

function showUploadError(message) {
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

    errorMsg.textContent = message;
    errorMsg.classList.remove("hidden")

    dropzone.classList.remove("disabled");

    document.getElementById("dropText").textContent = 'Try again';

    progressFill.style.width = "0%";

    fileInfo.innerHTML = `
        Error: ${message}
    `;
}

document.getElementById("uploadBtn").onclick = () => {
    handleFile2(selectedFile);
};


function handleFile(file) {
    if (!file) return;

    enableUploadingVisuals()

    // Simulate progress
    progressSection.style.display = "block";
    progressFill.style.width = "0%";
    fileInfo.textContent = `Uploading ${file.name}...`;

    let progress = 0;
    const timeInMS = 0.0001 * file.size;
    const interval = setInterval(() => {
        progress = Math.min(progress + (100 / timeInMS), 90);
        progressFill.style.width = progress + "%";

        if (progress >= 100) {
            clearInterval(interval);

//            showUploadSuccess(file.name, 1327941)

            showUploadError("Test error message.")
        }
    }, 100);
}

async function handleFile2(file) {
    if (!file) return;

    enableUploadingVisuals()

    // Get company name and fiscal year
    const companyName = document.getElementById("companyName").value.trim();
    const fiscalYear = document.getElementById("fiscalYear").value;

    try {
        const formData = new FormData();
        formData.append("company_name", companyName);
        formData.append("fiscal_year", fiscalYear);
        formData.append("file", file);

        // Simulate progress bar while uploading
        progressSection.style.display = "block";
        progressFill.style.width = "0%";
        fileInfo.textContent = `Uploading ${file.name}...`;

        let progress = 0;
        const timeInMS = 0.0001 * file.size;
        const interval = setInterval(() => {
            progress = Math.min(progress + (100 / timeInMS), 90);
            progressFill.style.width = progress + "%";
        }, 100);

        // Send POST request to API
        const response = await fetch("/api/uploads/csv", {
            method: "POST",
            body: formData
        });

        clearInterval(interval); // stop simulated progress
        progressFill.style.width = "100%";

        console.log(response);

        if (!response.ok) {
            const err = await response.json();
            showUploadError(err.detail);
            return;
        }

        const data = await response.json();

        // Update file info with name + row count
        showUploadSuccess(file.name, file.size, data.row_count, data.columns, data.preview)

    } catch (err) {
        console.log(err);
        fileInfo.textContent = "Upload failed: " + err.message;
    }
}


function formatBytes(bytes, decimals = 2) {
    if (!+bytes) return '0 Bytes'

    const k = 1024
    const dm = decimals < 0 ? 0 : decimals
    const sizes = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']

    const i = Math.floor(Math.log(bytes) / Math.log(k))

    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`
}


function addPreviewRows(columns, rows) {
    const table = document.querySelector(".app-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    // Clear old content
    thead.innerHTML = "";
    tbody.innerHTML = "";

    table.classList.remove("is-empty");

    // --- Create header ---
    const headerRow = document.createElement("tr");

    columns.forEach(col => {
        const th = document.createElement("th");
        th.textContent = col;
        headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);

    // --- Create rows ---
    rows.forEach(row => {
        const tr = document.createElement("tr");

        columns.forEach(col => {
            const td = document.createElement("td");
            td.textContent = row[col] ?? ""; // ensures correct column order
            tr.appendChild(td);
        });

        tbody.appendChild(tr);
    });
}

function resetPreviewTable() {
    // --- Reset table preview ---
    const table = document.querySelector(".app-table");
    const thead = table.querySelector("thead");
    const tbody = table.querySelector("tbody");

    thead.innerHTML = `
        <tr>
            <th>Column A</th>
            <th>Column B</th>
            <th>Column C</th>
        </tr>
    `;

    tbody.innerHTML = `
        <tr class="empty-row">
            <td colspan="100%">
                <div class="empty-state">
                    <div class="empty-title">No data available</div>
                    <div class="empty-subtitle">
                        Upload a file to preview your dataset here
                    </div>
                </div>
            </td>
        </tr>
    `;

    table.classList.add("is-empty");
}


function populateFiscalYears() {
    const select = document.getElementById("fiscalYear");
    const currentYear = new Date().getFullYear();

    const startYear = currentYear - 10;  // adjust range
    const endYear = currentYear + 1;    // allow next year if needed

    // Reset
    select.innerHTML = `<option value="">Select year</option>`;

    for (let year = endYear; year >= startYear; year--) {
        const option = document.createElement("option");
        option.value = year;
        option.textContent = year;
        select.appendChild(option);
    }
}