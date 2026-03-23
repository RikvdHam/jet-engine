
document.addEventListener("click", e => {
    const btn = e.target.closest(".validate-btn, #validateBtnMain");
    if (!btn) return;

    const submissionId = btn.dataset.submissionId;
    if (!submissionId) {
        console.warn("No submission ID found");
        return;
    }

    startValidation(submissionId);
});


function toggleSidebar() {
    document.getElementById("sidebar").classList.toggle("collapsed");
}

// --- Upload + Validation ---
const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("fileInput");
const progressSection = document.getElementById("progressSection");
const progressFill = document.getElementById("progressFill");
const fileInfo = document.getElementById("fileInfo");
const actionButtons = document.getElementById("actionButtons");
const validateBtnMain = document.getElementById("validateBtnMain");

dropzone.onclick = () => fileInput.click();

dropzone.ondragover = e => { e.preventDefault(); dropzone.classList.add("dragover"); };
dropzone.ondragleave = () => dropzone.classList.remove("dragover");
dropzone.ondrop = e => { e.preventDefault(); dropzone.classList.remove("dragover"); handleFile(e.dataTransfer.files[0]); };
fileInput.onchange = () => handleFile(fileInput.files[0]);

let validationCounter = 0;
function handleFile(file) {
    if (!file) return;
    validationCounter++;

    // Simulate template detection (pick random template)
    const detectedTemplate = templates[Math.floor(Math.random() * templates.length)];
    const submission = {id: 'user01_FIN-SEBS_02012026202300100', timestamp: Date.now(), fileName: file.name, template: 'FIN-SEBS', version: 'v2.1', status: 'UPLOADED'};

    // Simulate progress
    progressSection.style.display = "block";
    progressFill.style.width = "0%";
    fileInfo.textContent = `Uploading ${file.name}...`;

    // Create submission row


    let progress = 0;
    const interval = setInterval(() => {
        progress += 10;
        progressFill.style.width = progress + "%";

        if (progress >= 100) {
            clearInterval(interval);
            validateBtnMain.setAttribute('data-submission-id', submission.id);

            fileInfo.innerHTML = `
                <strong>${file.name}</strong><br>
                Detected template: <strong>${submission.template} (${submission.version})</strong>
            `;
            actionButtons.style.display = "flex";

            addSubmissionRow(submission, true);
        }
    }, 150);
