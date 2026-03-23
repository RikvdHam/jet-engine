document.addEventListener("DOMContentLoaded", () => {
    const toggleBtn = document.getElementById("toggleSidebarBtn");
    const sidebar = document.getElementById("sidebar");

    toggleBtn?.addEventListener("click", () => {
        sidebar.classList.toggle("collapsed");
    });

    const backBtn = document.getElementById("backBtn");
    const continueBtn = document.getElementById("continueBtn");

    continueBtn.disabled = true;
});
