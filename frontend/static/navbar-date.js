// Shared function to load latest price date in navbar
function loadNavbarLatestDate() {
    fetch('/api/latest_date')
        .then(response => response.json())
        .then(data => {
            const navbarDateElement = document.getElementById('navbarLatestDate');
            if (navbarDateElement) {
                navbarDateElement.textContent = data.latest_date || 'N/A';
            }
        })
        .catch(error => {
            console.error('Error loading latest date:', error);
            const navbarDateElement = document.getElementById('navbarLatestDate');
            if (navbarDateElement) {
                navbarDateElement.textContent = 'Error';
            }
        });
}

// Auto-load when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    loadNavbarLatestDate();
});

// Also try to load when the window is fully loaded (fallback)
window.addEventListener('load', function() {
    // Only load if the element still shows "Loading..."
    const navbarDateElement = document.getElementById('navbarLatestDate');
    if (navbarDateElement && navbarDateElement.textContent === 'Loading...') {
        loadNavbarLatestDate();
    }
}); 