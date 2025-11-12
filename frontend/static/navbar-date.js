// Shared function to load latest price date in navbar
function loadNavbarLatestDate() {
    fetch('/api/frontend/latest_date')
        .then(response => response.json())
        .then(data => {
            const navbarDateElement = document.getElementById('navbarLatestDate');
            if (navbarDateElement) {
                // Show last update timestamp if available, otherwise fall back to trading date
                if (data.last_update_formatted) {
                    navbarDateElement.textContent = data.last_update_formatted;
                } else if (data.latest_date) {
                    navbarDateElement.textContent = data.latest_date;
                } else {
                    navbarDateElement.textContent = 'N/A';
                }
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