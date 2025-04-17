// Declare global variables
let PROCESSING_STATS_API_URL;
let ANALYZER_API_URL;
let CONSIST_API_URL;

// Simplified fetch function with better error logging
function makeReq(url, elemId) {
    console.log(`Attempting to fetch from: ${url} for element: ${elemId}`);
    
    fetch(url)
        .then(res => {
            console.log(`Response status for ${elemId}: ${res.status}`);
            if (!res.ok) {
                throw new Error(`HTTP Error ${res.status}: ${res.statusText}`);
            }
            return res.json();
        })
        .then(result => {
            console.log(`Success for ${elemId}:`, result);
            document.getElementById(elemId).innerText = JSON.stringify(result, null, 2);
        })
        .catch(error => {
            console.error(`Failed fetch for ${elemId}:`, error);
            document.getElementById(elemId).innerText = `Error: ${error.message}`;
        });
}

// Get the current local date/time as a string
function getLocaleDateStr() {
    return new Date().toLocaleString();
}

// Fetch only consistency checks
function getConsistencyChecks() {
    if (CONSIST_API_URL && CONSIST_API_URL.checks) {
        console.log("Getting consistency checks from:", CONSIST_API_URL.checks);
        makeReq(CONSIST_API_URL.checks, "checks");
    } else {
        console.log("CONSIST_API_URL.checks is not defined");
    }
}

// Trigger consistency update
function triggerUpdate() {
    if (CONSIST_API_URL && CONSIST_API_URL.update) {
        console.log("Triggering update at:", CONSIST_API_URL.update);
        
        fetch(CONSIST_API_URL.update, {
            method: "POST"
        })
        .then(res => {
            console.log("Update response status:", res.status);
            if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
            return res.json();
        })
        .then(data => {
            console.log("Consistency update triggered:", data);
            alert("Consistency update triggered successfully!");
            
            // After update is successful, get the checks
            setTimeout(getConsistencyChecks, 500);
        })
        .catch(error => {
            console.error("Failed to trigger update:", error);
            alert(`Failed to trigger consistency update: ${error.message}`);
        });
    } else {
        console.log("CONSIST_API_URL.update is not defined");
        alert("CONSIST_API_URL.update is not defined.");
    }
}

// Update all stats
function getStats() {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr();
    
    if (PROCESSING_STATS_API_URL && ANALYZER_API_URL) {
        makeReq(PROCESSING_STATS_API_URL, "processing-stats");
        makeReq(ANALYZER_API_URL.stats, "analyzer-stats");
        makeReq(ANALYZER_API_URL.wind, "event-snow");
        makeReq(ANALYZER_API_URL.temp, "event-lift");
    }
    
    // Make consistency check request separately
    getConsistencyChecks();
}

// Main setup function
function setup() {
    console.log("Setup function running...");
    
    // Load config file
    fetch('config.json')
        .then(response => response.json())
        .then(data => {
            const BASE_URL = data.API_URL;
            console.log("Loaded BASE_URL:", BASE_URL);

            // Define the URLs with the BASE_URL
            PROCESSING_STATS_API_URL = `http://${BASE_URL}/processing/stats`;
            ANALYZER_API_URL = {
                stats: `http://${BASE_URL}/analyzer/stats`,
                wind: `http://${BASE_URL}/analyzer/events/wind-speed?index=0`,
                temp: `http://${BASE_URL}/analyzer/events/temperature?index=0`
            };
            CONSIST_API_URL = {
                update: `http://${BASE_URL}/consistency/update`,
                checks: `http://${BASE_URL}/consistency/checks`
            };

            console.log("Processing URL:", PROCESSING_STATS_API_URL);
            console.log("Analyzer URLs:", ANALYZER_API_URL);
            console.log("Consistency URLs:", CONSIST_API_URL);
            
            // Initialize the dashboard
            getStats();
            setInterval(getStats, 4000); // Update every 4 seconds
            
            // Add event listener for the update button
            document.getElementById("update-button").addEventListener("click", triggerUpdate);
        })
        .catch(error => {
            console.error('Error loading config:', error);
            alert(`Failed to load configuration: ${error.message}`);
        });
}

// Initialize when the DOM is fully loaded
window.addEventListener('DOMContentLoaded', setup);

// Also expose functions globally for debugging
window.triggerUpdate = triggerUpdate;
window.getStats = getStats;
window.getConsistencyChecks = getConsistencyChecks;