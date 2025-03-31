/* UPDATE THESE VALUES TO MATCH YOUR SETUP */

// Declare the variables at the global level so they can be accessed by all functions
let PROCESSING_STATS_API_URL;
let ANALYZER_API_URL;

// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}

const updateCodeDiv = (result, elemId) => document.getElementById(elemId).innerText = JSON.stringify(result)

const getLocaleDateStr = () => (new Date()).toLocaleString()

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    // Only make requests if URLs are defined
    if (PROCESSING_STATS_API_URL && ANALYZER_API_URL) {
        makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))
        makeReq(ANALYZER_API_URL.stats, (result) => updateCodeDiv(result, "analyzer-stats"))
        makeReq(ANALYZER_API_URL.wind, (result) => updateCodeDiv(result, "event-snow"))
        makeReq(ANALYZER_API_URL.temp, (result) => updateCodeDiv(result, "event-lift"))
    } else {
        console.log("API URLs not yet loaded")
    }
}

const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    let msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}

const setup = () => {
    // Load config first, then start periodic updates
    fetch('config.json')
        .then(response => response.json())
        .then(data => {
            const BASE_URL = data.API_URL;
            console.log(BASE_URL); // Use the API_URL from config.json

            // Define the URLs using the BASE_URL after it has been fetched
            PROCESSING_STATS_API_URL = `http://${BASE_URL}:8100/stats`;
            ANALYZER_API_URL = {
                stats: `http://${BASE_URL}:8900/stats`,
                wind: `http://${BASE_URL}:8900/events/wind-speed?index=0`,
                temp: `http://${BASE_URL}:8900/events/temperature?index=0`
            };

            console.log(PROCESSING_STATS_API_URL);
            console.log(ANALYZER_API_URL);
            
            // Now that we have the URLs, we can start getting stats
            getStats();
            setInterval(() => getStats(), 4000); // Update every 4 seconds
        })
        .catch(error => {
            console.error('Error loading config:', error);
            updateErrorMessages(`Failed to load configuration: ${error.message}`);
        });
}

document.addEventListener('DOMContentLoaded', setup)