// ==============================================================================
// Script: map.js
//
// Description:
//   This script contains all the core client-side logic for the WWTP Map Viewer.
//   It handles:
//   - Initializing the Leaflet map and its layers.
//   - Managing application state (filters, selected year, etc.).
//   - Fetching and processing facility and nutrient data from JSON files.
//   - Performing proximity matching to identify Anaerobic Digestion facilities.
//   - Dynamically creating and updating map markers and clusters.
//   - Generating charts within marker popups using Chart.js.
//   - Handling all user interactions with the filter controls.
//
// Usage:
//   This script is not run directly. It is loaded by `index.html` and
//   executed by the user's web browser.
// ==============================================================================


// --- CONSTANTS AND HELPERS ---

const AD_FILTER_OPTIONS = { ALL: 'all', ONLY: 'only_ad', EXCLUDE: 'exclude_ad' };
const ALL_STATES_VALUE = 'ALL';
const MARKER_COLORS = {
  AD_FACILITY: '#4CAF50', // A distinct green for anaerobic digestion facilities.
  OTHER_FACILITY: '#0078A8' // Blue for all other facilities.
};
const PROXIMITY_THRESHOLD = 0.01; // ~1.1km / 0.7 miles

// ... (all the functions from the previous response are here) ...

function getMarkerColor(site) {
    return anaerobicDigestionFacilities.has(site.npdes)
      ? MARKER_COLORS.AD_FACILITY
      : MARKER_COLORS.OTHER_FACILITY;
}

function getJsonPath(year, stateCode) {
    const yearAsNumber = parseInt(year, 10);
    let yearDirectory, filenameYear;

    if (yearAsNumber < 2009) {
        yearDirectory = 'PREFY2009';
        filenameYear = 'PREFY2009';
    } else {
        yearDirectory = year;
        filenameYear = year;
    }
    return `./leaflet_dmr_json/${yearDirectory}/${stateCode}/${stateCode}_${filenameYear}.json`;
}


// --- MAP INITIALIZATION ---

const map = L.map('map', { zoomControl: false }).setView([39.8283, -98.5795], 4);
L.control.zoom({ position: 'topright' }).addTo(map);

const osmLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 18, attribution: '© OpenStreetMap' });
const satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', { attribution: 'Tiles &copy; Esri' });
osmLayer.addTo(map);

const baseMaps = { "OpenStreetMap": osmLayer, "Satellite": satelliteLayer };
L.control.layers(baseMaps, null, { position: 'topright' }).addTo(map);


// --- GLOBAL VARIABLES & STATE ---
let currentLayer = null;
let anaerobicDigestionFacilities = new Set();
let adLocations = [];

const appState = {
  pollutant: "",
  state: null,
  year: null,
  adFilter: AD_FILTER_OPTIONS.ALL
};

// --- DOM ELEMENT CACHING ---
const pollutantSelect = document.getElementById("pollutantFilter");
const stateSelect = document.getElementById("stateFilter");
const yearSelect = document.getElementById("yearFilter");
const adSelect = document.getElementById("adFilter");
const siteCountValue = document.getElementById('site-count-value');
const loadingIndicator = document.getElementById('loading-indicator');
const progressBar = document.getElementById('progress-bar');
const progressText = document.getElementById('progress-text');
const notificationBar = document.getElementById('notification-bar');


// --- DATA PROCESSING AND DISPLAY ---

let resetViewBtn = null;

function findAdFacilityMatches(allSitesData, adFacilityLocations) {
    console.log("Starting proximity match to identify AD facilities...");
    progressText.textContent = 'Matching facilities...';
    
    const matchedNpdresIds = new Set();
    const adGrid = new Map();
    adFacilityLocations.forEach(loc => {
        if (loc.lat && loc.lon) {
            const key = `${Math.round(loc.lat * 100)},${Math.round(loc.lon * 100)}`;
            if (!adGrid.has(key)) adGrid.set(key, []);
            adGrid.get(key).push(loc);
        }
    });

    allSitesData.forEach(site => {
        if (site.lat && site.lon) {
            for (let dLat = -1; dLat <= 1; dLat++) {
                for (let dLon = -1; dLon <= 1; dLon++) {
                    const key = `${Math.round(site.lat * 100) + dLat},${Math.round(site.lon * 100) + dLon}`;
                    if (adGrid.has(key)) {
                        for (const adFacility of adGrid.get(key)) {
                            const latDiff = Math.abs(site.lat - adFacility.lat);
                            const lonDiff = Math.abs(site.lon - adFacility.lon);
                            if (latDiff < PROXIMITY_THRESHOLD && lonDiff < PROXIMITY_THRESHOLD) {
                                matchedNpdresIds.add(site.npdes);
                                dLat = 2; // break outer loop
                                break; // break inner loop
                            }
                        }
                    }
                }
            }
        }
    });
    console.log(`Proximity match complete. Identified ${matchedNpdresIds.size} AD facilities in the current dataset.`);
    return matchedNpdresIds;
}

function processAndDisplayData(sitesData, pollutantName, adFilterValue, countElement, selectedYear) {
    let filteredSites = sitesData;

    if (pollutantName) {
        filteredSites = sitesData.filter(site => site.pollutant.replace(',', '') === pollutantName);
    }
    
    filteredSites = filteredSites.map(site => {
        const yearMeasurements = site.measurements.filter(m => m.date.endsWith('/' + selectedYear));
        return { ...site, measurements: yearMeasurements };
    }).filter(site => site.measurements.length > 0);

    if (adFilterValue === AD_FILTER_OPTIONS.ONLY) {
        filteredSites = filteredSites.filter(site => anaerobicDigestionFacilities.has(site.npdes));
    } else if (adFilterValue === AD_FILTER_OPTIONS.EXCLUDE) {
        filteredSites = filteredSites.filter(site => !anaerobicDigestionFacilities.has(site.npdes));
    }
    
    countElement.textContent = filteredSites.length;

    if (currentLayer) map.removeLayer(currentLayer);
    if (filteredSites.length === 0) {
        if (resetViewBtn) resetViewBtn.classList.add('hidden');
        return;
    }
    if (resetViewBtn) resetViewBtn.classList.remove('hidden');

    const markers = L.markerClusterGroup({ chunkedLoading: true });

    filteredSites.forEach(site => {
        if (site.lat && site.lon) {
            const markerColor = getMarkerColor(site);
            const customIcon = L.divIcon({
                className: 'custom-div-icon',
                html: `<div class='marker-pin' style='background-color:${markerColor};'></div>`,
                iconSize: [30, 42],
                iconAnchor: [15, 42]
            });
            const marker = L.marker([site.lat, site.lon], { icon: customIcon });
            marker.bindPopup(`<strong>${site.npdes}</strong><br>Nutrient: ${site.pollutant}<br><div class="chart-container"><canvas id="chart-${site.npdes}"></canvas></div>`);
            marker.on('popupopen', () => {
                setTimeout(() => {
                    const canvas = document.getElementById(`chart-${site.npdes}`);
                    if (canvas && site.measurements && site.measurements.length > 0) {
                        const sortedMeasurements = [...site.measurements].sort((a, b) => new Date(a.date) - new Date(b.date));
                        const chartLabels = sortedMeasurements.map(m => m.date);
                        const chartData = sortedMeasurements.map(m => m.value);
                        if (canvas.chart) canvas.chart.destroy();
                        canvas.chart = new Chart(canvas.getContext('2d'), {
                            type: 'line',
                            data: { labels: chartLabels, datasets: [{ label: `Value (${site.unit})`, data: chartData, borderColor: '#007bff', tension: 0.1, fill: false }] },
                            options: { responsive: true, maintainAspectRatio: false, scales: { x: { ticks: { maxRotation: 90, minRotation: 70, font: { size: 10 } } }, y: { ticks: { font: { size: 10 } } } }, plugins: { legend: { display: false } } }
                        });
                    }
                }, 10);
            });
            markers.addLayer(marker);
        }
    });

    markers.addTo(map);
    currentLayer = markers;
    if (markers.getBounds().isValid()) {
        map.fitBounds(markers.getBounds().pad(0.1));
    }
}


// --- UI AND STATE MANAGEMENT ---

function showNotification(message, isError = true) { 
  notificationBar.textContent = message;
  notificationBar.className = isError ? 'error' : 'success';
  notificationBar.style.display = 'block';
  setTimeout(() => { notificationBar.style.display = 'none'; }, 5000);
}

function updateURL() {
  const params = new URLSearchParams();
  params.set('year', appState.year);
  params.set('state', appState.state);
  params.set('pollutant', appState.pollutant);
  params.set('ad', appState.adFilter);
  window.history.replaceState({}, '', `${window.location.pathname}?${params}`);
}

function updateMap() {
  appState.pollutant = pollutantSelect.value;
  appState.state = stateSelect.value;
  appState.year = yearSelect.value;
  appState.adFilter = adSelect.value;
  updateURL();

  if (!appState.state || !appState.year) {
    if (currentLayer) map.removeLayer(currentLayer);
    currentLayer = null; 
    siteCountValue.textContent = '0';
    if (resetViewBtn) resetViewBtn.classList.add('hidden');
    return;
  }

  loadingIndicator.classList.remove('hidden');
  
  const dataFetchPromise = (appState.state === ALL_STATES_VALUE)
    ? fetchAllStatesData(appState.year)
    : fetchSingleStateData(appState.year, appState.state);

  dataFetchPromise
    .then(allSitesData => {
      anaerobicDigestionFacilities = findAdFacilityMatches(allSitesData, adLocations);
      processAndDisplayData(allSitesData, appState.pollutant, appState.adFilter, siteCountValue, appState.year);
    })
    .catch(err => {
      if(currentLayer) map.removeLayer(currentLayer);
      currentLayer = null; 
      siteCountValue.textContent = '0';
      if (resetViewBtn) resetViewBtn.classList.add('hidden');
      showNotification(err.message || `Could not load data for ${appState.state} ${appState.year}.`);
    })
    .finally(() => loadingIndicator.classList.add('hidden'));
}

function fetchAllStatesData(year) {
    let completed = 0, total = stateCodes.length;
    progressBar.style.width = '0%';
    progressText.textContent = `Fetching data... (0/${total})`;
    const promises = stateCodes.map(code => {
      const path = getJsonPath(year, code);
      return fetch(path)
        .then(res => res.ok ? res.json() : null)
        .finally(() => {
          completed++;
          progressBar.style.width = `${Math.round((completed/total)*100)}%`;
          progressText.textContent = `Fetching data... (${completed}/${total})`;
        });
    });
    return Promise.all(promises).then(results => results.filter(Boolean).flat());
}

function fetchSingleStateData(year, state) {
    progressText.textContent = 'Loading...'; 
    progressBar.style.width = '50%';
    const path = getJsonPath(year, state);
    return fetch(path).then(res => {
        if (!res.ok) throw new Error(`File not found for ${state} in ${year}`);
        return res.json();
    });
}


// --- INITIALIZATION LOGIC ---

function initializeApp() {
    // Add controls to map
    const ResetViewControl = L.Control.extend({
      options: { position: 'topright' },
      onAdd: function(map) {
        const button = L.DomUtil.create('div', 'leaflet-control leaflet-bar hidden');
        button.id = 'reset-view-btn';
        button.title = 'Reset View';
        button.innerHTML = '⌖';
        L.DomEvent.on(button, 'click', L.DomEvent.stopPropagation)
                  .on(button, 'click', L.DomEvent.preventDefault)
                  .on(button, 'click', () => {
                    if (currentLayer && currentLayer.getBounds().isValid()) {
                      map.fitBounds(currentLayer.getBounds().pad(0.1));
                    }
                  });
        resetViewBtn = button;
        return button;
      },
      onRemove: function(map) { L.DomEvent.off(resetViewBtn); resetViewBtn = null; }
    });
    new ResetViewControl().addTo(map);

    const legend = L.control({ position: 'bottomright' });
    legend.onAdd = function (map) {
        const div = L.DomUtil.create('div', 'info legend');
        const labels = [
            { color: MARKER_COLORS.AD_FACILITY, text: 'Anaerobic Digestion' },
            { color: MARKER_COLORS.OTHER_FACILITY, text: 'Other Facility' }
        ];
        div.innerHTML += '<div class="title">Facility Type</div>';
        labels.forEach(label => {
            div.innerHTML += `<i style="background:${label.color}"></i> ${label.text}<br>`;
        });
        return div;
    };
    legend.addTo(map);

    // Populate filters
    pollutantData.forEach(p => pollutantSelect.add(new Option(p.displayName, p.displayName)));
    availableYears.forEach(y => yearSelect.add(new Option(y, y)));
    stateSelect.add(new Option("-- All States --", ALL_STATES_VALUE));
    stateCodes.forEach(s => stateSelect.add(new Option(s, s)));

    // Set up event listeners
    pollutantSelect.addEventListener("change", updateMap);
    stateSelect.addEventListener("change", updateMap);
    yearSelect.addEventListener("change", updateMap);
    adSelect.addEventListener("change", updateMap);

    // Load initial state from URL
    loadFromURL();
}

function loadFromURL() {
  const params = new URLSearchParams(window.location.search);
  yearSelect.value = params.get('year') || availableYears[availableYears.length - 1];
  stateSelect.value = params.get('state') || ALL_STATES_VALUE;
  pollutantSelect.value = params.get('pollutant') || "";
  adSelect.value = params.get('ad') || AD_FILTER_OPTIONS.ALL;
  
  updateMap();
}

// --- STARTUP SEQUENCE ---
// The application execution begins here when the script is loaded.
// The entire app initialization is dependent on first fetching the list of AD facilities.
// This is an asynchronous operation, so the rest of the app setup is placed
// inside the .then() block to ensure it only runs after the fetch is successful.
console.log("Fetching AD facility list...");
fetch('./AnaerobicDigestionFacilities.csv') // Attempt to fetch the CSV file from the root directory.
  .then(response => {
      // Check if the HTTP request was successful.
      if (!response.ok) {
          throw new Error(`Could not load AD facility CSV. Status: ${response.status}`);
      }
      // If successful, return the response body as plain text.
      return response.text();
  })
  .then(csvText => {
    // Once the text is loaded, parse it.
    const lines = csvText.trim().split('\n').slice(1); // Split into lines and skip the header row.
    adLocations = lines.map(line => {
      const columns = line.split(',');
      // Assumption: Latitude is column 12 (index 11), Longitude is column 13 (index 12).
      const lat = parseFloat(columns[11]);
      const lon = parseFloat(columns[12]);
      // Only return an object if both latitude and longitude are valid numbers.
      if (!isNaN(lat) && !isNaN(lon)) {
        return { lat, lon };
      }
      return null;
    }).filter(Boolean); // Filter out any null entries from rows that failed to parse correctly.

    console.log(`Successfully parsed ${adLocations.length} AD facility locations.`);
    
    // CRITICAL STEP: Now that the prerequisite data (adLocations) is loaded and parsed,
    // we can safely initialize the rest of the application.
    initializeApp();
  })
  .catch(err => {
    // If the fetch or parsing fails at any point, this block will execute.
    console.error("CRITICAL ERROR: Could not initialize map.", err);
    // Display a prominent error to the user, as the app is non-functional without this data.
    showNotification("Could not load the AD facility list. The application cannot start.", true);
  });