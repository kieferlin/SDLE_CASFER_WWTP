// initialize map and set its view
const map = L.map('map', { zoomControl: false }).setView([39.8283, -98.5795], 4); // center on the US

// manually add zoom control and set position to top right
L.control.zoom({
  position: 'topright' 
}).addTo(map);

// add tile layer (the map images)
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  attribution: '© OpenStreetMap contributors'
}).addTo(map);

// global variable to hold the current layer of markers
let currentLayer = null;
// global set to hold the NPDES IDs of anaerobic digestion facilities
let anaerobicDigestionFacilities = new Set();

/**
 * processes the site data, filters it, and displays markers on the map
 * @param {Array} sitesData - array of site objects
 * @param {string} pollutantName - name of the pollutant to filter by
 * @param {string} adFilterValue - value from the AD filter ('all', 'only_ad', 'exclude_ad')
 * @param {HTMLElement} countElement - span element to update with the count
 */
function processAndDisplayData(sitesData, pollutantName, adFilterValue, countElement) {
  console.log(`Processing ${sitesData.length} total records.`);
  console.log("Filtering by nutrient name:", pollutantName);
  console.log("Filtering by AD status:", adFilterValue);

  // filter by pollutant
  let filteredSites = sitesData.filter(site => site.pollutant === pollutantName);
  console.log("Sites after nutrient filter:", filteredSites.length);

  // filter by anaerobic digestion status
  if (adFilterValue === 'only_ad') {
    filteredSites = filteredSites.filter(site => anaerobicDigestionFacilities.has(site.npdes));
    console.log("Sites after 'Only AD' filter:", filteredSites.length);
  } else if (adFilterValue === 'exclude_ad') {
    filteredSites = filteredSites.filter(site => !anaerobicDigestionFacilities.has(site.npdes));
    console.log("Sites after 'Exclude AD' filter:", filteredSites.length);
  }

  if (countElement) {
    countElement.textContent = filteredSites.length;
  }
  
  if (currentLayer) {
    map.removeLayer(currentLayer);
    currentLayer = null;
    console.log("Removed existing map layer");
  }

  if (filteredSites.length === 0) {
    console.warn("No sites matched the final filters");
    return;
  }

  const layerGroup = L.layerGroup();
  filteredSites.forEach(site => {
    if (site.lat && site.lon) {
      const select = document.createElement("select");
      site.measurements.forEach(m => {
        const option = document.createElement("option");
        option.text = `${m.date}: ${m.value} ${site.unit}`;
        select.appendChild(option);
      });
      const popup = document.createElement("div");
      popup.innerHTML = `<strong>${site.npdes}</strong><br>Nutrient: ${site.pollutant}<br>`;
      popup.appendChild(select);
      L.marker([site.lat, site.lon]).bindPopup(popup).addTo(layerGroup);
    } else {
      console.warn("Skipping site with missing lat/lon:", site);
    }
  });

  layerGroup.addTo(map);
  currentLayer = layerGroup;
  console.log("Added new markers to map");
}

// event listener ensures the code runs only after the HTML is fully loaded
document.addEventListener("DOMContentLoaded", () => {
  // get references to all the UI elements
  const pollutantSelect = document.getElementById("pollutantFilter");
  const stateSelect = document.getElementById("stateFilter");
  const yearSelect = document.getElementById("yearFilter");
  const adSelect = document.getElementById("adFilter"); // NEW
  const siteCountValue = document.getElementById('site-count-value');
  const loadingIndicator = document.getElementById('loading-indicator');
  const progressBar = document.getElementById('progress-bar');
  const progressText = document.getElementById('progress-text');

  // fetch and parse the anaerobic digestion facilities CSV
  fetch('./2_wrrf_fuzzy_matching.csv')
    .then(response => {
      if (!response.ok) {
        throw new Error('Network response was not ok: 2_wrrf_fuzzy_matching.csv not found.');
      }
      return response.text();
    })
    .then(csvText => {
      const lines = csvText.trim().split('\n');
      // skip header row by starting slice at 1
      const ids = lines.slice(1).map(line => {
        const columns = line.split(',');
        // assumes NPDES ID is in the first column. Use trim to remove whitespace.
        return columns[0] ? columns[0].trim() : null;
      }).filter(Boolean); // filter out any null/empty values
      
      anaerobicDigestionFacilities = new Set(ids);
      console.log(`Loaded ${anaerobicDigestionFacilities.size} anaerobic digestion facility IDs.`);
    })
    .catch(error => {
      console.error('Error loading or parsing AD facilities CSV:', error);
      alert('Could not load the anaerobic digestion facility list. Filtering by this criteria will not work.');
    });

  // populate dropdowns
  pollutantData.forEach(pollutant => {
    const option = document.createElement("option");
    option.value = pollutant.displayName;
    option.textContent = pollutant.displayName;
    pollutantSelect.appendChild(option);
  });

  availableYears.forEach(year => {
    const option = document.createElement("option");
    option.value = year;
    option.textContent = year;
    yearSelect.appendChild(option);
  });
  
  const allStatesOption = document.createElement("option");
  allStatesOption.value = "ALL";
  allStatesOption.textContent = "-- All States --";
  stateSelect.appendChild(allStatesOption);
  
  stateCodes.forEach(state => {
    const option = document.createElement("option");
    option.value = state;
    option.textContent = state;
    stateSelect.appendChild(option);
  });

  // main function to fetch data and update the map based on filters
  function updateMap() {
    const pollutantName = pollutantSelect.value;
    const state = stateSelect.value;
    const year = yearSelect.value;
    const adFilterValue = adSelect.value;

    console.log("Filters selected:", { pollutant: pollutantName, state, year, adFilter: adFilterValue });

    if (!pollutantName || !state || !year) {
      if (currentLayer) map.removeLayer(currentLayer);
      currentLayer = null;
      if (siteCountValue) siteCountValue.textContent = '0';
      return;
    }

    loadingIndicator.classList.remove('hidden');

    if (state === 'ALL') {
      let completedRequests = 0;
      const totalRequests = stateCodes.length;
      progressBar.style.width = '0%';
      progressText.textContent = `Fetching data... (0/${totalRequests})`;

      const fetchPromises = stateCodes.map(stateCode => {
        const filePath = `./1_facility_csv_to_metadata_json/${year}/${stateCode}_${year}.json`;
        return fetch(filePath)
          .then(res => res.ok ? res.json() : null)
          .finally(() => {
            completedRequests++;
            const percentage = Math.round((completedRequests / totalRequests) * 100);
            progressBar.style.width = `${percentage}%`;
            progressText.textContent = `Fetching data... (${completedRequests}/${totalRequests})`;
          });
      });

      Promise.all(fetchPromises)
        .then(results => {
          progressText.textContent = 'Processing data...';
          const allSitesData = results.filter(Boolean).flat();
          // pass the new filter value to the processing function
          processAndDisplayData(allSitesData, pollutantName, adFilterValue, siteCountValue);
        })
        .catch(err => {
            console.error("A critical error occurred:", err);
            alert("An error occurred. Check console.");
        })
        .finally(() => {
            loadingIndicator.classList.add('hidden');
        });

    } else {
      progressText.textContent = 'Loading...';
      progressBar.style.width = '50%';

      const filePath = `./1_facility_csv_to_metadata_json/${year}/${state}_${year}.json`;
      fetch(filePath)
        .then(res => {
          if (!res.ok) throw new Error(`File not found: ${filePath}`);
          return res.json();
        })
        .then(data => {
          // pass the new filter value to the processing function
          processAndDisplayData(data, pollutantName, adFilterValue, siteCountValue);
        })
        .catch(err => {
          console.error(err);
          if (currentLayer) map.removeLayer(currentLayer);
          currentLayer = null;
          if (siteCountValue) siteCountValue.textContent = '0';
          alert(`Could not load data for ${state} ${year}.`);
        })
        .finally(() => {
          loadingIndicator.classList.add('hidden');
        });
    }
  }

  // add event listeners
  pollutantSelect.addEventListener("change", updateMap);
  stateSelect.addEventListener("change", updateMap);
  yearSelect.addEventListener("change", updateMap);
  adSelect.addEventListener("change", updateMap);
});