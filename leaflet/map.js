// create leaflet map
const map = L.map('map').setView([32.8067, -86.7911], 7);

// add base OpenStreetMap layer
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  attribution: '© OpenStreetMap contributors'
}).addTo(map);

// global reference to the currently displayed layer so we can remove it on update
let currentLayer = null;

// wait for the DOM to fully load before attaching event listeners
document.addEventListener("DOMContentLoaded", () => {
  const pollutantSelect = document.getElementById("pollutantFilter");
  const yearSelect = document.getElementById("yearFilter");

  // populate pollutant dropdown with file names from pollutantFiles.js
  pollutantFiles.forEach(file => {
    const label = file.replace(".json", "").replace(/_/g, " ");  // make label user-friendly
    const option = document.createElement("option");
    option.value = file;
    option.textContent = label;
    pollutantSelect.appendChild(option);
  });

  // populate year dropdown from availableYears array
  availableYears.forEach(year => {
    const option = document.createElement("option");
    option.value = year;
    option.textContent = year;
    yearSelect.appendChild(option);
  });

  // called whenever either dropdown value changes
  function updateMap() {
    const pollutant = pollutantSelect.value;
    const year = yearSelect.value;

    // make sure both filters are selected
    if (!pollutant || !year) return;

    // remove old pins layer if it exists
    if (currentLayer) {
      map.removeLayer(currentLayer);
      currentLayer = null;
    }

    // fetch data for selected pollutant and year
    fetch(`pollutant_filtered_by_year/${year}/${pollutant}`)
      .then(res => res.json())
      .then(data => {
        const layerGroup = L.layerGroup();

        data.forEach(site => {
          // skip sites missing lat/lon
          if (site.lat && site.lon) {
            // create dropdown for measurements at this location
            const select = document.createElement("select");
            site.measurements.forEach(m => {
              const option = document.createElement("option");
              option.text = `${m.date}: ${m.value} ${site.unit}`;
              select.appendChild(option);
            });

            // create popup content with facility info and measurement dropdown
            const popup = document.createElement("div");
            popup.innerHTML = `<strong>${site.npdes}</strong><br>Pollutant: ${site.pollutant}<br>`;
            popup.appendChild(select);

            // place marker on map and bind popup
            L.marker([site.lat, site.lon]).bindPopup(popup).addTo(layerGroup);
          }
        });

        // add the new marker group to the map
        layerGroup.addTo(map);
        currentLayer = layerGroup;
      });
  }

  // update map when user selects a new pollutant or year
  pollutantSelect.addEventListener("change", updateMap);
  yearSelect.addEventListener("change", updateMap);
});
