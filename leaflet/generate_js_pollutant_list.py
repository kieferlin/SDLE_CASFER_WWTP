import os

input_root = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/pollutant_filtered_by_year"
output_file = "/home/kyl29/CSE_MSE_RXF131/staging/casf/kyl29/leaflet/pollutant_list.js"

years = []
pollutants = set()

for year in sorted(os.listdir(input_root)):
    year_dir = os.path.join(input_root, year)
    if not os.path.isdir(year_dir):
        continue
    years.append(year)
    for filename in os.listdir(year_dir):
        if filename.endswith(".json"):
            pollutants.add(filename)

with open(output_file, "w") as f:
    f.write("const pollutantFiles = [\n")
    for p in sorted(pollutants):
        f.write(f'  "{p}",\n')
    f.write("];\n\n")

    f.write("const availableYears = [\n")
    for y in years:
        f.write(f'  "{y}",\n')
    f.write("];\n")

print(f"Wrote {len(pollutants)} pollutants and {len(years)} years to {output_file}")
