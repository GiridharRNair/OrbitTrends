import pandas as pd
import folium

# Load CSV
df = pd.read_csv("parking_locations.csv")

# Convert count column to integer
df["count ( location )"] = df["count ( location )"].astype(int)

# Location → Coordinates mapping
location_coords = {

"APTS/PHASE 3": (32.98780323880558, -96.75308337920461),
"LOT B": (32.98925080261045, -96.74487763203432),
"LOT F - VCB": (32.98602918546609, -96.74728694232807),
"LOT P": (32.99097540985813, -96.75024867533702),
"LOT I": (32.986646380603425, -96.75304783931331),
"APTS/PHASE 7": (32.9875818228362, -96.755599266077),
"APTS/PHASE 2": (32.98266011482911, -96.75558749699567),
"LOT D": (32.98711595286818, -96.74420450241712),
"LOT Q": (32.985424073402854, -96.74560997992751),
"Canyon Creek Heights South": (32.98105090797914, -96.75562205610873),
"APTS/PHASE 1": (32.98460530608658, -96.75466364564576),
"CANYON CREEEK HEIGHTS LOADING ZN": (32.98161840320695, -96.75522410089947),
"APTS/PHASE 8A": (32.987666789531424, -96.75306647781478),
"VEGA HALL LOT": (32.99135678667155, -96.75517112210972),

"LOT M - EAST": (32.98388964804842, -96.74623651998058),
"LOT C": (32.988304383853276, -96.74401037857074),
"PARKING STRUCTURE 1": (32.986131061836495, -96.74541940390091),
"LOT H": (32.98765342829231, -96.75307229807584),
"PARKING STRUCTURE 4": (32.98614617547292, -96.75359473273586),
"APTS/PHASE 9": (32.98885572774756, -96.75203528205611),
"PARKING STRUCTURE 3": (32.99071393903641, -96.75005694254071),
"LOT M SOUTHEAST 1 (GLD)": (32.983651220620416, -96.746213715551),
"LOT M- SOUTH": (32.98244126782916, -96.74693703273601),
"LOT J": (32.98440724164527, -96.75059715447689),
"LOT A": (32.99056885186209, -96.74536147639652),
"SPN LOT": (32.9940180924705, -96.7516818223598),
"LOT N": (32.99388761312533, -96.75174083095229),
"LOT T - CAMPUS": (32.99253781545319, -96.75240601880388)

}

# Map coordinates to dataframe
df["lat"] = df["location"].map(lambda x: location_coords.get(x, (None, None))[0])
df["lon"] = df["location"].map(lambda x: location_coords.get(x, (None, None))[1])

# Remove rows with missing coordinates
df = df.dropna()

# Create base map centered at UTD
center_lat = 32.9858
center_lon = -96.7501

m = folium.Map(location=[center_lat, center_lon], zoom_start=15)

# Scale radius based on count (min 20, max 80)
max_count = df["count ( location )"].max()
min_radius = 20
max_radius = 80

for _, row in df.iterrows():
    lat = row["lat"]
    lon = row["lon"]
    count = row["count ( location )"]
    location_name = row["location"]
    
    # Scale radius proportionally to count
    radius = min_radius + (count / max_count) * (max_radius - min_radius)
    
    folium.CircleMarker(
        location=[lat, lon],
        radius=radius,
        color="#3388ff",
        fill=True,
        fill_color="#3388ff",
        fill_opacity=0.5,
        popup=f"{location_name}: {count} tickets"
    ).add_to(m)

# Save map
m.save("utd_parking_ticket_heatmap.html")

print("Map saved as utd_parking_ticket_heatmap.html")