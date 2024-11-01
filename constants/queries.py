CREATE_ASTEROIDS_OBSERVATIONS_TABLE = """\
CREATE TABLE IF NOT EXISTS asteroids_details (\
asteroid_id INTEGER NOT NULL,\
neo_reference_id INTEGER NOT NULL,\
absolute_magnitude DOUBLE NOT NULL,\
estimated_diameter_km_max DOUBLE NOT NULL,\
estimated_diameter_km_min DOUBLE NOT NULL,\
isHazardous BOOL NOT NULL,\
close_approach_date DATETIME NOT NULL,\
miss_distance_km DOUBLE NOT NULL,\
uploaded_date DATETIME NOT NULL\
)
"""

TRUNCATE_ASTEROIDS_OBSERVATIONS_TABLE = """
TRUNCATE TABLE asteroids_details
"""