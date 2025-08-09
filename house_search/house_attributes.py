from datetime import datetime, date, time, timedelta
import json
from math import floor
import googlemaps
from dotenv import load_dotenv
import os
import re
import sys

OUTPUT_DIR = "locations"

def main():
    """Prints elevation and travel times for an address or location 
    as specified on the command line
    Required environment variables (can be in .env file):
    * GOOGLE_API_KEY
    * DESTINATION_JSON

    DESTINATION_JSON should look like
    {
        "friendly name": "123 Main Street, Mytown, AZ",
        "friendly name": "Clover Creek Trailhead, Somewhere, CO",
        ...
    }
    """

    try:
        load_dotenv()
        api_key = os.getenv("GOOGLE_API_KEY")
        gmaps = googlemaps.Client(key=api_key)

        dest_file = os.getenv("DESTINATIONS_JSON")

        with open(dest_file, 'r') as file:
            destinations = json.load(file)

    except Exception as e:
        print(f"Unable to read setup files: {e}")
        exit(1)


    #Read address from command line
    if len(sys.argv) > 1:
        address = sys.argv[1]
    else:
        print("Try again: Specify an address as the script's argument")
        exit(1) 

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    filename = os.path.join(OUTPUT_DIR, re.sub(r'\W+', '_', address) + '.txt')

    if os.path.exists(filename):
        print("Existing data:\n")
        with open(filename, "r") as file:
            for line in file:
                print(line.strip())

        response = input("\nDo you want to overwrite it? y/N\n")
        if response != "y":
            print("Exiting ...")
            exit(0)

    print(f"Collecting relevant data for {address} ...")

    results = {}

    try:
        address_coord = get_coordinates(gmaps, address)
    except Exception as e:
        print(f"Address could not be found: {e}")
        exit(1)

    try:
        elevation = floor(get_elevation_feet(gmaps, address_coord))
        print()
        print(f"Address Elevation: {elevation} feet")
        results["Elevation"] = elevation
    except Exception as e:
        print (f"No elevation found {e}")

    print()
    print("Estimated travel times, departing at midnight tonight:")
    try:    
        for dest in destinations:
            duration = get_travel_time(gmaps, start=address_coord, destination=destinations[dest])
            if duration:
                print(f"\t{dest}: {duration}")
                results[dest] = duration
            else:
                print(f"No route found to {destinations[dest]}")

    except Exception as e:
        print(f"Exception occured while calculating travel times: {e}")

    notes = input("Enter notes\n")

    with open(filename, "w") as file:
        file.write(f"{address}\n*** {notes} ***\n")
        for key in results:
            file.write(f"{key}: {results[key]}\n")


def get_coordinates(gmaps, address):
    """ Gets the GPS coordinates of a specified address / location as recognized by Google Maps
    gmaps -- instantiated Google client
    address -- location identifier. Could be an address, a place name, or a tuple of GPS coordinates
    """
    geocode_result = gmaps.geocode(address)

    if geocode_result:
        location = geocode_result[0]["geometry"]["location"]
    return (location['lat'], location['lng'])

def get_elevation_feet(gmaps, location):
    """ Gets the elevation of a location
    location -- tuple representing GPS coordinates
    """
    elevation_result = gmaps.elevation(location)

    if elevation_result:
        elevation_meters = elevation_result[0]["elevation"]
        elevation_feet = elevation_meters * 3.28084
    return elevation_feet


def get_travel_time(gmaps, start, destination):
    """ Gets travel time for a specified start and end location
    This function specifies a start time of midnight tonight with the assumption that
    midnight tonight will be the most predictable route time for purposes of comparison

    gmaps -- instantiated Google client
    start -- Could be an address, a place name, or a tuple of GPS coordinates
    destination -- Could be an address, a place name, or a tuple of GPS coordinates
    """

    midnight = datetime.combine(date.today() + timedelta(days=1), time.min)
    midnight_timestamp = int(midnight.timestamp())

    directions_result = gmaps.directions(start, destination, mode="driving", departure_time=midnight_timestamp)
    if directions_result:
        duration = directions_result[0]["legs"][0]["duration"]["text"]
        return duration



if __name__ == "__main__":
    main()
