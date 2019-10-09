import json
import os
import time

def parse_data(new_data, category):

	files_path = PATH + category
	files = os.listdir(files_path)

	for file in files:
		file_path = files_path + '/' + file
		loaded_json = read_json(file_path)

		parsed_json = parse_json(loaded_json, category)
		new_data["data"] += parsed_json


def save_json(new_data):

	output = PATH + "yelp-restaurants.json"
	with open(output, 'w') as f:
		json.dump(new_data, f)

def save_bulk(bulk_data):

	output = PATH + "bulk_restaurants.json"
	with open(output, 'w') as f:
		for line in bulk_data:
			f.write(json.dumps(line) + "\n")

def read_json(file_path):

	with open(file_path, 'r') as f:
		lines = f.readlines()
		loaded_json = json.loads(lines[0])

	return loaded_json

def parse_json(loaded_json, category):

	businesses = loaded_json['businesses']
	requests = []
	global bulk_id
	global bulk_data
	global parsed_id

	for business in businesses:

		if "id" in business:
			if business["id"] not in parsed_id:
				bulk_id += 1
				request = {}
				parsed_id.append(business["id"])

				request["businessID"] = business["id"]

				if "name" in business:
					request["name"] = business["name"]
				
				if "categories" in business:
					c_array = business["categories"]
					cuisine = []
					for c in c_array:
						alias = c["alias"]
						if alias == "newamerican":
							alias = "american"
						if alias == "indpak":
							alias = "indian"
						cuisine.append(alias)
					if category not in cuisine:
						cuisine.append(category)
					request["cuisine"] = cuisine

				if "location" in business:
					addrs = business["location"]["display_address"]
					location = ""
					for addr in addrs:
						if location != "":
							location += ", "
						location += addr
					request["location"] = location
					request["zip_code"] = business["location"]["zip_code"]
				
				if "coordinates" in business:
					latitude = business["coordinates"]["latitude"]
					longitude = business["coordinates"]["longitude"]
					request["coordinates"] = {"latitude": latitude, "longitude": longitude}

				if "display_phone" in business:
					request["phone"] = business["display_phone"]

				if "review_count" in business:
					request["review_count"] = business["review_count"]

				if "rating" in business:
					request["rating"] = business["rating"]

				firstline = { "index": { "_index": "restaurants", "_type": "Restaurants", "_id": str(bulk_id) } }
				bulk_data.append(firstline)
				secondline = { "businessID": request["businessID"], "cuisine": request["cuisine"] }
				bulk_data.append(secondline)

				requests.append(request)

	return requests

PATH = "C:/Users/Hongmin/Documents/Hongmin/Columbia/Courses/COMS 6998_008/Assignments/Assignment1/yelp_data/"
new_data = {"data": []}
bulk_id = 0
bulk_data = []
parsed_id = []

parse_data(new_data, "chinese")
parse_data(new_data, "indian")
parse_data(new_data, "italian")
parse_data(new_data, "mexican")
parse_data(new_data, "american")
parse_data(new_data, "korean")
parse_data(new_data, "thai")
parse_data(new_data, "japanese")
parse_data(new_data, "vegetarian")

save_json(new_data)
save_bulk(bulk_data)
