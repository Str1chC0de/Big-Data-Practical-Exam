import requests
import json
import os

from datetime import datetime

def collect_Data():
    #Define Index for Comic Count
    i = 1

    #Create API URL and request Data from xkcd.com - Initial request to get into the while loop
    request_target = "https://xkcd.com/"+ str(i) +"/info.0.json"
    request_result = requests.get(request_target)

    while request_result.status_code == 200:

        #File 404 doesn`t exist so skip index 404`
        if i == 404:
            i += 1

        #Create API URL and request Data from xkcd.com - Normal while loop data request
        request_target = "https://xkcd.com/"+ str(i) +"/info.0.json"
        request_result = requests.get(request_target)

        #Profe response code if 200 its a succcess
        if request_result.status_code == 200:

            #Result in raw json format
            json_raw = request_result.json()

            #Result in json as onjects
            json_object = json.loads(request_result.content)

            #temp variable for the year of comic --> to create filename
            comic_year = json_object["year"]

            #create file name and define the directory
            file_name = str(comic_year)+ "_" + str(i) + '.json'
            directory_name = os.path.join("/home/airflow/airflow/xkcd")

            #combine filename and path 
            total_path = os.path.join(directory_name, file_name)

            #Dump the json into file
            with open(total_path, 'w') as outputfile:
                json.dump(json_raw, outputfile)

            #increment the index of the comic
            i += 1

        #print error message
        else:
            print("Error: status code is not 200 \n" + "Response status code: " + str(request_result.status_code))

if __name__ == "__main__":
    collect_Data()