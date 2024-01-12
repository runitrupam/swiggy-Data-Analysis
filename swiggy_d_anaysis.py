'''

Problem Statement :-
How many cities (including subregions) where Swiggy is having its restaurants listed?
How many cities  (don't include subregions) where Swiggy is having their restaurants listed?
3. The Subregion of Delhi with the maximum number of restaurants listed on Swiggy?
4. Name the top 5 Most Expensive Cities in the Datasets.
5. List out the top 5 Restaurants with Maximum & minimum ratings throughout the dataset.
6. Name of top 5 cities with the highest number of restaurants listed.
Top 10 cities as per the number of restaurants listed?
Name the top 5 Most Popular Restaurants in Pune.
Which SubRegion in Delhi is having the least expensive restaurant in terms of cost?
Top 5 most popular restaurant chains in India?
Which restaurant in Pune has the most number of people visiting?
Top 10 Restaurants with Maximum Ratings in Banglore
Top 10 Restaurant in Patna w.r.t rating 


'''

import zipfile
import json
import time
import ijson
import pandas as pd
import numpy as np
from utils import *

# Assuming your ZIP file is named 'swiggy.zip' and contains a single JSON file 'data.json'
# Adjust the file paths accordingly if your files are in different locations
zip_file_path = 'swiggy.zip'
json_file_name = 'data.json'


# Function to read and parse JSON data from a zipped file
def read_zipped_json(zip_file_path, json_file_name):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        with zip_ref.open(json_file_name) as json_file:
            return json.load(json_file)


# Function to fetch data stream wise using ijson
def fetch_data(zip_file_path, json_file_name):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        with zip_ref.open(json_file_name) as json_file:
            count = 0
            data = {}
            parser = ijson.parse(json_file)
            for prefix, event, value in parser:

                if '.' in prefix and event == 'string':
                    pref_list = prefix.split('.')
                    if 'menu' in pref_list or 'link' in pref_list:
                        continue
                    # last = {pref_list[-1] :value}
                    curr = data
                    for p in pref_list:
                        try:
                            if p == pref_list[-1]:
                                curr[p] = value
                                break
                            if p not in curr:
                                curr[p] = {}
                            else:
                                if type(curr[p]) != dict:
                                    curr[p] = {p: curr[p]}
                            curr = curr[p]
                        except Exception as e:
                            print('exce', e, pref_list, prefix)
                            pass

                # if count > 50000000:
                #     break
                count += 1

    return data


def get_df_of_restaurant_for_a_region(data, city=None, region=None):
    if 'restaurants' in data:
        curr_city_list = []
        for rest_id in data['restaurants']:
            dict_rest = data['restaurants'][rest_id]
            if 'menu' in dict_rest:
                del dict_rest['menu']
            df_rest = pd.DataFrame(dict_rest, index = [0])
            df_rest['rest_id'] = rest_id
            curr_city_list.append(df_rest)
        try:
            curr_city_df = pd.concat(curr_city_list)
        except Exception as e:
            print('Error = ', e, 'city = ', city, 'region = ', region, curr_city_list)
            # Handle empty city case by appending an empty DataFrame
            curr_city_df = pd.DataFrame({1: 1}, index = [0])
        return curr_city_df
    else:
        return pd.DataFrame({1: 1}, index = [0])


def get_final_df_city_wise(data):
    start_time = time.time()
    res_df = []
    for city in data:
        if 'restaurants' in data[city]:
            curr_city_df = get_df_of_restaurant_for_a_region(data[city], city = city)
            curr_city_df['city'] = city
        else:
            curr_city_list = []
            for sub_region in data[city]:
                if sub_region == 'link':
                    continue
                curr_sub_region_df = get_df_of_restaurant_for_a_region(data[city][sub_region], city = city,
                                                                       region = sub_region)
                curr_sub_region_df['sub_region'] = sub_region
                curr_city_list.append(curr_sub_region_df)
            try:
                curr_city_df = pd.concat(curr_city_list)
            except Exception as e:
                print('Error = ', e, city, curr_city_list)
                # Handle empty city case by appending an empty DataFrame
                curr_city_df = pd.DataFrame({1: 1}, index = [0])
            curr_city_df['city'] = city
        res_df.append(curr_city_df)

    final_df = pd.concat(res_df)
    end_time = time.time()
    time_taken = end_time - start_time
    print("Time taken in get_final_df_city_wise: {:.2f} seconds".format(time_taken))

    return final_df


# Measure the time taken to fetch data stream wise
start_time = time.time()
# data2 = fetch_data(zip_file_path, json_file_name) # 355.76 seconds
end_time = time.time()
time_taken = end_time - start_time
print("Time taken: {:.2f} seconds".format(time_taken))

# Measure the time taken to fetch data outer key-wise
start_time = time.time()
data = read_zipped_json(zip_file_path, json_file_name)  # 90 sec
end_time = time.time()
time_taken = end_time - start_time
print("Time taken: {:.2f} seconds".format(time_taken))

# s = get_df_of_restaurant_for_a_region(data['Ambala'],city = 'Ambala')

final_df = get_final_df_city_wise(data)


# 1
city_include_subregion = get_cities_count_1_2(data, include_subregion = 1)

# 2
city_no_subregion = get_cities_count_1_2(data, include_subregion = 0)

# 3. The Subregion of Delhi with the maximum number of restaurants listed on Swiggy?
subregion_of_delhi_with_max_rest = get_subregion_of_city_with_max_restaurants_3(data, 'Delhi')

# 4. Name the top 5 Most Expensive Cities in the Datasets.
five_expensive_city = get_5_most_expensive_city(data)


# 5. List out the top 5 Restaurants with Maximum & minimum ratings throughout the dataset.


'''
5. List out the top 5 Restaurants with Maximum & minimum ratings throughout the dataset.
6. Name of top 5 cities with the highest number of restaurants listed.
Top 10 cities as per the number of restaurants listed?
'''




'''


res_df = []
li = []
for city in data: 
    if 'restaurants' in data[city]:
        # link = data[city]['link']
        curr_city_list = []
        for rest_id in data[city]['restaurants']:
            dict_rest = data[city]['restaurants'][rest_id]
            # if 'menu' in dict_rest:
            #     del dict_rest['menu']
            df_rest = pd.DataFrame(dict_rest , index= [0])
            df_rest['rest_id'] = rest_id
            df_rest['city'] = city
            # df_rest['link'] = [link for x in range(df_rest.shape[0])]
            # df_rest = pd.DataFrame(dict_rest , index= [x for x in range(len(dict_rest)) ])
            curr_city_list.append(df_rest)
        try:
            curr_city_df = pd.concat(curr_city_list)
        except Exception as e:
            print('Error = ',e,city,curr_city_list)
            # Handle empty city case by appending an empty DataFrame
            curr_city_df = pd.DataFrame({'city': city}, index = [0])
    else:
        continue
    res_df.append(curr_city_df)

'''
