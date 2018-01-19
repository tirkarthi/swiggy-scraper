import glob
import os
import json
from pprint import pprint
import sqlite3

db_file_name = 'data'
create_hotel_table = 'create table hotels(id integer primary key, hotel_id integer, name text, avg_rating float, area text, total_ratings integer, minimum_order integer, city text)';
create_items_table = 'create table items(id integer primary key, hotel_id integer, category text, name text, is_popular boolean, recommended boolean, is_veg boolean, price integer, FOREIGN KEY (hotel_id) REFERENCES hotels(id));'
create_tags_table = 'create table tags(id integer primary key, hotel_id integer, name text, FOREIGN KEY (hotel_id) REFERENCES hotels(id))'
insert_hotel = 'insert into hotels({}) values({})'
insert_item = 'insert into items({}) values({})'
insert_tags = 'insert into tags(hotel_id, name) values(?, ?)'

os.remove(db_file_name)
conn = sqlite3.connect(db_file_name)
cursor = conn.cursor()
cursor.execute(create_hotel_table)
cursor.execute(create_items_table)
cursor.execute(create_tags_table)

files = glob.glob('*json')

for file in files:
    with open(file) as  f:
        key_mapping = {'avgRating': 'avg_rating', 'totalRatings': 'total_ratings',
                       'minimumOrder': 'minimum_order', 'isPopular': 'is_popular', 'isVeg': 'is_veg'}
        item_keys = ['category', 'name', 'isPopular', 'recommended', 'isVeg', 'price']
        hotel_keys = ['city', 'avgRating', 'area', 'name', 'totalRatings', 'tags', 'minimumOrder']
        data = json.load(f)
        hotel = data.get("data", {})
        if hotel:
            items = hotel.get("menu", {}).get("items", {}).items()
            hotel_details = {key_mapping.get(key, key): hotel.get(key)
                             for key in hotel_keys if hotel.get(key) is not None}
            hotel_id = hotel['id']
            tags = hotel_details.pop('tags', [])

            prepare_stmt = insert_hotel.format(','.join(['hotel_id'] + list(hotel_details.keys())), ','.join(['?'] * (len(hotel_details.keys()) + 1)))
            cursor.execute(prepare_stmt, [hotel_id] + list(hotel_details.values()))

            for tag in tags:
                cursor.execute(insert_tags, [hotel_id, tag])

            for item_id, item in items:
                item_details = {key_mapping.get(key, key): item.get(key)
                               for key in item_keys if item.get(key) is not None}
                item_prepare_stmt = insert_item.format(','.join(['hotel_id'] + list(item_details.keys())), ','.join(['?'] * (len(item_details.keys()) + 1)))
                cursor.execute(item_prepare_stmt, [hotel_id] + list(item_details.values()))

cursor.close()
conn.commit()
conn.close()
