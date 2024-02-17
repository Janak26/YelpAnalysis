import psycopg2
from shapely.geometry import Point
import pandas as pd
import psycopg2.extras


conn_details = psycopg2.connect(
	host="localhost",
	database="YELP",
	user="postgres",
	password="JVKMarg",
	port= "5432"
)

cursor = conn_details.cursor()


### Start Inserting Business
# sql_query = '''CREATE TABLE business
# 		(
# 			business_id VARCHAR(30) NOT NULL PRIMARY KEY,
# 			name VARCHAR(100) NOT NULL,
# 			address VARCHAR(120),
# 			city VARCHAR(50),
# 			state VARCHAR(10),
# 			postal_code VARCHAR(10),
# 			geolocation POINT,
# 			rating NUMERIC(3,1),
# 			review_count INTEGER,
# 			is_open BOOLEAN,
# 			categories VARCHAR(600)
# 	)


# def insertBusiness():
# 	df = pd.read_csv(r"D:\Projects\YELP\data\businessOnly.csv")
# 	allDocs = []
# 	for index, row in df.iterrows():
		
# 		business_id = row['business_id']
# 		name = row['name']
# 		address = row['address']
# 		city = row['city']
# 		state = row['state']
# 		zipCode = row['postal_code']
# 		lati = row['latitude']
# 		longi = row['longitude']
# 		stars = row['stars']
# 		reviewC = row['review_count']
# 		is_open = row['is_open']
# 		if is_open == 0:
# 			is_open = False
# 		else:
# 			is_open = True
# 		categos = row['categories']
# 		docSingle = (business_id, name, address, city, state, zipCode, longi, lati, stars, reviewC, is_open, categos)

# 		sql_query = '''INSERT INTO business VALUES(%s, %s, %s, %s, %s, %s, Point(%s, %s), %s, %s, %s, %s)'''
# 		cursor.execute(sql_query, docSingle)

# 		if index%10000 == 0:
# 			print(index)
# 			conn_details.commit()
# 	conn_details.commit()
# 	cursor.close()
# 	conn_details.close()
## End inserting business


### Start of inserting user details
# sql_query = '''CREATE TABLE user_details
# 	(
# 		user_id VARCHAR(30) NOT NULL PRIMARY KEY,
# 		user_name VARCHAR(100) NOT NULL,
# 		review_count INTEGER,
# 		yelping_since TIMESTAMP,
# 		useful INTEGER,
# 		funny INTEGER,
# 		cool INTEGER,
# 		fans INTEGER,
# 		average_stars NUMERIC(4,2),
# 		compliment_hot INTEGER,
# 		compliment_more INTEGER,  
# 		compliment_profile INTEGER,  
# 		compliment_cute INTEGER,
# 		compliment_list INTEGER,  
# 		compliment_note INTEGER,  
# 		compliment_plain INTEGER,  
# 		compliment_cool INTEGER,  
# 		compliment_funny INTEGER,  
# 		compliment_writer INTEGER,  
# 		compliment_photos INTEGER
# 	)'''


# def insertUser():
# 	df = pd.read_csv(r"D:\Projects\YELP\data\user.csv")
# 	allDocs = []
# 	for index, row in df.iterrows():
# 		user_id = row['user_id'],
# 		user_name = row['name'],
# 		review_count = row['review_count'],
# 		yelping_since = row['yelping_since'],
# 		useful = row['useful'],
# 		funny = row['funny'],
# 		cool = row['cool'],
# 		fans = row['fans'],
# 		average_stars = row['average_stars'],
# 		compliment_hot = row['compliment_hot'], 
# 		compliment_more = row['compliment_more'], 
# 		compliment_profile = row['compliment_profile'],
# 		compliment_cute = row['compliment_cute'],
# 		compliment_list = row['compliment_list'],
# 		compliment_note = row['compliment_note'],
# 		compliment_plain = row['compliment_plain'],
# 		compliment_cool = row['compliment_cool'],
# 		compliment_funny = row['compliment_funny'],
# 		compliment_writer = row['compliment_writer'],
# 		compliment_photos = row['compliment_photos']
		
# 		docSingle = (user_id, user_name, review_count, yelping_since, useful, funny, cool, fans, average_stars,
# 			compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note,
# 			compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos)

# 		sql_query = '''INSERT INTO user_details VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
# 		cursor.execute(sql_query, docSingle)

# 		if index%10000 == 0:
# 			print(index)
# 			conn_details.commit()
# 	conn_details.commit()
# 	cursor.close()
# 	conn_details.close()
### end of inserting user details




### start of inserting business hours
# sql_query = '''CREATE TABLE business_hours
# 	(
# 		business_id VARCHAR(30) NOT NULL PRIMARY KEY,
# 		monday_open TIME,
# 		monday_close TIME,
# 		tuesday_open TIME,
# 		tuesday_close TIME,
# 		wednesday_open TIME,
# 		wednesday_close TIME,
# 		thursday_open TIME,
# 		thursday_close TIME,
# 		friday_open TIME,
# 		friday_close TIME,
# 		saturday_open TIME,
# 		saturday_close TIME,
# 		sunday_open TIME,
# 		sunday_close TIME
# 	)'''



# def insertBusinessHours():
# 	df = pd.read_csv(r"D:\Projects\YELP\data\businessHours.csv")
# 	allDocs = []
# 	for index, row in df.iterrows():
# 		business_id = row['business_id']
	
# 		monday_open = row['Monday_Open']
# 		if pd.isnull(monday_open):
# 			monday_open = None
# 		monday_close = row['Monday_Close']
# 		if pd.isnull(monday_close):
# 			monday_close = None
		
# 		tuesday_open = row['Tuesday_Open']
# 		if pd.isnull(tuesday_open):
# 			tuesday_open = None
# 		tuesday_close = row['Tuesday_Close']
# 		if pd.isnull(tuesday_close):
# 			tuesday_close = None

# 		wednesday_open = row['Wednesday_Open']
# 		if pd.isnull(wednesday_open):
# 			wednesday_open = None
# 		wednesday_close = row['Wednesday_Close']
# 		if pd.isnull(wednesday_close):
# 			wednesday_close = None

# 		thursday_open = row['Thursday_Open']
# 		if pd.isnull(thursday_open):
# 			thursday_open = None
# 		thursday_close = row['Thursday_Close']
# 		if pd.isnull(thursday_close):
# 			thursday_close = None
		
# 		friday_open = row['Friday_Open']
# 		if pd.isnull(friday_open):
# 			friday_open = None
# 		friday_close = row['Friday_Close']
# 		if pd.isnull(friday_close):
# 			friday_close = None
		
# 		saturday_open = row['Saturday_Open']
# 		if pd.isnull(saturday_open):
# 			saturday_open = None
# 		saturday_close = row['Saturday_Close']
# 		if pd.isnull(saturday_close):
# 			saturday_close = None

# 		sunday_open = row['Sunday_Open']
# 		if pd.isnull(sunday_open):
# 			sunday_open = None
# 		sunday_close = row['Sunday_Close']
# 		if pd.isnull(sunday_close):
# 			sunday_close = None

# 		docSingle = (business_id, monday_open, monday_close, tuesday_open, tuesday_close, wednesday_open, wednesday_close,
# 			thursday_open, thursday_close, friday_open, friday_close, saturday_open, saturday_close, sunday_open, sunday_close)

# 		sql_query = '''INSERT INTO business_hours VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
# 		cursor.execute(sql_query, docSingle)

# 		if index%10000 == 0:
# 			print(index)
# 			conn_details.commit()
# 	conn_details.commit()
# 	cursor.close()
# 	conn_details.close()
## end of inserting business hours




# sql_query = '''CREATE TABLE business_attributes
# 	(
# 		business_id VARCHAR(30) NOT NULL PRIMARY KEY,
# 		ByAppointmentOnly BOOLEAN,
# 		BusinessAcceptsCreditCards BOOLEAN,
# 		BikeParking BOOLEAN,
# 		RestaurantsPriceRange2 INTEGER,
# 		CoatCheck BOOLEAN,
# 		RestaurantsTakeOut BOOLEAN,
# 		RestaurantsDelivery BOOLEAN,
# 		Caters BOOLEAN,
# 		WiFi VARCHAR(10),
# 		BusinessParking_garage BOOLEAN,
# 		BusinessParking_street BOOLEAN,
# 		BusinessParking_validated BOOLEAN,
# 		BusinessParking_lot BOOLEAN,
# 		BusinessParking_valet BOOLEAN,
# 		WheelchairAccessible BOOLEAN,
# 		HappyHour BOOLEAN,
# 		OutdoorSeating BOOLEAN,
# 		HasTV BOOLEAN,
# 		RestaurantsReservations BOOLEAN,
# 		DogsAllowed BOOLEAN,
# 		Alcohol VARCHAR(20),
# 		GoodForKids BOOLEAN,
# 		RestaurantsAttire VARCHAR(10),
# 		RestaurantsTableService BOOLEAN,
# 		RestaurantsGoodForGroups BOOLEAN,
# 		DriveThru BOOLEAN,
# 		NoiseLevel VARCHAR(10),
# 		Ambience_romantic BOOLEAN,
# 		Ambience_intimate BOOLEAN,
# 		Ambience_touristy BOOLEAN,
# 		Ambience_hipster BOOLEAN,
# 		Ambience_divey BOOLEAN,
# 		Ambience_classy BOOLEAN,
# 		Ambience_trendy BOOLEAN,
# 		Ambience_upscale BOOLEAN,
# 		Ambience_casual BOOLEAN,
# 		GoodForMeal_dessert BOOLEAN,
# 		GoodForMeal_latenight BOOLEAN,
# 		GoodForMeal_lunch BOOLEAN,
# 		GoodForMeal_dinner BOOLEAN,
# 		GoodForMeal_brunch BOOLEAN,
# 		GoodForMeal_breakfast BOOLEAN,
# 		BusinessAcceptsBitcoin BOOLEAN,
# 		Smoking VARCHAR(10),
# 		Music_dj BOOLEAN, 
# 		Music_background_music BOOLEAN,
# 		Music_no_music BOOLEAN,
# 		Music_jukebox BOOLEAN,
# 		Music_live BOOLEAN,
# 		Music_video BOOLEAN,
# 		Music_karaoke BOOLEAN,
# 		GoodForDancing BOOLEAN,
# 		AcceptsInsurance BOOLEAN,
# 		BestNights_monday BOOLEAN,
# 		BestNights_tuesday BOOLEAN,
# 		BestNights_friday BOOLEAN,
# 		BestNights_wednesday BOOLEAN,
# 		BestNights_thursday BOOLEAN,
# 		BestNights_sunday BOOLEAN,
# 		BestNights_saturday BOOLEAN,
# 		BYOB BOOLEAN,
# 		Corkage BOOLEAN
# 	)'''


def fillNull(varName, varValue):
	if varName in ['Smoking', 'NoiseLevel', 'RestaurantsAttire', 'Alcohol', 'WiFi']:
		if pd.isnull(varValue) or varValue == "None":
			return None

	if pd.isnull(varValue) or varValue == "None":
		return None

	if type(varValue) == str:
		if '_' in varValue:
			varValue = varValue.replace('_', ' ')
			return varValue

	return varValue


def insertBusinessAttributes():
	df = pd.read_csv(r"D:\Projects\YELP\data\businessAttributesNew.csv")
	allDocs = []
	for index, row in df.iterrows():
		
		business_id = row['business_id']
		
		ByAppointmentOnly = fillNull('ByAppointmentOnly', row['ByAppointmentOnly'])

		BusinessAcceptsCreditCards = fillNull('BusinessAcceptsCreditCards', row['BusinessAcceptsCreditCards'])

		BikeParking = fillNull('BikeParking', row['BikeParking'])

		RestaurantsPriceRange2 = fillNull('RestaurantsPriceRange2', row['RestaurantsPriceRange2'])

		CoatCheck = fillNull('CoatCheck', row['CoatCheck'])
		
		RestaurantsTakeOut = fillNull('RestaurantsTakeOut', row['RestaurantsTakeOut'])

		RestaurantsDelivery = fillNull('RestaurantsDelivery', row['RestaurantsDelivery'])

		Caters = fillNull('Caters', row['Caters'])

		WiFi = fillNull('WiFi', row['WiFi'])

		BusinessParking_garage = fillNull('BusinessParking_garage', row['BusinessParking_garage'])

		BusinessParking_street = fillNull('BusinessParking_street', row['BusinessParking_street'])

		BusinessParking_validated = fillNull('BusinessParking_validated', row['BusinessParking_validated'])

		BusinessParking_lot = fillNull('BusinessParking_lot', row['BusinessParking_lot'])

		BusinessParking_valet = fillNull('BusinessParking_valet', row['BusinessParking_valet'])

		WheelchairAccessible = fillNull('WheelchairAccessible', row['WheelchairAccessible'])

		HappyHour = fillNull('HappyHour', row['HappyHour'])

		OutdoorSeating = fillNull('OutdoorSeating', row['OutdoorSeating'])

		HasTV = fillNull('HasTV', row['HasTV'])

		RestaurantsReservations = fillNull('RestaurantsReservations', row['RestaurantsReservations'])

		DogsAllowed = fillNull('DogsAllowed', row['DogsAllowed'])

		Alcohol = fillNull('Alcohol', row['Alcohol'])

		GoodForKids = fillNull('GoodForKids', row['GoodForKids'])

		RestaurantsAttire = fillNull('RestaurantsAttire', row['RestaurantsAttire'])

		RestaurantsTableService = fillNull('RestaurantsTableService', row['RestaurantsTableService'])

		RestaurantsGoodForGroups = fillNull('RestaurantsGoodForGroups', row['RestaurantsGoodForGroups'])

		DriveThru = fillNull('DriveThru', row['DriveThru'])

		NoiseLevel = fillNull('NoiseLevel', row['NoiseLevel'])

		Ambience_romantic = fillNull('Ambience_romantic', row['Ambience_romantic'])

		Ambience_intimate = fillNull('Ambience_intimate', row['Ambience_intimate'])

		Ambience_touristy = fillNull('Ambience_touristy', row['Ambience_touristy'])
		
		Ambience_hipster = fillNull('Ambience_hipster', row['Ambience_hipster'])
		
		Ambience_divey = fillNull('Ambience_divey', row['Ambience_divey'])
		
		Ambience_classy = fillNull('Ambience_classy', row['Ambience_classy'])
		
		Ambience_trendy = fillNull('Ambience_trendy', row['Ambience_trendy'])
		
		Ambience_upscale = fillNull('Ambience_upscale', row['Ambience_upscale'])
		
		Ambience_casual = fillNull('Ambience_casual', row['Ambience_casual'])
		
		GoodForMeal_dessert = fillNull('GoodForMeal_dessert', row['GoodForMeal_dessert'])
		
		GoodForMeal_latenight = fillNull('GoodForMeal_latenight', row['GoodForMeal_latenight'])
		
		GoodForMeal_lunch = fillNull('GoodForMeal_lunch', row['GoodForMeal_lunch'])
		
		GoodForMeal_dinner = fillNull('GoodForMeal_dinner', row['GoodForMeal_dinner'])
		
		GoodForMeal_brunch = fillNull('GoodForMeal_brunch', row['GoodForMeal_brunch'])
		
		GoodForMeal_breakfast = fillNull('GoodForMeal_breakfast', row['GoodForMeal_breakfast'])
		
		BusinessAcceptsBitcoin = fillNull('BusinessAcceptsBitcoin', row['BusinessAcceptsBitcoin'])
		
		Smoking = fillNull('Smoking', row['Smoking'])
		
		Music_dj = fillNull('Music_dj', row['Music_dj'])
		
		Music_background_music = fillNull('Music_background_music', row['Music_background_music'])
		
		Music_no_music = fillNull('Music_no_music', row['Music_no_music'])
		
		Music_jukebox = fillNull('Music_jukebox', row['Music_jukebox'])
		
		Music_live = fillNull('Music_live', row['Music_live'])
		
		Music_video = fillNull('Music_video', row['Music_video'])
		
		Music_karaoke = fillNull('Music_karaoke', row['Music_karaoke'])
		
		GoodForDancing = fillNull('GoodForDancing', row['GoodForDancing'])
		
		AcceptsInsurance = fillNull('AcceptsInsurance', row['AcceptsInsurance'])
		
		BestNights_monday = fillNull('BestNights_monday', row['BestNights_monday'])
		
		BestNights_tuesday = fillNull('BestNights_tuesday', row['BestNights_tuesday'])
		
		BestNights_friday = fillNull('BestNights_friday', row['BestNights_friday'])
		
		BestNights_wednesday = fillNull('BestNights_wednesday', row['BestNights_wednesday'])
		
		BestNights_thursday = fillNull('BestNights_thursday', row['BestNights_thursday'])
		
		BestNights_sunday = fillNull('BestNights_sunday', row['BestNights_sunday'])
		
		BestNights_saturday = fillNull('BestNights_saturday', row['BestNights_saturday'])
		
		BYOB = fillNull('BYOB', row['BYOB'])
		
		Corkage = fillNull('Corkage', row['Corkage'])

		


		docSingle = (business_id, ByAppointmentOnly, BusinessAcceptsCreditCards, BikeParking, RestaurantsPriceRange2,
			CoatCheck, RestaurantsTakeOut, RestaurantsDelivery, Caters, WiFi, BusinessParking_garage, BusinessParking_street,
			BusinessParking_validated, BusinessParking_lot, BusinessParking_valet, WheelchairAccessible, HappyHour,
			OutdoorSeating, HasTV, RestaurantsReservations, DogsAllowed, Alcohol, GoodForKids, RestaurantsAttire,
			RestaurantsTableService, RestaurantsGoodForGroups, DriveThru, NoiseLevel, Ambience_romantic, Ambience_intimate,
			Ambience_touristy, Ambience_hipster, Ambience_divey, Ambience_classy, Ambience_trendy, Ambience_upscale,
			Ambience_casual, GoodForMeal_dessert, GoodForMeal_latenight, GoodForMeal_lunch, GoodForMeal_dinner,
			GoodForMeal_brunch, GoodForMeal_breakfast, BusinessAcceptsBitcoin, Smoking, Music_dj, Music_background_music,
			Music_no_music, Music_jukebox, Music_live, Music_video, Music_karaoke, GoodForDancing, AcceptsInsurance,
			BestNights_monday, BestNights_tuesday, BestNights_friday, BestNights_wednesday, BestNights_thursday,
			BestNights_sunday, BestNights_saturday, BYOB, Corkage)

		sql_query = '''INSERT INTO business_attributes VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
		cursor.execute(sql_query, docSingle)

		if index%10000 == 0:
			print(index)
			conn_details.commit()
	conn_details.commit()
	cursor.close()
	conn_details.close()



# cursor.execute(sql_query)
# conn_details.commit()
# cursor.close()
# conn_details.close()