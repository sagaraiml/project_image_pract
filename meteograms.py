#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagar_paithankar
"""


import os
import sys
import pandas as pd
from PIL import Image
from datetime import datetime, timedelta
import pytesseract
import logging
import time
import pymysql
sys.path.append('/usr/local/lib/python3.5/dist-packages/pytesseract/')
#path1 = '/Users/sagar_paithankar/Desktop/image_recognition/Delhi_meteo/'
path1 = '/root/radar/delhi/meteo/'
os.chdir(path1)

from image_processing import ImageProcessing
from image_grabber import ImageGrabber
processor = ImageProcessing()
grabber = ImageGrabber()

# Setting Logger
logger = logging.getLogger('meteo')
hdlr = logging.FileHandler(path1 + '/logs/meteo.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info('Started')

# Setting Timezone
os.environ["TZ"] = "Asia/Kolkata"
time.tzset() 
logger.info('Timezone Set: {}'.format(datetime.now()))

start = datetime.now().replace(second=0, microsecond=0) 
end =   start + timedelta(minutes=3*60)
seconds = (end - start).total_seconds()
minutes = []
step = timedelta(minutes=3*60)

for i in range(0, int(seconds), int(step.total_seconds())):
    minutes.append(start + timedelta(seconds=i))
    
for minute in minutes:
    logger.info(minute)
    date = minute
    date_format = date.strftime('%Y-%m-%d-%H-%M')
    image_name = 'SFD-meteogram_{}'.format(date_format)
    image_name = image_name[:-2]
    
    ### Image Grabbing
    try:
        for number in range(11):
            image_name1 = image_name + datetime.strftime((pd.to_datetime(date).floor('15min') + timedelta(minutes=number)),"%M")
            try:
                grabber.get_image(image_name1,path1)
                logger.info('Image: {0}_Grabbed_successfully'.format(image_name1))
                image_name = image_name1
                break
            except Exception as e:
                logger.info('Error while grabbing image: {0} _due to: {1}'.format(image_name1, str(e)))
                logger.info('Re-trying grabbing image:{0}'.format(image_name1))
                try:
                    grabber.get_image(image_name1,path1)
                    image_name = image_name1
                    logger.info('On re-trying Image: {0}_Grabbed_successfully'.format(image_name1))
                    break
                except:
                    logger.info('Re-trying also failed_Error while grabbing image: {0} _due to: {1}'.format(image_name1, str(e)))
                    logger.info('no_image!')
                    
    except:
         pass
     
    ### Image Loading and Pixelation to RGB
    try:
        original = Image.open(path1+"images/{}.gif".format(image_name)).convert('RGB')
        logger.info('Image: {0}_loaded_successfully'.format(image_name))
    except Exception as e:
        logger.info('Error for image: {0} _due to: {1}'.format(image_name, str(e)))
        logger.info('Re-trying loading image:{0}'.format(image_name))
        try:
            original = Image.open(path1+"images/{}.gif".format(image_name)).convert('RGB')
            logger.info('On re-trying Image: {0}_loaded_successfully'.format(image_name))
        except Exception as e:
            logger.info('Re-trying also failed_Error while loading image: {0} _due to: {1}'.format(image_name, str(e)))
            logger.info('bad_image!')

    
    ### Image Processing
    try:
        ### Cropping Image for time
        timing = original.crop((185, 1065,208, 1100))
        next_date = pytesseract.image_to_string(timing, config='-psm 6')
        next_date = datetime(date.year,date.month,int(next_date)).date()
        dates = next_date - timedelta(days=1)
        logger.info('Day 0 is {}'.format(dates))
        
        ### Cropping image to have just the radar image
        img = original.crop((120, 950,1090, 1060))
        logger.info('image cropped')
        
        ### Removing unnecessary colors
        img = processor.remove_colors(img)
        logger.info('color removed')
        
        ### Replacing red with green to have a single color
        img = processor.replace_colors(img)
        logger.info('color replaced')
        
        ### Making List of rain forecasting pixels
        rain = processor.rainbar(img)
        logger.info('rain bars ready')
        
        ### Checking if rain forecasted
        if rain:
            logger.info('There is some rain!')
            ### Getting location for each pixel
            rain = processor.pixelcoding(rain,img)
            logger.info('Pixel coding done')
            ### Gettiung forecast
            barish = processor.get_forecast(rain,img,dates)
            logger.info('Getting forecast')
        else:
            barish = processor.no_rain(img,dates)
            logger.info('No rain and df ready')
        
        barish['created_at'] = datetime.now()
        barish['forecast'] = barish['forecast'].apply(lambda x: int(1) if x > 0 else int(0))
        logger.info('DF ready to go')
#        processor.store_forecast(barish)
        conn = pymysql.connect("139.59.42.147","dummy","dummy123","energy_consumption")
        mycursor = conn.cursor()
        query = "INSERT INTO meteo_data(`date`, `time`, `forecast`, `horizon`, `created_at`) VALUES ('{}','{}','{}','{}','{}') ON DUPLICATE KEY UPDATE forecast='{}', horizon='{}', created_at='{}'"
        for row in barish.values:
           	mycursor.execute(query.format(row[0], row[1], row[2], row[3], row[4], row[2], row[3], row[4]))
        conn.commit()
        mycursor.close()
        conn.close()
        logger.info('Data stored in the DB!')
    except:
        logger.info('Error While Image Processing')
            