#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
@author: sagar_paithankar
"""

import numpy as np
from PIL import Image
import pandas as pd
import pickle
from datetime import datetime, timedelta

# Import User Defined Class
from db import DB

class ImageProcessing:
    
    def remove_colors(self,image):
        pixels = [[0, 0, 0],[170, 170, 170]]
        for combination in pixels:
            data = np.array(image)
            rgb = data[:,:,:3]
            original_green = [255, 255, 255] # Replacing with White
            mask = np.all(rgb == combination, axis = -1)
            data[mask] = original_green
            image = Image.fromarray(data)
        
        return image
    
    
    def rainbar(self,image):
        rain = image.getcolors()
        rain = [x for x in rain if x[1] != (255, 255, 255)]
        return rain
    
    def replace_colors(self,image):
        try:
            pixels = [[250, 60, 60]]
            for combination in pixels:
                data = np.array(image)
                rgb = data[:,:,:3]
                original_green = [32, 208, 32] # Replacing with White
                mask = np.all(rgb == combination, axis = -1)
                data[mask] = original_green
                image = Image.fromarray(data)
        
        except:
            pass
        return image
    
    
    def pixelcoding(self,rain,image):
        pixel = list(image.getdata())
        indices = [i for i, x in enumerate(pixel) if x == rain[0][1]]
        indices = [(x%970,x%110) for x in indices]
        df = pd.DataFrame(data=indices,index= range(len(indices)),columns=['row','column'])
        return df
    
    def get_forecast(self,rain,img,dates):
        df_rain = rain.sort_values('row').groupby('row').mean().reset_index()
        df = pd.DataFrame()
        time_block = list(pd.date_range('2018-01-01 00:00:00','2018-01-02 00:00:00',freq='15T'))[:-1]
        time_block = [x.time() for x in time_block]
        for i in range(0,img.size[0],97):
            df_temp = pd.DataFrame(columns=['date','time','forecast','horizon'])
            raining = []
            for j in range(97):
                if i+j in df_rain.iloc[:,0].tolist():
                    raining.append(1)
                else:
                    raining.append(0)
            
            raining[95] = (raining[95]+raining[96])/2
            raining = raining[:-1]
            df_temp['time'] = time_block
            df_temp['forecast'] = raining
            df_temp['date'] = dates + timedelta(i/97)
            df_temp['horizon'] = int(i/97) 
            df = pd.concat([df,df_temp])
            
        return df
    
    def no_rain(self,img,dates):
        df = pd.DataFrame()
        time_block = list(pd.date_range('2018-01-01 00:00:00','2018-01-02 00:00:00',freq='15T'))[:-1]
        time_block = [x.time() for x in time_block]
        for i in range(0,img.size[0],97):
            df_temp = pd.DataFrame(columns=['date','time','forecast','horizon'])
            df_temp['time'] = time_block
            df_temp['forecast'] = 0
            df_temp['date'] = dates + timedelta(i/97)
            df_temp['horizon'] = int(i/97)
            df = pd.concat([df,df_temp])
            
        return df
    
    def store_forecast(self,df):
        db = DB()
        engine = db.getEngine()
        try:
            df.to_sql(con=engine,name = 'meteo_data',if_exists="append",index=False)
        except:
            print("Error in DB store")
        return df
        
    
    
    