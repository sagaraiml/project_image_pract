#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
@author: sagar_paithankar
"""

import pymysql
from sqlalchemy import create_engine
 
class DB(object):
    
    def getConnection(self):
        # Open database connection
        db = pymysql.connect("139.59.42.147","dummyt","dummyt123","energy_consumption")
        # prepare a cursor object using cursor() method
        # cursor = db.cursor()
        return db
    
    def getEngine(self):
        engine = create_engine('mysql+pymysql://dummyt:dummyt123@139.59.42.147:3306/energy_consumption', echo=False)
        return engine