#!/usr/bin/env python3.5
# -*- coding: utf-8 -*-
"""
@author: sagar_paithankar
"""

import paramiko

class ImageGrabber:
    
    def get_image(self,image_name,path1):
        remote_file = '/home/amss/meteogram_images/SFD-meteogram/{}.gif'.format(image_name)
        local_file = path1+'/images/{}.gif'.format(image_name)
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname='139.59.16.54',username='root',password='dentintheuniverse',port=22)
        ftp_client=ssh_client.open_sftp()
        ftp_client.get(remote_file,local_file)
        ftp_client.close()
        ssh_client.close()
