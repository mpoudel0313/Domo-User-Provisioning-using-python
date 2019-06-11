
# -*- coding: utf-8 -*-
"""
Created on Thu May 31 15:38:12 2018

@author: mpoudel

This scripts automates the user provisioning in domo.
"""



import pandas as pd
from pydomo import Domo
import os
import math


# SCRIPT PARAMETERS
CLIENT_ID = "<Enter the client Id>"
CLIENT_SECRET = "<Enter the client secret>"
orgchart_dataset_id = "<domo dataset id for the company orgchart data>"
API_HOST = 'api.domo.com'
domo_dataset_id = '<domo dataset id for existing users in domo>'  #this information can be pulled from domostats

# Main class for the user provisioning and deprovisioning
class UserManagement:
    def __init__(self):
        domo = self.init_domo_(CLIENT_ID,CLIENT_SECRET)
        self.get_domo_users(domo)
        self.get_orgchart_users(domo)
        self.users(domo)
        
    #initiate the connection to domo
    def init_domo_(self,client_id,client_secret,**kwargs):
        return Domo(client_id,client_secret,api_host = API_HOST,**kwargs)
    
    #export domo users list 
    def get_domo_users(self,domo):   
         #remove the filename if exists in working directory 
        filename = ['domo_users.csv']
        for file in filename:
            if os.path.exists(file):
                os.remove(file)       
        datasets = domo.datasets
        # file path where file to be exported 
        file_path = './domo_users.csv'
        include_csv_header = True
        domo_user_download = datasets.data_export_to_file(domo_dataset_id,file_path,include_csv_header)  
        domo_user_download.close()
        #read the exported user file using pandas and  store into dataframe
        domo_user_data = pd.DataFrame(pd.read_csv('domo_users.csv'))
        
        user_list = []        
        #loop through dataframe and 
        for user in domo_user_data.values:
            user_detail = {}
            # employees that does not have missing employee id flag them with type 'empid'   
            if not self.isnan(user[7]):
                user_detail['id'] = int(user[0])
                user_detail['employee_id'] = int(user[7])
                user_detail['email'] =user[6] 
                user_detail['type'] = 'empid'
                #append that to the list
                user_list.append(user_detail)
            else:
                #employees that has missing employee id flag them as type 'no_empid
                if self.isnan(user[7]):
                    user_detail['id'] = int(user[0])
                    user_detail['employee_id'] = user[7]
                    user_detail['email'] = user[6]
                    user_detail['type'] = 'no_empid'
                    #append that to the list
                    user_list.append(user_detail)
        return user_list
        
    
     #get orgchart user information   
    def get_orgchart_users(self,domo):
        # remove file if already exists in your working directory
        filename = ['orgchart_dataset.csv']
        for file in filename:
            if os.path.exists(file):
                os.remove(file)
        datasets = domo.datasets    
        # file path where file to be exported 
        file_path = './orgchart_dataset.csv'
        include_csv_header = True
        orgchart_download = datasets.data_export_to_file(orgchart_dataset_id,file_path,include_csv_header)
        orgchart_download.close()
        #read the exported orgchart file using pandas and store into dataframe
        orgchart_data = pd.DataFrame(pd.read_csv('orgchart_dataset.csv'))
        return orgchart_data

    #function for identifying null values
    def isnan(self,value):
            try:
                return math.isnan(float(value))
            except:
                return False
    
    
    #main methods that does all the user provisioning and deprovisioning
    def users(self,domo):
        orgchart_users = self.get_orgchart_users(domo)
        domo_users = self.get_domo_users(domo)
              
        #to hold users list that are recently hired,termed and missing profile information
        new_hires = []         
        termed = []
        missing_profile = []
        
        #Loop through the orgchart
        for user in orgchart_users.values:
            #Check if the employee is active. 
            if user[7] == 'Active':
                #If Active check if employee belongs to sales staff or support staff
                if user[4] != 'Administration': 
                    #If employee belongs to sales staff and user profile exists in domo but with missing information
                    if user[6] in [x['email'] for x in domo_users if x['type']== 'no_empid']:
                        #Store the employee information in dictionary within list
                        missing_info = {}
                        user_id = {}
                        missing_info['title'] = user[2]
                        missing_info['employeeNumber'] = user[0]
                        missing_info['email'] = user[6]
                        missing_info['name'] = user[1]
                        #Get domo user id based on matching email from orgchart and domo profile
                        user_id['id'] = int(''.join([str(x['id']) for x in domo_users if x['email'] == user[6]]))
                        missing_profile.append([user_id,missing_info]) 
                
                    # If employee id does not exists in domo
                    elif user[0] not in [x['employee_id'] for x in domo_users] :
                        
                        #If user  email does not exists in domo
                        if user[6] not in [x['email'] for x in domo_users if x['type'] == 'no_empid']:
                            
                            #skip if any of the following information are missing in orgchart
                            #employee name
                            if self.isnan(user[1]):
                                continue
                            
                            #Employee work email
                            if self.isnan(user[6]):
                                continue
                            
                            #Employee Title
                            if self.isnan(user[2]):
                                continue
                            
                            #Employee Office Location
                            if self.isnan(user[5]):
                                continue
                            
                            #If email is not company email
                            if 'company.com' not in user[6]:
                                continue
                            
                            #Store the the accounts to be created in dictionary within list
                            new_hire_info = {}
                            new_hire_info['employeeNumber'] = user[0]
                            new_hire_info['name'] = user[1]
                            new_hire_info['email'] = user[6]
                            new_hire_info['role'] = 'Participant'    #role is default as participant
                            new_hire_info['title'] = user[2]
                            new_hire_info['location'] = user[5]
                            new_hires.append(new_hire_info)
            else:
                #If employee is termed
                if user[7] == 'Termed':
                    # and user exists in domo 
                    if user[0] in [x['employee_id'] for x in domo_users]:
                        #gets users domo user id
                        termed.append([x['id'] for x in domo_users if x['employee_id'] == user[0]])
        termed_users = [j for i in termed for j in i]
        #If you would like to sent an email invite for domo
        send_invite = False
        
        #Delete users that are termed
        if termed_users:
            for x in termed_users:
                try:
                    domo.users.delete(x)
                except:
                    pass
        else:
            pass
        
        # Create account for new hires
        if new_hires:
            for x in new_hires:
                try:
                    domo.users.create(x,send_invite)
               
                except:
                    pass
        else:
            pass
        
        # Update user profile for missing information
        if missing_profile:
            for x in missing_profile:
                try:
                    domo.users.update(x[0]['id'],x[1])
                except:
                    pass
        else:
            pass
        
UserManagement()


        
