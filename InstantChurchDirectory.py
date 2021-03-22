# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
#This cell will attempt to read the directory file using pandas - I like the output! 
import pandas
import re
import phonenumbers

df = pandas.read_csv('InstantChurchDirectory-20210218-042000.csv')
# Set missing values to empty strings
df['Child_Names'] = df['Child_Names'].fillna('')
df['Address'] = df['Address'].fillna('')  
df['City'] = df['City'].fillna('')
df['State'] = df['State'].fillna('')
df['Phone_Numbers'] = df['Phone_Numbers'].fillna('')
df['Emails'] = df['Emails'].fillna('')
df['Additional_Details'] = df['Additional_Details'].fillna('')
df['Notes'] = df['Notes'].fillna('')

# Print a sample
df.head(5)


# %%
#Good Code: This cell will attempt to create a new dataframe with rows for each person

phone_cf = df.copy(deep=True)

# Keep only the columns needed to create the contacts (FamilyId will be the key)
phone_cf.drop(columns=['Emails', 'Last_Name', 'First_Name', 'Child_Names'], inplace=True)
phone_cf.drop(columns=['Address', 'City', 'State','ZIP'], inplace=True)
phone_cf.drop(columns=['AnniversaryDate', 'Additional_Details'], inplace=True)
phone_cf.drop(columns=['Notes', 'Active', 'ImageExists', 'DateModified'], inplace=True)
phone_cf.head(10)


# %%
#Good Code: This cell will attempt to create a new dataframe with rows for each person

email_cf = df.copy(deep=True)

# Keep only the columns needed to create the contacts (FamilyId will be the key)
email_cf.drop(columns=['Phone_Numbers', 'Last_Name', 'First_Name', 'Child_Names'], inplace=True)
email_cf.drop(columns=['Address', 'City', 'State','ZIP'], inplace=True)
email_cf.drop(columns=['AnniversaryDate', 'Additional_Details'], inplace=True)
email_cf.drop(columns=['Notes', 'Active', 'ImageExists', 'DateModified'], inplace=True)
email_cf.head(10)


# %%
# Good Code:  This cell will attempt to create multiple rows for each phone
phone_cf['Phone_Numbers'] = phone_cf['Phone_Numbers'].str.split(',')
phone_cf =  phone_cf.explode('Phone_Numbers').reset_index(drop=True)
cols = list(phone_cf.columns)
cols.append(cols.pop(cols.index('FamilyId')))
phone_cf = phone_cf[cols]
phone_cf.head(10)


# %%
# Good Code:  This cell will attempt to create multiple rows for each email
email_cf['Emails'] = email_cf['Emails'].str.split(',')
email_cf =  email_cf.explode('Emails').reset_index(drop=True)
cols = list(email_cf.columns)
cols.append(cols.pop(cols.index('FamilyId')))
email_cf = email_cf[cols]
email_cf.head(10)


# %%
# Good Code: Combining a function with phone number cleanup
# Note:  Need to add code to return a blank string if no phone number present
def find_phone_number(row):
    phone_row = ''.join(map(str, row))
    for match in phonenumbers.PhoneNumberMatcher(phone_row, "US"):
        return str(phonenumbers.format_number(match.number,                              phonenumbers.PhoneNumberFormat.NATIONAL))
phone_cf['PhoneG']=phone_cf['Phone_Numbers'].apply(lambda x: find_phone_number(x))
phone_cf.head(10)


# %%
# Good Code: Combining a function with phone owner cleanup
def find_phone_owner(row):
    phone_row = ''.join(map(str, row))
    if phone_row.rfind(')'):
        owner = phone_row.rstrip("*")[phone_row.rfind('(')+1:-1]
    else:
        owner = ''
    return str(owner.strip())
phone_cf['Phone_Owner']=phone_cf['Phone_Numbers'].apply(lambda x: find_phone_owner(x))
phone_cf.drop(columns=['Phone_Numbers'], inplace=True)
phone_cf.head(10)


# %%
# Good Code: Combining a function with email owner cleanup
def find_email_owner(row):
    email_row = ''.join(map(str, row))
    if email_row.rfind(')'):
        owner = email_row.rstrip("*")[email_row.rfind('(')+1:-1]
    else:
        owner = ''
    return str(owner.strip())
email_cf['Email_Owner']=email_cf['Emails'].apply(lambda x: find_email_owner(x))
email_cf.head(10)


# %%
# Good Code: Combining a function with email cleanup
# Note:  Need to add code to return a blank string if no email present
def find_email_address(row):
    email_row = ''.join(map(str, row))  # convert to a string
    email_row_str = email_row.strip()   # remove leading and trailing blanks
    email_address_list = email_row_str.split(" ") # split into a list
    return str(email_address_list[0])
email_cf['Email_Address']=email_cf['Emails'].apply(lambda x: find_email_address(x))
email_cf.drop(columns=['Emails'], inplace=True)
email_cf.head(10)


# %%
# Merge the Dataframes: THis cell will attempt to merge the email and phone dataframes with the main
def merge_and_stat(df_1, df_2, merge_key):
    # This function will merge 2 dataframes using left join and a key 
    name1 =[x for x in globals() if globals()[x] is df_1][0]
    name2 =[x for x in globals() if globals()[x] is df_2][0]
    merge_df = pandas.merge(df_1,df_2,on=merge_key)
    print("Dataframe Merge: %s and %s" % (name1, name2))
    merge_df.head(5)
    return merge_df    

df = merge_and_stat(df, email_cf,['FamilyId'])
df = merge_and_stat(df, phone_cf,['FamilyId'])

# Create an index to drop rows where: 
#   Email_Owner is not blank and
#   Phone_Owner is not blank and
#   Email_Owner is not equal to Phone_Owner
index_names = df[ (df['Phone_Owner'] != '') & (df['Email_Owner'] !='') & (df['Phone_Owner'] != df['Email_Owner'])].index 
  
# drop these given row indexes from dataFrame 
df.drop(index_names, inplace = True) 


# %%
# This cell will write out the results to a csv file
df.to_csv('InstantChurchDirectoryOutput.csv')


# %%



