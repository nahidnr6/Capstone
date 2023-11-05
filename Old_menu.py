import pyspark
from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName('CreditCardSystem1').getOrCreate()

spark = SparkSession.builder.appName('CreditCardSystem').getOrCreate()

#CREATING BRANCH PANDAS DATAFRAME
#read branch json file
import pandas as pd
from pyspark.sql.types import DateType
branch = spark.read.load(r"C:\Users\Learner_XZHCG302\Downloads\cdw_sapp_branch.json", format="json", header = True,inferSchema = True)

#create temp view to query branch table
branch.createTempView("branch_table")

#create updated phone dataframe by querying branch table 
updated_phone = spark.sql("SELECT CONCAT('(',substr(branch_phone,1,3),')',substr(branch_phone,4,3),'-',substr(branch_phone,7,4)) as updated_phone from branch_table")

#convert updated_phone into pandas dataframe
updated_phone = updated_phone.select("*").toPandas()

#convert branch into pandas df
new_branch = branch.select("*").toPandas()

#concat dfs into one 
updated_branch = pd.concat([new_branch, updated_phone], axis =1)
#updated_branch

branch_spark = spark.createDataFrame(updated_branch)

#CREATING CREDIT DATA FRAME
#read credit json file
credit = spark.read.load(r"C:\Users\Learner_XZHCG302\Downloads\cdw_sapp_credit.json", format="json", header = True,inferSchema = True)
#create temp table to query credit dataframe
credit.createTempView("credit_table")

#combine year, month, and day with sql query and store into a new dataframe
timeid = spark.sql("SELECT CONCAT(year, month, day) as TIMEID FROM credit_table")

#convert credit dataframe and timeid dataframe into pandas dataframes
new_credit = credit.select("*").toPandas()
timeid = timeid.select("*").toPandas()

#concat the two pandas dataframe
updated_credit = pd.concat([new_credit, timeid], axis =1)

#CREATING CUSTMER PANDAS DATAFRAME
#Read the custmer json file into a dataframe
custmer = spark.read.load(r"C:\Users\Learner_XZHCG302\Downloads\cdw_sapp_custmer.json", format="json", header = True,inferSchema = True)

#Create a temp view of custmer
custmer.createTempView("custmer_table")

#create dataframe by querying custmer table with lower middle name
middle_name = spark.sql("select lower(middle_name) as middle_name from custmer_table")

#create full street address dataframe
full_address = spark.sql("SELECT CONCAT(apt_no, ', ', street_name) as full_street_address FROM custmer_table")

#convert full address dataframe to pandas dataframe
full_address = full_address.select("*").toPandas()

#convert middle name dataframe into pandas dataframe
middle_name = middle_name.select("*").toPandas()

#create custmer phone dataframe
spark.sql("SELECT CONCAT('(',substr(cust_phone,1,3),')',substr(cust_phone,4,3),'-',substr(cust_phone,7,4)) as updated_phone from custmer_table")

#convert custmer dataframe into pandas dataframe 
new_custmer = custmer.select("*").toPandas()

#Concat the two new pandas dataframes into one
updated_custmer = pd.concat([new_custmer, middle_name, full_address], axis =1)

#print updated customer table 
updated_custmer

#2.1 1) Used to display the transactions made by customers living in a given zip code for a given month and year.
#Order by day in descending order.
#from pyspark.sql.functions import col, asc,desc
def transactions(year, month, zipcode):
    df = credit.join(custmer, credit.CUST_SSN == custmer.SSN,  'outer')
    df.filter( (df['year'] == year) & (df['month'] == month) & (df['CUST_ZIP'] == zipcode)).sort('day', ascending= False).show(10) 

#Req 2.1 2) Used to display the number and total values of transactions for a given type.

#Used credit pyspark dataframe and filtered based on transaction type. To get the total values, used the group by and sum method.
from pyspark.sql.functions import sum
def total_transactions(type):
    print(credit.filter(credit['transaction_type'] == type).count())
    credit.filter(credit['transaction_type'] == type).groupBy().sum('transaction_value').show()

#Req 2.1 3) Used to display the total number and total values of transactions for branches in a given state.

def branch_transactions(state): 
    df2 = credit.join(branch, credit.BRANCH_CODE == branch.BRANCH_CODE,  'outer')
    #below prints the total transactions 
    print(df2.filter(df2['BRANCH_STATE'] == state).count())
    #below prints the total value of all the transactions
    df2.filter(df2['BRANCH_STATE'] == state).groupBy().sum('transaction_value').show()

#Req 2.2 1)Used to check the existing account details of a customer.
def account_details(SSN):
    custmer.filter(custmer['SSN'] == SSN).show()

#Req 2.2 3) Modify Account details
# Code is directly in the menu if statements

#Req 2.2 3) Used to generate a monthly bill for a credit card number for a given month and year.
def monthly_bill(credit_card_no, month, year):
    bill = credit.filter((credit['credit_card_no']== credit_card_no) & (credit['month']== month) & (credit['year']==year )).toPandas()
    print(bill['TRANSACTION_VALUE'].sum())


while True:
    #selection = pyip.inputMenu(['Total Transactions in zipcode', 'Banking Transactions', 'Visualization Menu'], numbered=True)
    print("Please choose one of the following menu options")
    print("1. Total Transactions in zipcode")
    print("2. Total Transactions for transaction type and total cost of all transactions")
    print("3. Total transcations and total value for all branches in a given state ")
    print("4. Account details")
    print("5. Credit card bill for the month")
    print("6. Banking transcations")
    print("7. Modify account details")
    print("8. Quit")
        
    selection = input("Enter Choice: ")
    selection = selection.strip()

    if (selection == '1'):
        year = input("Choose the year:")
        month = input("Choose the month:")
        zipcode = input("Choose zipcode:")
        print(transactions(year, month, zipcode))

    elif (selection == '2'):
        types = input("Choose the transaction type:")
        print(total_transactions(types))

    elif (selection == '3'):
        state = input("Choose a state:")
        print(branch_transactions(state))

    elif (selection == '4'): 
        SSN = input("Enter your SSN:")
        account_details(SSN)

    elif(selection == '5'):
        credit_card_no = input("Enter your credit card number:")
        month = input("Enter a month:")
        year = input("Enter a year:")
        monthly_bill(credit_card_no, month, year)

    elif(selection == '7'):
        SSN = input("Enter your SSN:")
        print("1. Credit Card Number")
        print("2. Phone number")
        print("3. Email")
        choice = input("Which of the above would you like to modify:")
        if choice == '1': 
            new_CCN = input('Enter new credit card number:')
            df = (custmer.filter(custmer['SSN'] == SSN).toPandas())
            df.at[0, 'CREDIT_CARD_NO'] = new_CCN
            print(df)
        if choice == '2': 
            new_phone = input('Enter new phone number:')
            df = (custmer.filter(custmer['SSN'] == SSN).toPandas())
            df.at[0, 'CUST_PHONE'] = new_phone
            print(df)
        if choice == '3':
            email = input('Enter new email address:' )
            df = (custmer.filter(custmer['SSN'] == SSN).toPandas())
            df.at[0, 'CUST_EMAIL'] = email
            print(df)

    elif(selection == '8'):
        break
    