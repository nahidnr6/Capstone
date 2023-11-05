import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession\
    .builder\
    .config("spark.jars", "/Users/nahidrahman/opt/miniconda3/lib/python3.9/site-packages/pyspark/jars/mysql-connector-j-8.0.31.jar")\
    .appName("menu")\
    .getOrCreate()

df_branch = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_BRANCH") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_credit = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_custmer = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTMER") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

#2.1
#Function to display the transactions made by customers living in a given zip code for a given month and year. Ordered in descending order.
def transactions(year, month, zipcode):
    df = df_credit.join(df_custmer, df_credit.CUST_SSN == df_custmer.SSN,  'outer')
    df.filter( (df['year'] == year) & (df['month'] == month) & (df['CUST_ZIP'] == zipcode)).sort('day', ascending= False).show(10)

#2.1 2) Function used to display the number and total values of transactions for a given type.

#Used credit pyspark dataframe and filtered based on transaction type. To get the total values, used the group by and sum method.
from pyspark.sql.functions import sum
def total_transactions(type):
    transactions_of_type = (df_credit.filter(df_credit['transaction_type'] == type).count())
    print(f'The number of {type} transactions is {transactions_of_type}')
    value_of_transaction_type = df_credit.filter(df_credit['transaction_type'] == type).groupBy().sum('transaction_value')

    # Extract and format the value
    total_value = value_of_transaction_type.collect()[0][0]
    formatted_total_value = round(total_value, 2)
    print(f"The value of these transactions is ${formatted_total_value}")

#Req 2.1 3) Used to display the total number and total values of transactions for branches in a given state.

def branch_transactions(state):
    df2 = df_credit.join(df_branch, df_credit.BRANCH_CODE == df_branch.BRANCH_CODE, 'outer')
    # Store the count of transactions in the specified state
    transactions_in_state = df2.filter(df2['BRANCH_STATE'] == state).count()
    # Print the total number of transactions
    print(f"The number of transactions in {state} is {transactions_in_state}") 
    # Print the total value of all the transactions
    value_of_transactions = df2.filter(df2['BRANCH_STATE'] == state).groupBy().sum('transaction_value')

    # Extract and format the value
    total_value = value_of_transactions.collect()[0][0]
    formatted_total_value = round(total_value, 2)

    # Print the value of the transactions rounded to two decimal places
    print(f"The value of these transactions is ${formatted_total_value}")


#Req 2.2 1)Used to check the existing account details of a customer.
def account_details(SSN):
    df_custmer.filter(df_custmer['SSN'] == SSN).show()

#Req 2.2 3) Modify Account details
# Code is directly in the menu if statements

#Req 2.2 3) Used to generate a monthly bill for a credit card number for a given month and year.
import calendar
def monthly_bill(credit_card_no, month, year):
    month = int(month)
    bill = df_credit.filter((df_credit['credit_card_no']== credit_card_no) & (df_credit['month']== month) & (df_credit['year']==year )).toPandas()
    bill_sum = round(bill['TRANSACTION_VALUE'].sum(), 2)
    month_name = calendar.month_name[month]
    print(f"The monthly bill for {month_name} is ${bill_sum}")

#Menu 

while True:

    #selection = pyip.inputMenu(['Total Transactions in zipcode', 'Banking Transactions', 'Visualization Menu'], numbered=True)
    print("Please enter the number of one of the following menu options:")
    print("(1) Total Transactions in Zipcode")
    print("(2) Total Transactions and Value of Transaction Type")
    print("(3) Total Transactions and Value in State")
    print("(4) Account Details")
    print("(5) Credit Card Bill in Month")
    print("(6) Banking Transactions")
    print("(7) Modify Account Details")
    print("(8) Quit")

    selection = input("Enter Choice: ")
    selection = selection.strip()

    if (selection == '1'):
        year = input("Choose the year:")
        month = input("Choose the month:")
        zipcode = input("Choose zipcode:")
        print(transactions(year, month, zipcode))

    elif (selection == '2'):
        print ("Gas\nEntertainment\nHealthcare\nGrocery\nTest\nEducation")
        types = input("Enter one of the following transaction types above: ") 
        print('...')
        print(total_transactions(types))

    elif (selection == '3'):
        state = input("Enter a state(abbrevation):")
        print('...')
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
            df = (df_custmer.filter(df_custmer['SSN'] == SSN).toPandas())
            df.at[0, 'CREDIT_CARD_NO'] = new_CCN
            print(df)
        if choice == '2': 
            new_phone = input('Enter new phone number:')
            df = (df_custmer.filter(df_custmer['SSN'] == SSN).toPandas())
            df.at[0, 'CUST_PHONE'] = new_phone
            print(df)
        if choice == '3':
            email = input('Enter new email address:' )
            df = (df_custmer.filter(df_custmer['SSN'] == SSN).toPandas())
            df.at[0, 'CUST_EMAIL'] = email
            print(df)
    
    elif(selection == '8'):
        break
