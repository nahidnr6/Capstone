#Part 3: Banking Application

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

df_customer = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_customer") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

#Menu 
print("Hello!")

while True:
    print("Choose one of the following options below.")
    print("(1) List of Transactions in Zipcode in Month")
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

        def list_transactions_by_zipcode_month(df_credit, df_customer):
            try:
                # Create temporary views for the DataFrames
                df_credit.createOrReplaceTempView("credit")
                df_customer.createOrReplaceTempView("customer")

                # Get user input for month, year, and zipcode
                month = int(input("Enter the month: "))
                year = input("Enter the year: ")
                zipcode = input("Enter the zipcode: ")

                # Perform Spark SQL operation to list transactions
                result_df = spark.sql(f"""
                    SELECT BRANCH_CODE, credit.CREDIT_CARD_NO, CUST_SSN, DAY, MONTH, TRANSACTION_ID, TRANSACTION_TYPE, TRANSACTION_VALUE, YEAR, customer.CUST_ZIP
                    FROM credit JOIN customer ON customer.SSN == credit.CUST_SSN
                    WHERE MONTH == {month} AND YEAR == {year} AND customer.CUST_ZIP == {zipcode}
                """)

                # Show the result DataFrame
                result_df.show()

            except Exception as e:
                print(f"An error occurred: {e}")

        # Example usage:
        list_transactions_by_zipcode_month(df_credit, df_customer)


    elif (selection == '2'):

        df_credit.createOrReplaceTempView("credit")
        spark.sql("SELECT distinct transaction_type from credit").show()
        type = input("Select one of the transaction types above: ")
        print(f"The total number of {type} transactions is: ")
        spark.sql(f"SELECT count(*) as Number \
                    FROM credit \
                    WHERE Transaction_Type == '{type}'").show()
        
        print(f"The total value of {type} transactions is: ")
        spark.sql(f"Select round(sum(Transaction_Value),2) as Value \
                    FROM credit \
                    WHERE Transaction_Type == '{type}'").show()

    elif (selection == '3'):
       
        df_branch.createOrReplaceTempView("branch")
        df_credit.createOrReplaceTempView("credit")

        state = input("Enter a state (abbreviation): ")

        print(f"The number of transactions in {state} is:")
        spark.sql(f"SELECT count(TRANSACTION_ID) as Transactions \
                  FROM branch \
                  JOIN credit on branch.BRANCH_CODE == credit.BRANCH_CODE \
                  WHERE branch_state == '{state}'").show()

        print(f"The total value of transactions in {state} is:")
        spark.sql(f"SELECT round(sum(Transaction_Value), 2) as Value \
                  FROM branch \
                  JOIN credit on branch.BRANCH_CODE == credit.BRANCH_CODE \
                  WHERE branch_state == '{state}'").show()

    elif (selection == '4'): 
        SSN = input("Enter your SSN:")
        df_customer.createOrReplaceTempView('customer')
        spark.sql(f"SELECT * FROM customer WHERE SSN == '{SSN}'").show()

    elif (selection == '5'):
        
        number = input("Enter your credit card number? 16 digits. No spaces, no dashes please: ")
        month = input("Enter a month(number): ")
        year = input("Enter a year: ")
        df_credit = df_credit.filter(df_credit.CREDIT_CARD_NO == number)
        df_credit = df_credit.filter(df_credit.MONTH == month)
        df_credit = df_credit.filter(df_credit.YEAR == year)
        df_credit.createOrReplaceTempView("credit")
        spark.sql("SELECT * from credit ORDER BY DAY").show()
        spark.sql("SELECT sum(TRANSACTION_VALUE) as BALANCE from credit").show()


    elif(selection == '7'):
        SSN = input("Enter your SSN:")
        print("1. Credit Card Number")
        print("2. Phone number")
        print("3. Email")
        choice = input("Which of the above would you like to modify:")
        if choice == '1': 
            new_CCN = input('Enter new credit card number:')
            df = (df_customer.filter(df_customer['SSN'] == SSN).toPandas())
            df.at[0, 'CREDIT_CARD_NO'] = new_CCN
            print(df)
        if choice == '2': 
            new_phone = input('Enter new phone number:')
            df = (df_customer.filter(df_customer['SSN'] == SSN).toPandas())
            df.at[0, 'CUST_PHONE'] = new_phone
            print(df)
        if choice == '3':
            email = input('Enter new email address:' )
            df = (df_customer.filter(df_customer['SSN'] == SSN).toPandas())
            df.at[0, 'CUST_EMAIL'] = email
            print(df)
    
    elif(selection == '8'):
        print("Goodbye!")
        break

    else: 
        print("Please enter a valid selection (1, 2, 3, 4, 5, 6, 7, or 8).")
        print("...")
