# Capstone

### Process

The Capstone was the final project for my Per Scholas Data Engineering Bootcamp. The picture below shows the workflow of the project. I created a Banking database, a console-based banking application where users can retrieve information, and a data analysis/visualization section. I split it into three files: ETL_process.ipynp for the ETL process, Data_Analysis.ipynb for the data analysis/visualization part and Banking_App.py for developing the banking application and menu where users can query the data.

![Alt text](image.png)

**ETL Process:**

There were four datasets I had to extract: bank branch, credit card customer , credit card transaction , and loan application data. The first three were provided as JSON files, so I used Pyspark to read the data as dataframes. I extracted the last dataset, which was given as an API endpoint, by using a GET request and created a loan dataframe. After extracting the data, I had to perform several transformations. For the branch, customer, and credit data, we were given mapping documents that showed exactly what transformations needed to be done. For example, the format of the branch phone number needed to be changed from XXXXXXXXXX to (XXX)XXX-XXXX. Once all the transformations were done, I loaded the data into the database as branch, customer, credit, and loan tables using the PySpark DataFrameWriter API. Here is a link to this section of the project. Here is a link to the ETL process : [ETL](https://github.com/nahidnr6/Capstone/blob/main/ETL_process.ipynb)

**Data Analysis:**

Once I had all the data in the database, I was ready to perform the data analysis section of the project. In this section, the task was to find and plot valuable information that can be used for Business Analysts. I found how often each transaction type was made and created a pie chart. I also found which states have the most customers, the sum of all transactions for each custoemr, and which customer has the highest transaction amount. Here is a link to the data analysis section : [Data Analysis](https://github.com/nahidnr6/Capstone/blob/main/Data_Analysis.ipynb)

Here are some of the visualizations I created:

![image](https://github.com/nahidnr6/Capstone/assets/64870566/4fe5c562-839b-47be-a95c-5db113c52b8b)

![image](https://github.com/nahidnr6/Capstone/assets/64870566/32decd00-0c6c-481d-98d1-e7db290f8fc1)

![image](https://github.com/nahidnr6/Capstone/assets/64870566/a3da9da8-e58c-4e29-ae65-57221a783d8a)

![image](https://github.com/nahidnr6/Capstone/assets/64870566/de181abd-dfec-45ec-9d16-d9b79846cd84)

**Banking Application:**

In the next part of the project, I was tasked to create a console-based application where users can retrieve transaction details and customer details through a menu. I created the Banking_App.py file. When the file is run, users are asked to select one of the following options:

(1) List of Transactions in Zipcode in Month  
(2) Total Transactions and Value of Transaction Type  
(3) Total Transactions and Value in State  
(4) Account Details  
(5) Credit Card Bill in Month  
(6) Banking Transactions  
(7) Modify Account Details  
(8) Quit

Depending on what the user inputs in the menu, a function would be called displaying the output. I created the functions by querying the data using SparkSQL Here is a link to the banking application: [Bank App](https://github.com/nahidnr6/Capstone/blob/main/Banking_App.py)
