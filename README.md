# Capstone

Process:

- I split up the project into two files, a python file and jupyter file. I used the jupyter file to complete each step one by one. First by reading and loading the json files and writing them into the database. Then I created functions that used SQL queries and Pandas dataframes to display customer and transaction information such as the total transactions in a zipcode, the total number of transactions of a given type, the total for a given branch, a display for customers , a display to generate the monthly bill, etc. After, I created the visualizations for the Business Analysts such as the percentage rates of each transactions, etc. Once I completed each step in the notebook, I transferred them to a python file where I created the menu for the user. Depending on what the user inputs, in the menu, a function would be called displaying the output.

Challenges:

- One of the challenged I faced was creating the functions from the pysaprk dataframe that was created. Because Pyspark and Pandas uses different syntax's, some methods might work for one thing but not the other. I also utilitized SQL querys to alter the dataframes as well. The main way I resolved this challenge figuring out logically what needed to be done to the dataframe, was by googling exactly how to do it in panda/spark/sqlm and using that syntax. I used all sorts of methods such as filtering, mapping, loc, groupby, count, sum, sort, subqueries, round, joins, having statesments, etc. Using all of these really helped sharpen my problem solving skills
