This is the course project by Yucheng Wang and Hongsheng Xie. The video url is https://www.youtube.com/watch?v=_1teXLYY9Ts.

# The result of each tasks could be find in the All_code.ipynb file under src folder

# The step to make the code works in the new machine

Firstly, you should navigate to the src folder and find the Create table.sql, you can run it in your pgadmin or you run it in python.

<img width="851" alt="image" src="https://user-images.githubusercontent.com/89940553/164589022-8e18fd62-48bf-4c41-b4d7-afc856ae82b2.png">

After create the table, you can run task1 to pupulate the table by the csv files. For task1, before you run the code in your terminal, you should, 

<img width="498" alt="image" src="https://user-images.githubusercontent.com/89940553/164578736-af088a08-2960-4a00-965e-38ac34a0ac30.png">

change the path of the csv files, also, you should change the password and name of the postgres.

<img width="681" alt="image" src="https://user-images.githubusercontent.com/89940553/164578799-edc9572b-a746-4af2-abb9-6a50aee793f3.png">

And when you finished running the task1, the data is populated in your database, then when you want to the rest of the task, you should change the password and name of the postgres for each of the task. For example, task2_1, you should change the contents inside the red frames.

<img width="893" alt="image" src="https://user-images.githubusercontent.com/89940553/164579101-0c0845ff-e0b7-4f88-8b7b-947a4e36b47e.png">

Then, you should be able to run all the task on your computer properly.


When you are trying to run the test on your computer, you should type "pytest test.py" in your terminal, remember, before that, you should also change the password and name of the postgres for the functions in the test.py too. 

After that, you should be able to run all the code.





# Task 1
The Schema fifa and the table is created by [Create table.sql](https://github.com/36-652-752-Spring-2022/course-project-Yuchengyw6/blob/master/src/Create%20table.sql), the data cleaning and imputation is performed by [Task 1](https://github.com/36-652-752-Spring-2022/course-project-Yuchengyw6/blob/master/src/Task1.ipynb)

## Constraints
The constraints here are 
1, The data type, which I set all the numeric variables to the data type Integer, and rest of the variables, like name and etc. I set them to be varchar() variables with proper length(The length is relatively large since we need all the data in a column not to exceed the maximum length).

2, For the skills variables, the exact values were calculated, the missing values were imputed by using the mean. For rest of the variables, some columns were dropped since these columns contains <50% of data. These columns are： 'release_clause_eur', 'player_tags', 'loaned_from', 'nation_position', 'nation_jersey_number', 'gk_diving', 'gk_handling', 'gk_kicking', 'gk_reflexes', 'gk_speed', 'gk_positioning', 'player_traits'.

3, The primary key here is the unique sofifa_id, which is not nullable. As for other variables, they are actually nullable. The primary key is auto incremental and other variables are not.

4, In some of the variables, they contain some data with the format int+int or int-int, which is hard to recognize by any numeric type, for these variables, I calculated the results, we assume that that we should calculate the result here.

## Screenshots

<img width="232" alt="image" src="https://user-images.githubusercontent.com/89940553/160954420-3a196f55-39d9-43ab-b7f6-852e1ba59e7a.png">

<img width="232" alt="image" src="https://user-images.githubusercontent.com/89940553/160959563-4d305e8c-8556-4d53-9328-3226342e17ec.png">

<img width="238" alt="image" src="https://user-images.githubusercontent.com/89940553/160959627-3834a229-cd29-4a2c-abae-d9b927db23bf.png">

<img width="221" alt="image" src="https://user-images.githubusercontent.com/89940553/160959661-b6820c05-f5b2-4ffa-8faa-ba5f7c48e2a0.png">

# Task 2
You can find all the source code, the .py files in the src folder. The usage of each function could be found in the video. Also you can refer to the comment in each .py file to find out the purpose of each function. You can also find out the result for each task in the result folder. Also, the All_code.ipynb is the ipynb file of the code, you can see the result inside.

# Task3
All the documentations of the function could be find in the src folder, above each function.

The errors is handled properly by using try, except structure, for example, in task2_1,

<img width="910" alt="image" src="https://user-images.githubusercontent.com/89940553/164579904-0661b186-766d-49ea-a58b-8df39961e83a.png">

If the data is not read properly, an error would appear. We did this kind of thing for all the code we have.

The unit test is developed, and you can find the code of the unit test in the folder test, all happy path and sad path are tested.

## Scenarios we tested

### Task1:

Task1 is data cleaning and populating, so we did not distinguished between happy and sad. We did not test too much for this task.

<img width="373" alt="image" src="https://user-images.githubusercontent.com/89940553/164580475-cf1499cb-48e3-4c55-a095-3bbb575d4f2c.png">

1, whether the data is read or not, if not, then the data will be None.

2. whether the data is correctly read, we evaluated the number of rows.

### Task2_1:

Happy path:

<img width="588" alt="image" src="https://user-images.githubusercontent.com/89940553/164580608-0fc1acd7-b4b8-44f4-8e80-3217ebb9cb04.png">

We tested some combinations of the input:
1, year1=2015, year2=2016, x=5, then it should output 5 names, they are 'Scott Brown', 'Adam Smith', 'Mustafa Akbaş', 'Alvin Arrondel', 'Liam Kelly'.

2, year1=2015, year2=2019, x=5, then it should also have 5 names, they should be 'Liam Kelly', 'Adam Smith', 'Jorge Rodríguez', 'Tom Davies', 'Abdulrahman Al Ghamdi'

3, year1=2016, year2=2017, x=5, then it should also have 5 names, they should be 'Scott Brown', 'Danny Ward', 'Alan Smith', 'Jorge Rodríguez', 'Adam Smith'

4,...

In this test we could test any year1<year2 and year1, year2 all in 2015~2020.

Sad path:

<img width="615" alt="image" src="https://user-images.githubusercontent.com/89940553/164582801-5bc38ec3-37a7-4dec-888a-6a7d4613bd89.png">

According to the figure above, we tested some invalid input, for example, when year is a string, when x is not a int, when year1>year2, etc.

### Task 2_2:

Happy path:

<img width="406" alt="image" src="https://user-images.githubusercontent.com/89940553/164581527-0223e081-6dd9-4a25-89bb-3045c3b152ad.png">

there is not too many things worth testing in this task, we tested the normal input of y, y=5 and y=7.

Sad path:

<img width="486" alt="image" src="https://user-images.githubusercontent.com/89940553/164581745-716104b0-68df-4d7d-b801-05a087c4237a.png">

Here we simply tested the invalid input, if we did not have the return "Invalid", it will tell you that, it should be a wrong input(but the code did not react as it is a wrong input). The definition of the invalid input could be find in the documentation.

### Task2_3:

Happy path:

<img width="449" alt="image" src="https://user-images.githubusercontent.com/89940553/164581951-3f7ae1a6-d730-4614-853c-aa8b66930680.png">

Similarly, we tested some normal inputs and their outputs in this test

Sad path:

<img width="889" alt="image" src="https://user-images.githubusercontent.com/89940553/164582116-f833d676-235e-41ec-9af1-e9083aa7a088.png">

It is similar, we tested for invalid input, the definition of the invalid input could be find in the documentation.

### Task2_4:

Task2_4_1 and Task2_4_2 are almost the same, so we will discuss only one of them. 

Happy path:

<img width="389" alt="image" src="https://user-images.githubusercontent.com/89940553/164582318-c29e2a22-963b-43bb-a5bb-cb13c8adab71.png">

We tested the default input, 2017 and 2019, to see whether the result if SUB or not.

Sad path:

<img width="598" alt="image" src="https://user-images.githubusercontent.com/89940553/164582753-200266b6-44ca-4d2e-a1a5-51493e8ce495.png">

It is similar, we tested for invalid input, the definition of the invalid input could be find in the documentation.

### Task 2_5:

Happy path:

<img width="417" alt="image" src="https://user-images.githubusercontent.com/89940553/164582596-f285f7df-00ed-4365-ad94-ccac347255bf.png">

Similarly, we tested the default input and 2017, 2019, too see whether the output is England or not.

Sad path:

<img width="587" alt="image" src="https://user-images.githubusercontent.com/89940553/164582660-7392bfc2-dd01-4b50-8459-4c8b6be3457b.png">

It is similar, we tested for invalid input, the definition of the invalid input could be find in the documentation.

<img width="1139" alt="image" src="https://user-images.githubusercontent.com/89940553/164583465-98c37661-f4c6-4d77-8332-e1ee0c92babf.png">

Here is the test result of our test, all the test passed. And the coverage covered almost all the code with a 100% of coverage like the figure below.

We did not deployed our code on the cloud.



# Assignments Repository

In this repository, you'll work on your homework assignments and submit your completed assignments for review by the TAs.

### Please remember:
1. Your Source Code should be located under src folder.
2. Update the checklist document under docs folder
3. In this ReadMe, add assumptions, screenshots, comments, and other requirements
4. Delete all unnecessary source code or test files under src or test folders.
