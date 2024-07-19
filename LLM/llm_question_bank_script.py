import pandas as pd

# Define the questions
questions = [
    {"question": "What is the average cost of insurance for customers in the state of IN?", "complexity": "Easy", "correct_sql_query": "SELECT AVG(cost) FROM insurance_data WHERE state = 'IN';"},
    {"question": "How many unique customers are in the dataset?", "complexity": "Easy", "correct_sql_query": "SELECT COUNT(DISTINCT customer_ID) FROM insurance_data;"},
    {"question": "What is the total number of records in the dataset?", "complexity": "Easy", "correct_sql_query": "SELECT COUNT(*) FROM insurance_data;"},
    {"question": "Find the minimum and maximum cost of insurance in the dataset.", "complexity": "Easy", "correct_sql_query": "SELECT MIN(cost), MAX(cost) FROM insurance_data;"},
    {"question": "List all distinct states present in the dataset.", "complexity": "Easy", "correct_sql_query": "SELECT DISTINCT state FROM insurance_data;"},
    {"question": "What is the average cost of insurance for homeowners?", "complexity": "Medium", "correct_sql_query": "SELECT AVG(cost) FROM insurance_data WHERE homeowner = 1;"},
    {"question": "How many customers have a car age of 10 years or more?", "complexity": "Medium", "correct_sql_query": "SELECT COUNT(DISTINCT customer_ID) FROM insurance_data WHERE car_age >= 10;"},
    {"question": "Find the most common group size among customers.", "complexity": "Medium", "correct_sql_query": "SELECT group_size, COUNT(*) as count FROM insurance_data GROUP BY group_size ORDER BY count DESC LIMIT 1;"},
    {"question": "What is the average cost of insurance for customers who have made previous claims (C_previous > 0)?", "complexity": "Medium", "correct_sql_query": "SELECT AVG(cost) FROM insurance_data WHERE C_previous > 0;"},
    {"question": "List the average cost of insurance for each state.", "complexity": "Medium", "correct_sql_query": "SELECT state, AVG(cost) as average_cost FROM insurance_data GROUP BY state;"},
    {"question": "Find the customers who have the highest insurance cost and list their details.", "complexity": "Hard", "correct_sql_query": "SELECT * FROM insurance_data WHERE cost = (SELECT MAX(cost) FROM insurance_data);"},
    {"question": "Calculate the average duration of previous insurance for each risk factor.", "complexity": "Hard", "correct_sql_query": "SELECT risk_factor, AVG(duration_previous) as avg_duration FROM insurance_data GROUP BY risk_factor;"},
    {"question": "Find the most common combination of car_age and car_value.", "complexity": "Hard", "correct_sql_query": "SELECT car_age, car_value, COUNT(*) as count FROM insurance_data GROUP BY car_age, car_value ORDER BY count DESC LIMIT 1;"},
    {"question": "Identify the top 3 states with the highest average insurance cost.", "complexity": "Hard", "correct_sql_query": "SELECT state, AVG(cost) as avg_cost FROM insurance_data GROUP BY state ORDER BY avg_cost DESC LIMIT 3;"},
    {"question": "Determine the percentage of customers who are homeowners and have a risk factor of 4.", "complexity": "Hard", "correct_sql_query": "SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM insurance_data)) as percentage FROM insurance_data WHERE homeowner = 1 AND risk_factor = 4;"}
]

# Convert the list of dictionaries to a DataFrame
question_bank = pd.DataFrame(questions)

# Add the 'generated_sql_query' column with empty values
question_bank['generated_sql_query'] = ""

# Define the new questions
new_questions = [
    {"question": "What is the number of records for customers from the state of NY?", "complexity": "Easy", "correct_sql_query": "SELECT COUNT(*) FROM insurance_data WHERE state = 'NY';"},
    {"question": "List all unique car values in the dataset.", "complexity": "Easy", "correct_sql_query": "SELECT DISTINCT car_value FROM insurance_data;"},
    {"question": "Find the total number of homeowners in the dataset.", "complexity": "Easy", "correct_sql_query": "SELECT COUNT(*) FROM insurance_data WHERE homeowner = 1;"},
    {"question": "What is the earliest and latest recorded time in the dataset?", "complexity": "Easy", "correct_sql_query": "SELECT MIN(time), MAX(time) FROM insurance_data;"},
    {"question": "How many customers have a risk factor of 3?", "complexity": "Easy", "correct_sql_query": "SELECT COUNT(DISTINCT customer_ID) FROM insurance_data WHERE risk_factor = 3;"},
    {"question": "Find the average cost for each combination of car age and car value.", "complexity": "Medium", "correct_sql_query": "SELECT car_age, car_value, AVG(cost) FROM insurance_data GROUP BY car_age, car_value;"},
    {"question": "List the total number of records for each record type.", "complexity": "Medium", "correct_sql_query": "SELECT record_type, COUNT(*) FROM insurance_data GROUP BY record_type;"},
    {"question": "Find the count of customers with group size greater than 2.", "complexity": "Medium", "correct_sql_query": "SELECT COUNT(DISTINCT customer_ID) FROM insurance_data WHERE group_size > 2;"},
    {"question": "Calculate the average age of the oldest person in each state.", "complexity": "Medium", "correct_sql_query": "SELECT state, AVG(age_oldest) FROM insurance_data GROUP BY state;"},
    {"question": "What is the total cost for customers with a car value of '2 g'?", "complexity": "Medium", "correct_sql_query": "SELECT SUM(cost) FROM insurance_data WHERE car_value = '2 g';"},
    {"question": "Determine the customer_ID with the highest total insurance cost.", "complexity": "Hard", "correct_sql_query": "SELECT customer_ID, SUM(cost) as total_cost FROM insurance_data GROUP BY customer_ID ORDER BY total_cost DESC LIMIT 1;"},
    {"question": "Calculate the average cost for each combination of state and car age.", "complexity": "Hard", "correct_sql_query": "SELECT state, car_age, AVG(cost) FROM insurance_data GROUP BY state, car_age;"},
    {"question": "List the average cost for customers grouped by their duration of previous insurance.", "complexity": "Hard", "correct_sql_query": "SELECT duration_previous, AVG(cost) FROM insurance_data GROUP BY duration_previous;"},
    {"question": "Identify the most common combination of state and group size.", "complexity": "Hard", "correct_sql_query": "SELECT state, group_size, COUNT(*) as count FROM insurance_data GROUP BY state, group_size ORDER BY count DESC LIMIT 1;"},
    {"question": "Find the average insurance cost for married couples with different risk factors.", "complexity": "Hard", "correct_sql_query": "SELECT risk_factor, AVG(cost) FROM insurance_data WHERE married_couple = 1 GROUP BY risk_factor;"}
]

# Convert the list of dictionaries to a DataFrame
new_question_bank = pd.DataFrame(new_questions)

# Add the 'generated_sql_query' column with empty values
new_question_bank['generated_sql_query'] = ""

# Append the new questions
combined_question_bank = pd.concat([question_bank, new_question_bank], ignore_index=True)

# Define the summary statistics questions
summary_statistics_questions = [
    {"question": "What is the average cost of insurance across all records?", "complexity": "Easy", "correct_sql_query": "SELECT AVG(cost) FROM insurance_data;"},
    {"question": "Find the maximum and minimum insurance cost.", "complexity": "Easy", "correct_sql_query": "SELECT MAX(cost), MIN(cost) FROM insurance_data;"},
    {"question": "Calculate the average, median, and standard deviation of car age.", "complexity": "Medium", "correct_sql_query": "SELECT AVG(car_age), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY car_age) AS median_car_age, STDDEV(car_age) FROM insurance_data;"},
    {"question": "What is the total number of distinct customers in the dataset?", "complexity": "Easy", "correct_sql_query": "SELECT COUNT(DISTINCT customer_ID) FROM insurance_data;"},
    {"question": "Find the most common state in the dataset.", "complexity": "Medium", "correct_sql_query": "SELECT state, COUNT(*) AS count FROM insurance_data GROUP BY state ORDER BY count DESC LIMIT 1;"}
]

# Convert the list of dictionaries to a DataFrame
summary_statistics_df = pd.DataFrame(summary_statistics_questions)

# Add the 'generated_sql_query' column with empty values
summary_statistics_df['generated_sql_query'] = ""

# Append the new summary statistics questions to the combined question bank
updated_question_bank = pd.concat([combined_question_bank, summary_statistics_df], ignore_index=True)


# Sort by complexity
sorted_question_bank = updated_question_bank.sort_values(by='complexity')

# Save the combined and sorted DataFrame to an Excel file
sorted_question_bank.to_csv('question_bank.xlsx', index=False)

print('Question BANK generated successfully!')