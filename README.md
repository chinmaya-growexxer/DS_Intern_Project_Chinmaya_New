# DS_Intern_Project_Chinmaya


### Dataset Description for Car Insurance Policy Price Prediction
##Overview:

The dataset contains transaction histories of customers who purchased insurance policies. Each customer's entire quote history is recorded, with the last row indicating the purchased coverage options.
##Key Concepts:

    - Customer: May represent multiple individuals as policies can cover more than one person. Each customer has multiple shopping points (instances where they view products with specific characteristics and costs).
    - Shopping Point: Defined by a customer's interaction with a product at a specific time. Characteristics and product costs may change over time.
    Product Options: Each product has 7 customizable options with 2-4 possible ordinal values.

##Variables:

    - customer_ID: Unique identifier for each customer.
    - shopping_pt: Unique identifier for the shopping point of a given customer.
    - record_type: Indicates whether the record is a shopping point (0) or purchase point (1).
    - day: Day of the week (0-6, 0=Monday).
    - time: Time of day (HH:MM:SS).
    - state: State where the shopping point occurred.
    - location: Location ID of the shopping point.
    - group_size: Number of people covered under the policy (1-4).
    - homeowner: Homeownership status (0=no, 1=yes).
    - car_age: Age of the customer’s car.
    - car_value: Value of the customer’s car when new.
    - risk_factor: Risk assessment of the customer (1-4).
    - age_oldest: Age of the oldest person in the customer's group.
    - age_youngest: Age of the youngest person in the customer's group.
    - married_couple: Indicates if the customer group contains a married couple (0=no, 1=yes).
    - C_previous: Previous product option C/ Type of insured vehicle the customer has previously (0=nothing, 1=Economy, 2=Mid-sized, 3=Luxury, 4=High-performance).
    - duration_previous: Duration (in years) the customer was covered by their previous insurer.

##Coverage Options:

    -A: Insurance coverage/risk profile (0=Basic, 1=Standard, 2=Premium).
    -B: Binary policyholder attribute (0=Non-smoker, 1=Smoker).
    -C: Type of insured vehicle (1=Economy, 2=Mid-sized, 3=Luxury, 4=High-performance).
    -D: Usage/purpose of the vehicle (1=Personal, 2=Business, 3=Commercial).
    -E: Vehicle safety features (0=No, 1=Yes).
    -F: Driver's record/history (0=Clean, 1=Minor violations, 2=Accidents, 3=Severe violations).
    -G: Geographical location/risk zone (1=Urban, 2=Suburban, 3=Rural, 4=Hazardous).

##Target Variable:

    - cost: Cost of the quoted coverage options.

This dataset is used to predict the price of car insurance policies based on customer characteristics, product options, and interaction history.
