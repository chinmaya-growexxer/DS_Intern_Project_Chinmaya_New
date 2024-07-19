import streamlit as st
import joblib
import pandas as pd

# Load the preprocessing steps and the model
label_encoders = joblib.load('label_encoder.pkl')
minmax_scalers = joblib.load('minmax_scaler.pkl')
model = joblib.load('random_forest_model.pkl')

selected_features = ['STATE','GROUP_SIZE','HOMEOWNER','CAR_AGE','CAR_VALUE',
                     'MARRIED_COUPLE','AGE_YOUNGEST','C_PREVIOUS','DURATION_PREVIOUS', 'A' ,'B','C','D','E','F' ,'G']
categorical_columns = ['STATE', 'CAR_VALUE']
numerical_columns = ['CAR_AGE', 'AGE_YOUNGEST']

def preprocess_input(data):
    # Convert input data to DataFrame
    df = pd.DataFrame(data, index=[0])

    # Initialize a dictionary to hold preprocessed values
    preprocessed_data = {}

    # Convert categorical variables using label_encoders
    for col in categorical_columns:
        if col in data:
            preprocessed_data[col] = label_encoders[col].transform([data[col]])[0]

    df[numerical_columns] = minmax_scalers.transform(df[numerical_columns])

    # Create DataFrame from preprocessed_data
    df = pd.DataFrame([preprocessed_data])

    # Ensure all selected features are present, filling with zeros if missing
    for feature in selected_features:
        if feature not in df.columns:
            df[feature] = 0

    return df[selected_features]

# Streamlit UI
st.title('Car Insurance Policy Price Prediction App')

# Collect user inputs
#RECORD_TYPE = st.selectbox('Record Type', options=[0, 1], format_func=lambda x: "0: Shopping point" if x == 0 else "1: Purchase Point")
STATE = st.selectbox('State', label_encoders['STATE'].classes_)
GROUP_SIZE = st.number_input('Group Size', min_value=1,max_value =5, value=1)
HOMEOWNER = st.selectbox('Homeowner', options=[0, 1], format_func=lambda x: "0: No" if x == 0 else "1: Yes")
CAR_AGE = st.number_input('Car Age', min_value=0, max_value=100, value=0)
CAR_VALUE = st.selectbox('Car Value(least to most)', label_encoders['CAR_VALUE'].classes_)
#RISK_FACTOR = st.selectbox('Risk Factor(least to most)', options=[1, 2, 3, 4], format_func=lambda x: f"{x}: Level {x-1}")
MARRIED_COUPLE = st.selectbox('Married Couple', options=[0, 1], format_func=lambda x: "0: No" if x == 0 else "1: Yes")
AGE_YOUNGEST = st.number_input('Age Youngest', min_value=0, max_value=100, value=0)
C_PREVIOUS = st.selectbox('Vehicle Type Previously (C_Prev):', options=[0, 1, 2, 3, 4], format_func=lambda x: ["0: Nothing","1: Economy", "2: Mid-sized", "3: Luxury", "4: High-performance"][x])
DURATION_PREVIOUS = st.number_input('Duration Previous', min_value=0, max_value=100, value=0)

# Add descriptions and examples for A to G
A = st.selectbox('A (Coverage Level):', options=[0, 1, 2], format_func=lambda x: ["0: Basic coverage", "1: Standard coverage", "2: Premium coverage"][x])
#B = st.selectbox('D (Usage Type):', options=[1, 2, 3], format_func=lambda x: ["1: Personal", "2: Business", "3: Commercial"][x-1])
B = st.selectbox('B (Risk Profile):', options=[0, 1], format_func=lambda x: ["0: Non-Smoker", "1: Smoker"][x])
C = st.selectbox('C (Vehicle Type):', options=[1, 2, 3, 4], format_func=lambda x: ["1: Economy", "2: Mid-sized", "3: Luxury", "4: High-performance"][x-1])
D = st.selectbox('D (Usage Type):', options=[1, 2, 3], format_func=lambda x: ["1: Personal", "2: Business", "3: Commercial"][x-1])
E = st.selectbox('E (Safety Features):', options=[0, 1], format_func=lambda x: ["0: Lacks safety features", "1: Equipped with safety features"][x])
F = st.selectbox('F (Driver Record):', options=[0, 1, 2, 3], format_func=lambda x: ["0: Clean record", "1: Minor violations", "2: History of accidents", "3: Severe violations"][x])
G = st.selectbox('G (Geographical Location):', options=[1, 2, 3, 4], format_func=lambda x: ["1: Urban", "2: Suburban", "3: Rural", "4: Hazardous"][x-1])

# Collect all inputs into a dictionary
input_data = {
    #'RECORD_TYPE': RECORD_TYPE,
    'STATE': STATE,
    'GROUP_SIZE': GROUP_SIZE,
    'HOMEOWNER': HOMEOWNER,
    'CAR_AGE': CAR_AGE,
    'CAR_VALUE': CAR_VALUE,
    #'RISK_FACTOR': RISK_FACTOR,
    'MARRIED_COUPLE': MARRIED_COUPLE,
    'AGE_YOUNGEST': AGE_YOUNGEST,
    'C_PREVIOUS': C_PREVIOUS,
    'DURATION_PREVIOUS': DURATION_PREVIOUS,
    'A': A,
    'B': B,
    'C': C,
    'D': D,
    'E': E,
    'F': F,
    'G': G
}

# Predict button
if st.button('Predict'):
    # Check for null values
    if any(pd.isnull(value) for value in input_data.values()):
        st.error('Please fill in all the values.')
    else:
        # Preprocess the input data
        preprocessed_data = preprocess_input(input_data)

        # Make prediction using the model
        prediction = model.predict(preprocessed_data)

        # Round prediction to 2 decimal places
        rounded_prediction = round(prediction[0], 2)

        # Display prediction
        st.success(f'The Predicted Cost of the Policy is {rounded_prediction}')