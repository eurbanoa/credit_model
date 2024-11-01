# Snowflake Credentials
connection_parameters = {
    "account": "vw78242.south-central-us.azure",
    "user": "oweurbanoa",
    "password": "tAsLu1cAs!",
    "warehouse": "CREDIT_RISK_WH",
    "role": "ACCOUNTADMIN",
    "database": "CREDIT_RISK",
    "schema": "PUBLIC"
}

# Data Cleaning Configuration
data_cleaning_config = {
    "admissible_categories": {
        "marital": [
            "Single",
            "Divorced",
            "Separated",
            "Married",
            "Living together"
        ],
        "target_var": [
            "1",
            "0"
        ],
        "occup": [
            "Employee",
            "Worker",
            "Student"
        ]
    },
    "date_format": "%d%b%Y - %H:%M:%S",
    "default_fill_values": {
        "numeric": 0,
        "categorical": "Unknown"
    },
    "target": "target_var",
    "features": [
        "income",
        "loan_amount",
        "term_length",
        "install_to_inc",
        "schufa",
        "num_applic",
        "occup_Worker",
        "occup_Student",
        "marital_Divorced",
        "marital_Separated",
        "marital_Married",
        "marital_Living together"
    ],
    "cutoff": 0.5
}




