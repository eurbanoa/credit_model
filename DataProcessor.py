import modin.pandas as pd
from snowflake.snowpark import Session, functions as F
import snowflake.snowpark.modin.plugin
from config1 import connection_parameters, data_cleaning_config

class DataProcs:
    def __init__(self):
        """
        Initialize the DataProcessor by starting a Snowflake session and loading data.
        """
        # Start Snowflake session and load data
        self.session = self.__start_session__()
        self.raw_data = self.__load_data_from_snowflake__()

        # Placeholder for cleaned data
        self.cleaned_data = None

    def __start_session__(self):
        """Start a Snowflake session using parameters from config.py."""
        session = Session.builder.configs(connection_parameters).create()
        if session is not None:
            print("Connected to Snowflake")
            session.use_database("CREDIT_RISK")
            session.use_schema("PUBLIC")
            return session
        else:
            raise ConnectionError("Failed to establish a connection with Snowflake.")

    def __load_data_from_snowflake__(self):
        """Load data from Snowflake using Snowpark."""
        print("Loading data from Snowflake...")
        df = pd.read_snowflake("CREDIT_DATA")
        return df

    def __clean_header__(self):
        """Set the second row as header and drop the first row with placeholder headers."""
        print("Cleaning header row...")
        self.raw_data.columns = self.raw_data.iloc[0]  # Set the second row as header
        self.raw_data = self.raw_data.drop(0)          # Drop the original row with placeholder headers
        self.raw_data = self.raw_data.reset_index(drop=True)  # Reset index for a clean DataFrame

    # Cleaning methods

    def __change_name_id__(self):
        self.raw_data.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

    def __clean_OBS_DATE__(self):
        """Convert OBS_DATE to datetime format without additional formatting."""
        date_format = data_cleaning_config['date_format']
        self.raw_data['OBS_DATE'] = pd.to_datetime(self.raw_data['OBS_DATE'], format=date_format, errors='coerce')

    def __clean_marital__(self):
        admissible_categories = data_cleaning_config['admissible_categories']['marital']
        self.raw_data['marital'] = self.raw_data['marital'].where(self.raw_data['marital'].isin(admissible_categories), other=pd.NA)
        self.raw_data['marital'] = pd.Categorical(self.raw_data['marital'], categories=admissible_categories, ordered=False)

    def __clean_target__(self):  # Use the correct method name here
        admissible_categories_target = data_cleaning_config['admissible_categories']['target_var']
        self.raw_data['target_var'] = self.raw_data['target_var'].where(self.raw_data['target_var'].isin(admissible_categories_target), other=pd.NA)
        self.raw_data['target_var'] = pd.Categorical(self.raw_data['target_var'], categories=admissible_categories_target, ordered=False)

    def __clean_occup__(self):
        admissible_categ_occup = data_cleaning_config['admissible_categories']['occup']
        self.raw_data['occup'] = self.raw_data['occup'].where(self.raw_data['occup'].isin(admissible_categ_occup), other=pd.NA)
        self.raw_data['occup'] = pd.Categorical(self.raw_data['occup'], categories=admissible_categ_occup, ordered=False)

    def __clean_schufa__(self):
        self.raw_data['schufa'] = pd.to_numeric(self.raw_data['schufa'], errors='coerce').round(4)

    def __clean_income__(self):
        self.raw_data['income'] = pd.to_numeric(self.raw_data['income'], errors='coerce').round(4)

    def __clean_loan_amount__(self):
        self.raw_data['loan_amount'] = pd.to_numeric(self.raw_data['loan_amount'], errors='coerce').round(4)

    def __clean_term_length__(self):
        self.raw_data['term_length'] = pd.to_numeric(self.raw_data['term_length'], errors='coerce').round(4)

    def __clean_install_to_inc__(self):
        self.raw_data['install_to_inc'] = pd.to_numeric(self.raw_data['install_to_inc'], errors='coerce').round(4)

    def __clean_num_applic__(self):
        self.raw_data['num_applic'] = pd.to_numeric(self.raw_data['num_applic'], errors='coerce').fillna(0).astype(int)

    def __get_cleaned_data__(self):
        """Perform all data cleaning steps and return the cleaned data."""
        self.__clean_header__()  # First, clean the header
        self.__change_name_id__()
        self.__clean_OBS_DATE__()
        self.__clean_marital__()
        self.__clean_target__()  # Use the correct method name here
        self.__clean_occup__()
        self.__clean_schufa__()
        self.__clean_income__()
        self.__clean_loan_amount__()
        self.__clean_term_length__()
        self.__clean_install_to_inc__()
        self.__clean_num_applic__()
        self.cleaned_data = self.raw_data.copy()
        return self.cleaned_data

    # Imputation methods

    def __impute_delete__(self):
        """Delete all rows with missing values."""
        if self.cleaned_data is None:
            raise ValueError("Cleaned data is not available. Please call get_cleaned_data() first.")
        return self.cleaned_data.dropna()

    def __impute_median__(self):
        """Impute numerical variables with the median and categorical variables with the most frequent category."""
        for column in self.cleaned_data.columns:
            if self.cleaned_data[column].dtype in ['float64', 'int64']:  # Check if the column is numeric
                median_value = self.cleaned_data[column].median()
                self.cleaned_data[column].fillna(median_value, inplace=True)
            else:
                mode_value = self.cleaned_data[column].value_counts().idxmax() if not self.cleaned_data[column].value_counts().empty else None
                if mode_value is not None:
                    self.cleaned_data[column].fillna(mode_value, inplace=True)
        return self.cleaned_data

    #def __create_dummies__(self):
        """Create dummy variables for 'occup' and 'marital' columns using Snowpark SQL."""
        print("Creating dummy variables using Snowpark SQL...")
        
        # Ensure self.cleaned_data is a regular Pandas DataFrame
        pandas_df = self.cleaned_data.to_pandas() if hasattr(self.cleaned_data, 'to_pandas') else self.cleaned_data

        # Convert to a Snowpark DataFrame
        snowpark_df = self.session.create_dataframe(pandas_df)

        # Add dummy columns for 'occup' categories
        for category in data_cleaning_config['admissible_categories']['occup']:
            snowpark_df = snowpark_df.with_column(f'occup_{category}', F.when(F.col('occup') == category, 1).otherwise(0))

        # Add dummy columns for 'marital' categories
        for category in data_cleaning_config['admissible_categories']['marital']:
            snowpark_df = snowpark_df.with_column(f'marital_{category}', F.when(F.col('marital') == category, 1).otherwise(0))

        # Drop original 'occup' and 'marital' columns
        snowpark_df = snowpark_df.drop('occup', 'marital')
        
        # Convert back to Modin DataFrame if necessary
        self.cleaned_data = snowpark_df.to_pandas()

    def __convert_target_var__(self):
        """Convert 'target_var' to numeric."""
        self.cleaned_data['target_var'] = self.cleaned_data['target_var'].astype(int)

    def __convert_OBS_DATE__(self):
        """Convert 'OBS_DATE' to a string format to avoid Snowflake TIMESTAMP conflicts."""
        # Set the OBS_DATE column to string to avoid TIMESTAMP issues in Snowflake
        self.cleaned_data['OBS_DATE'] = pd.to_datetime(self.cleaned_data['OBS_DATE'], errors='coerce').astype(str)


    def master(self, clean: bool, impute_method: str):
        """
        Perform cleaning and imputation based on the provided arguments.
        :param clean: Boolean indicating whether to clean the data.
        :param impute_method: String indicating the imputation method ('delete' or 'median').
        :return: DataFrame after cleaning and imputing.
        """
        if clean:
            self.__get_cleaned_data__()

        self.__clean_target__()  # Use the correct method name here

        if impute_method == 'delete':
            self.cleaned_data = self.__impute_delete__()
        elif impute_method == 'median':
            self.cleaned_data = self.__impute_median__()
        else:
            raise ValueError("Invalid imputation method. Use 'delete' or 'median'.")

        #self.__create_dummies__()
        self.__convert_target_var__()
        self.__convert_OBS_DATE__()

        return self.cleaned_data
