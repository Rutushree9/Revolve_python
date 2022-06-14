import argparse
import pandas as pd
from glob2 import glob

def get_params() -> dict:
    """
    Obtain the data locations using argument parser
    """
    # Logging
    print("\n\n Capturing the locations for the data")
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

def get_cust_data(params:dict) -> pd.DataFrame:
    """
    Extract Customer Data
    Parameters:
    params : Dictionary containing all the data addresses
    """
    # Logging
    print("\n\n Obtaining Customer Data:\n")
    try:
        return pd.read_csv(params["customers_location"])
    except FileNotFoundError:
        # Logging
        print("Customers data cannot be found. Please check the customer location.")

def get_product_data(params:dict) -> pd.DataFrame:
    """
    Extract Products Data
    Parameters:
    params : Dictionary containing all the data addresses
    """
    # Logging
    print("\n\n Obtaining Product Data:\n")
    try:
        return pd.read_csv(params["products_location"])
    except FileNotFoundError:
        # Logging
        print("Products data cannot be found. Please check the product location.")

def get_transaction_data(params:dict) -> pd.DataFrame:
    """
    Extract Transactions Data
    Parameters:
    params : Dictionary containing all the data addresses
    """
    # Logging
    print("\n\n Obtaining Transactions Data:\n")
    try:
        path_to_json = params["transactions_location"]

            # Gather all the JSON files
        dfList = []
        jsonFiles = glob(f'{path_to_json}/**/*.json') 

            # Logging
        print("\n\n Reading the JSON files")
        for jsonFile in jsonFiles:
                df = pd.read_json(jsonFile, lines = True)
                dfList.append(df)
                
        dfTrainingDF = pd.concat(dfList, axis=0)
        cust_id_data = pd.concat([pd.DataFrame(x) for x in dfTrainingDF['basket']], keys=dfTrainingDF['customer_id']).reset_index(level=1, drop=True).reset_index()
        purchase_date_data = pd.concat([pd.DataFrame(x) for x in dfTrainingDF['basket']], keys=dfTrainingDF['date_of_purchase']).reset_index(level=1, drop=True).reset_index()
        return cust_id_data.merge(purchase_date_data, on = ["product_id", "price"])
    except Exception as ex:
        # Logging
        print(f"Error in processing the transactional data: {ex}")

def merged_data(params:dict) -> pd.DataFrame:
    """
    Merge all the data to get the required outcome
    Parameters:
    params : Dictionary containing all the data addresses
    """
    try:
        # Logging
        print("\n Merging the data")
        customer_data = get_cust_data(params)
        print("\n customer_data: \n",customer_data.head() )
        product_data = get_product_data(params)
        print("\n product_data: \n",product_data.head() )
        transactions_data = get_transaction_data(params)
        print("\n transactions_data:\n",transactions_data.head() )
        cust_trans_data = customer_data.merge(transactions_data, on="customer_id")
        data = pd.DataFrame(cust_trans_data.merge(product_data, on = "product_id"))
        return data.groupby(["customer_id", 
                                "loyalty_score", "product_id", 
                                "product_category"])["date_of_purchase"].nunique().reset_index().rename(columns={"date_of_purchase": "purchase_count"})
    except Exception as ex:
        # Logging
        print(f"Error in processing the merged data: {ex}")

def main() -> None:
    """
    Data Pipeline function to obtain the required outcome.
    """
    params = get_params()
    final_data = merged_data(params)
    print("\n\n final_data:\n", final_data.head())
    final_data.to_csv(f'{params["output_location"]}//final_outcome.csv', index=False)

if __name__ == "__main__":
    main()
