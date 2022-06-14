
import python_assignment_final.solution.solution_start as soln

PARAMS = {'customers_location': './input_data/starter/customers.csv', 
'products_location': './input_data/starter/products.csv', 
'transactions_location': './input_data/starter/transactions/', 
'output_location': './output_data/outputs/'}


def test_customers_data():
    """
    Testing get_cust_data methods
    """
    customer_data = soln.get_cust_data(PARAMS)
    assert all(customer_data.columns == ["customer_id", "loyalty_score"]) and (len(customer_data)> 0)

def test_product_data():
    """
    Testing get_product_data methods
    """
    product_data = soln.get_product_data(PARAMS)
    assert all(product_data.columns == ["product_id", "product_description", "product_category"]) and (len(product_data)>0)

def test_transactions_data():
    """
    Testing get_transaction_data methods
    """
    transactions_data = soln.get_transaction_data(PARAMS)
    assert all(transactions_data.columns == ["customer_id", "product_id", "price", "date_of_purchase"]) and (len(transactions_data)> 0)

def test_merged():
    """
    Testing merged_data methods
    """
    data = soln.merged_data(PARAMS)
    assert all(data.columns == ["customer_id", "loyalty_score", 'product_id', "product_category", "purchase_count"]) and (len(data)> 0)