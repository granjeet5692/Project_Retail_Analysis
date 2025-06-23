import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state, filter_orders
from lib.ConfigReader import get_app_config

@pytest.mark.skip() # markers used to segregate the tests 
def test_read_customers_df(spark):
	customer_count = read_customers(spark, 'LOCAL').count()
	assert customer_count == 12435
	
@pytest.mark.skip()
def test_read_orders_df(spark):
	orders_count = read_orders(spark, 'LOCAL').count()
	assert orders_count == 68884

@pytest.mark.skip()
def test_filter_closed_orders(spark):
	orders_df = read_orders(spark, 'LOCAL')
	filetred_count = filter_closed_orders(orders_df).count()
	assert filetred_count == 7556

@pytest.mark.skip()
def test_read_app_config():
	config = get_app_config('LOCAL')
	assert config['orders.file.path'] == 'data/orders.csv'

@pytest.mark.skip()
def test_count_states(spark, expected_results):
	customers_df = read_customers(spark, 'LOCAL')
	actual_results = count_orders_state(customers_df)
	assert actual_results.collect() == expected_results.collect()

@pytest.mark.skip()
def test_check_closed_count(spark):
	orders_df = read_orders(spark, 'LOCAL')
	filetred_count = filter_orders(orders_df, 'CLOSED').count()
	assert filetred_count == 7556

@pytest.mark.skip()
def test_check_pp_count(spark):
	orders_df = read_orders(spark, 'LOCAL')
	filetred_count = filter_orders(orders_df, 'PENDING_PAYMENT').count()
	assert filetred_count == 15030

@pytest.mark.skip()
def test_check_complete_count(spark):
	orders_df = read_orders(spark, 'LOCAL')
	filetred_count = filter_orders(orders_df, 'COMPLETE').count()
	assert filetred_count == 22900

## define parametrized test cases
@pytest.mark.parametrize(
	"status,count",
	[('CLOSED', 7556), ('PENDING_PAYMENT', 15030), ('COMPLETE', 22900)	]
)	
def test_check_count(spark, status, count):
	orders_df = read_orders(spark, 'LOCAL')
	filetred_count = filter_orders(orders_df, status).count()
	assert filetred_count == count
	
	