import pandas as pd
import json
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

def create_customers_data(ti, csv_path):
    customers = []
    for i in range(1, 50):
        customer = {}
        customer["customer_id"] = i
        customer["first_name"] = fake.first_name()
        customer["last_name"] = fake.last_name()
        customer["address"] = fake.address().replace(",", "").replace("\n", " ")
        customer["email"] = fake.email()
        customers.append(customer)
    customers_df = pd.DataFrame(customers)
    ti.xcom_push(key="no_customers", value={"no_cus":len(customers_df)})
    customers_df.to_csv(f"{csv_path}/customers.csv", index=False)

def create_items_data(csv_path):
    item_ids = list(range(1, 11))
    item_names = ["grocery", "fruit", "cereal", "electronic", "gadget", "kitcheneries", "office appliances", "sports", "cosmestics", "foodstuff"]
    unit_price = [10, 22.5, 30, 100, 85, 12.8, 45, 35.5, 65.7, 23]
    items_dict = {"item_id": item_ids, "item_name": item_names, "unit_price": unit_price}
    items_df = pd.DataFrame(items_dict)
    items_df.to_csv(f"{csv_path}/items.csv", index=False)

def create_dateTable(num_ord, md_path, csv_path):
    with open(md_path, "r") as file:
        last_ord_id = json.load(file)["last_order_id"]
    
    start = datetime.now().date()
    end = start + timedelta(1)
    dates = []
    for i in range(last_ord_id+1, last_ord_id+num_ord+1):
        date = {}
        date["date_id"] = i
        date["date"] = fake.date_time_between(start_date=start, end_date=end)
        date["year"] = date.get("date").year
        date["month"] = date.get("date").month
        date["day"] = date.get("date").day
        date["time"] = date.get("date").time()
        dates.append(date)
    dates_df = pd.DataFrame(dates)
    dates_df.to_csv(f"{csv_path}/datetable.csv", index=False)

def create_orders_data(ti, num_ord, md_path, csv_path, items_csv):
    with open(md_path, "r") as file:
        last_ord_id = json.load(file)["last_order_id"]
    no_cus = ti.xcom_pull(key="no_customers", task_ids="generate_customers_data")["no_cus"]
    items = pd.read_csv(items_csv)
    items_dict = items.set_index('item_id').to_dict()
    orders = []
    for i in range(last_ord_id+1, last_ord_id+num_ord+1):
        order = {}
        order["order_id"] = i
        order["date_id"] = fake.random_int(min=last_ord_id+1, max=last_ord_id+num_ord, step=1)
        order["customer_id"] = fake.random_int(min=1, max=no_cus, step=1)
        order["item_id"] = fake.random_int(min=1, max=len(items), step=1)
        order["quantity"] = fake.random_int(min=1, max=10, step=1)
        order["amount"] = items_dict.get("unit_price").get(order["item_id"]) * order["quantity"]
        orders.append(order)
    with open(md_path, "w") as file:
        json.dump({"last_order_id": i}, file)
    orders_df = pd.DataFrame(orders)
    orders_df.to_csv(f"{csv_path}/orders.csv", index=False)