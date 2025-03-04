import pandas as pd 
import random 
from faker import Faker 
from datetime import datetime, timedelta

#  ********* generate dummy data tbl_branch *********
def generate_dummy_data():
    fake = Faker()
    num_branch_records = 50 # num branch 

    branch_data = {
        "id": [i + 1 for i in range(num_branch_records)],
        "name": [fake.company() for _ in range(num_branch_records)],
        "location": [fake.city() for _ in range(num_branch_records)],
        "address": [fake.address() for _ in range(num_branch_records)],
        "email": [fake.email() for _ in range(num_branch_records)],
        "phone": [fake.phone_number() for _ in range(num_branch_records)],
        "created_time": [fake.date_time_this_decade() for _ in range(num_branch_records)],
        "modified_time": [fake.date_time_this_decade() for _ in range(num_branch_records)]
    }

    df_branch = pd.DataFrame(branch_data)
    df_branch.to_csv("df_branch.csv", index=False)

    #  ********* generate dummy data tbl_employee *********
    employee_data = []
    global_employee_id = 1 

    branch_data = pd.read_csv("df_branch.csv")

    for branch_id in branch_data["id"]:
        employee_list = []
        total_employees = random.randint(5,10) # generate random emp 5-10 
        active_count = 0 

        # loop for create information for each employee
        for _ in range(total_employees):
            created_at = fake.date_time_this_decade() # create random created_at
        #    modified_at = created_at + timedelta(days=random.randint(0,30)) # ensure modified_at more than created_at 

            # ensure active_status should be more than 3 
            if active_count < 3: 
                active_status = True 
                active_count += 1 
            else:
                active_status = random.choice([True, False])
                if active_status:
                    active_count +=1
            
            employee_list.append({
                "id": global_employee_id,
                "branch_id": branch_id,
                "name": fake.name(),
                "salary": round(random.uniform(40000000, 8000000),2),
                "active": active_status,
                "address": fake.address(),
                "email": fake.email(),
                "created_at": fake.date_time_this_decade(),   
            })
            global_employee_id +=1
        
        employee_data.extend(employee_list)

    df_employee = pd.DataFrame(employee_data)
    df_employee.to_csv("df_employee.csv", index=False)

            
    #  ********* generate dummy data tbl_schedule_employee *********

    def get_day_of_week(date):
        return date.strftime('%A') # get name from date
    employee_data = pd.read_csv("df_employee.csv")

    schedule_data = []
    global_schedule_id = 1 
    #start_date = datetime(datetime.now().year, 1,1)
    start_date = datetime(2024, 1, 1)
    end_date =  datetime(2024,1,1)
    delta = end_date - start_date

    for branch_id in branch_data["id"]:
        # create schedulle only for employee active 
        active_employees = [
        emp.to_dict() for _, emp in employee_data.iterrows()
        if emp["branch_id"] == branch_id and emp["active"]
        ]
        if len(active_employees)< 3:
            continue 

        for day in range(delta.days + 1):
            current_date = start_date + timedelta(days = day)

            # get day from date 
            day_of_week = get_day_of_week(current_date)

            if day_of_week in ["Saturday", "Sunday"]:
                num_employees = min(2, len(active_employees))
                shifts = [(10,15), (15,20)]
                shifts = [(7,12), (12,17), (17,22)]
            else:
                num_employees = len(active_employees)
                shifts = [(7,12), (12,17), (17,22)]
            
            # distribusi shift employee
            shift_capacities = [num_employees // len(shifts)] * len(shifts)
            for i in range(num_employees % len(shifts)):
                shift_capacities[i] +=1
            
            employees_left = active_employees[:]
            shift_assignments = []

            for i, (shift_start, shift_end)  in enumerate(shifts):
                if shift_capacities[i] > 0:
                    assigned_employees = random.sample(employees_left, shift_capacities[i]) # get random employee for shift
                    shift_assignments.append((assigned_employees, f"{shift_start}:00 - {shift_end}:00"))
                    employees_left = [emp for emp in employees_left if emp not in assigned_employees] # delete assign employee from list employee
            
            for assigned_employees, shift_time in shift_assignments:
                for emp in assigned_employees:
                    schedule_data.append({
                        "id": global_schedule_id,
                        "branch_id": branch_id,
                        "employee_id": emp["id"],
                        "day": day_of_week,
                        "date": current_date.date(),
                        "jam_shift": shift_time,
                        "created_time": fake.date_time_this_decade(),
                    })
                    global_schedule_id +=1

    df_schedule = pd.DataFrame(schedule_data)
    df_schedule.to_csv("df_schedule.csv", index=False)

    #  ********* generate dummy data tbl_product *********

    data_product = [
        (1, "iPhone 15 Pro", "Electronics", "Smartphones", 15000000, 5, 10),
        (2, "Samsung Galaxy S23", "Electronics", "Smartphones", 20000000, 5, 5),
        (3, "MacBook Pro M3", "Electronics", "Laptops", 22000000, 5, 20),
        (4, "Asus Zenbook Flip", "Electronics", "Laptops", 11000000, 5, 20),
        (5, "Asus ROG Zephyrus G14", "Electronics", "Laptops", 15000000, 5, 30),
        (6, "JBL Flip 6", "Electronics", "Speakers", 3000000, 10, 100),
        (7, "Logitech MX Master 3", "Electronics", "Accessories", 1700000, 20, 200),
        (8, "Air Jordan 1 Retro High Dior", "Apparel", "Footwear", 125000000, 2, 5),
        (9, "Adidas Ultraboost", "Apparel", "Footwear", 3000000, 10, 20),
        (10, "versace chain contour", "Apparel", "Clothing", 2700000, 10, 20),
        (11, "Rolex Submariner", "Fashion", "Watches", 150000000, 2, 5),
        (12, "Fossil Leather Wallet", "Fashion", "Accessories", 1000000, 5, 10),
        (13, "Samsung 65\" Smart TV", "Home Appliances", "Televisions", 10000000, 5, 40),
        (14, "Dyson V15 Vacuum", "Home Appliances", "Cleaning Devices", 2000000, 10, 200),
        (15, "Philips Air Fryer", "Home Appliances", "Kitchen Appliances", 700000, 10, 200),
    ]

    created_time = datetime.today().strftime('%Y-%m-%d')

    df_product = pd.DataFrame(data_product, columns=["id", "product_name", "category", "sub_category", "price", "profit", "stock"])
    df_product["created_time"] = created_time

    df_product["modified_time"] = None
    df_product.to_csv("df_product.csv", index=False)


    #  ********* generate dummy data tbl_promotions *********

    special_events = {
        "2025-01-01": "New Year",
        "2025-02-10": "Chinese New Year",
        "2025-02-14": "Valentine's Day",
        "2025-03-08": "International Women's Day",
        "2025-03-11": "Nyepi Day",
        "2025-03-17": "St. Patrick's Day",
        "2025-03-29": "Good Friday",
        "2025-04-01": "April Fool's Day",
        "2025-04-10": "Eid al-Fitr",
        "2025-04-11": "Eid al-Fitr Holiday",
        "2025-04-21": "Kartini Day",
        "2025-05-01": "Labor Day",
        "2025-05-09": "Ascension Day of Jesus Christ",
        "2025-06-01": "Pancasila Day",
        "2025-06-17": "Eid al-Adha",
        "2025-07-04": "US Independence Day",
        "2025-07-07": "Islamic New Year",
        "2025-08-17": "Independence Day",
        "2025-09-16": "Prophet Muhammad’s Birthday",
        "2025-10-31": "Halloween",
        "2025-11-10": "Heroes Day",
        "2025-11-29": "Black Friday",
        "2025-12-24": "Christmas Eve",
        "2025-12-25": "Christmas Day",
        "2025-12-26": "Boxing Day",
        "2025-12-31": "New Year's Eve"
    }

    current_year = datetime.now().year
    # add event setiap akhir bulan 
    for month in range(1,13):
        last_day = datetime(current_year, month, 1) + timedelta(days=32)
        last_day = last_day.replace(day=1) - timedelta(days=1)
        special_events[last_day.strftime("%Y-%m-%d")] = f"End of {last_day.strftime('%B')} Sale"

    promotion_data = []
    promo_id = 1 

    for date, event in sorted(special_events.items()):
        promotion_data.append({
            "id": promo_id,
            "event_name": f"{event} ({date})",
            "disc": random.randint(5,10),
            "time": date,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        promo_id +=1

    df_promotions = pd.DataFrame(promotion_data)
    df_promotions.to_csv("df_promotions.csv", index=False)

    #  ********* generate dummy data tbl_customers *********

    num_records_cust = 100

    data_cust = []
    for i in range(1, num_records_cust + 1):
        name = fake.name()
        address = fake.address().replace("\n", ", ")
        phone = fake.phone_number()
        email = fake.email()
        created_at = fake.date_time_between(start_date="-2y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
        modified_at = (datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S") + timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d %H:%M:%S")
        data_cust.append([i, name, address, phone, email, created_at, modified_at])

    df_cust = pd.DataFrame(data_cust, columns=["id", "name", "address", "phone", "email", "created_at", "modified_at",])
    df_cust.to_csv("df_cust.csv", index=False)

    #  ********* generate dummy data tbl_payment_method, tbl_order_status, tbl_payment_status, & tbl_shipping_status  *********

    data_payment_method = {
        (1, "Cash Payment"),
        (2, "Bank Transfer"),
        (3, "Credit Card"),
        (4, "Virtual Account")
    }
    data_order_status = {
        (1, "ongoing"),
        (2, "done")
    }
    data_payment_status = {
        (1, "processing"),
        (2, "paid"),
        (3, "failed")
    }
    data_shipping_status = {
        (1, "ongoing"),
        (2, "done"),
        (3, "failed")
    }


    created_time = datetime.today().strftime('%Y-%m-%d')
    df_payment_method = pd.DataFrame(data_payment_method, columns = ["id", "name"])
    df_order_status = pd.DataFrame(data_order_status, columns=["id", "name"])
    df_payment_status = pd.DataFrame(data_payment_status, columns=["id", "name"])
    df_shipping_status = pd.DataFrame(data_shipping_status, columns=["id", "name"])

    df_payment_method["created_time"] = created_time
    df_order_status["created_time"] = created_time
    df_payment_status["created_time"] = created_time
    df_shipping_status["created_time"] = created_time

    df_payment_method.to_csv("df_payment_method.csv", index=False)
    df_order_status.to_csv("df_order_status.csv", index=False)
    df_payment_status.to_csv("df_payment_status.csv", index=False)
    df_shipping_status.to_csv("df_shipping_status.csv", index=False)


    #  ********* generate dummy data tbl_sales *********

    customer_ids = df_cust["id"].tolist()
    product_ids = df_product["id"].tolist()
    branch_ids = df_branch["id"].tolist()

    transactions_per_month = random.randint(500, 1000)

    order_status_options = [1, 2]
    order_status_weghts = [0.1, 0.9]

    payment_status_options = [1, 2, 3]
    payment_status_weights = [0.05, 0.9, 0.05]

    payment_method_options = [1, 2, 3, 4]

    shipping_status_options = [1, 2, 3]
    shipping_status_weights = [0.05, 0.9, 0.05]

    sales_data = []
    sale_id = 1

    def get_random_order_time(date):
        if date.weekday() < 5:  # Weekday (Monday-Friday)
            hour = random.randint(7, 22)
        else:  # Weekend (Saturday-Sunday)
            hour = random.randint(10, 20)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return datetime(date.year, date.month, date.day, hour, minute, second)

    for month in range(1, 13):
        start_date = datetime(2025, month, 1)
        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

        for _ in range(transactions_per_month):
            product_id = random.choice(product_ids)
            customer_id = random.choice(customer_ids)
            branch_id = random.choice(branch_ids)
            quantity = random.randint(1,10)
            payment_method = random.choice(payment_method_options)
            is_online_transaction = random.choice([True, False])
            delivery_fee = 0
            is_free_delivery_fee = None 

            if is_online_transaction: 
                delivery_fee = random.randint(12000, 50000)
                is_free_delivery_fee = random.choice([True, False])

            random_date = fake.date_between(start_date=start_date, end_date=end_date)
            order_date = get_random_order_time(random_date).strftime("%Y-%m-%d %H:%M:%S")
            
            order_status = random.choices(order_status_options, weights=order_status_weghts, k=1)[0]
            payment_status = None
            shipping_status = None

            if order_status == 2:
                payment_status = random.choices(payment_status_options, weights=payment_status_weights, k=1)[0]
                if payment_status == 2:
                    shipping_status = random.choices(shipping_status_options, weights=shipping_status_weights, k=1)[0]
        
            if not is_online_transaction:
                shipping_status = None 
            
            created_at = order_date
            modified_at = (datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S") + timedelta(days=random.randint(1,30))).strftime("%Y-%m-%d %H:%M:%S")

            sales_data.append([sale_id, product_id, customer_id, branch_id, quantity, payment_method, order_date,
                            order_status, payment_status, shipping_status, is_online_transaction, delivery_fee, is_free_delivery_fee, created_at, modified_at])
            sale_id +=1

    df_sales = pd.DataFrame(sales_data, columns=["id", "product_id", "customer_id", "branch_id", "quantity", "payment_method", "order_date",
                                                "order_status", "payment_status", "shipping_status","is_online_transaction", "delivery_fee", "is_free_delivery_fee", "created_at", "modified_at"])

    df_sales.to_csv("df_sales.csv", index=False)


