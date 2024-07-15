import requests
import json
import pandas as pd

data = {}
lst = []  # for storing data.


# Please see what is printed from beginning for example The most expensive is:  Annibale Colombo Sofa
# price for sofa is - Most expensive price is:  2036.4918539999999

def get_actual_data():
    # Only 30 products is in api get from dummyjson file
    global data
    response = requests.get("https://dummyjson.com/products")  # please open that in browser only 30 is visible
    data = response.json()


def most_expensive():
    global data
    disc = data["products"]

    for i in range(len(disc)):

        price = 0  # price
        discount = 0  # discount
        multiple_price_discount = 0
        global lst
        for j in range(len(disc)):

            if disc[j]["discountPercentage"]:
                discount = disc[j]["discountPercentage"]
            if disc[j]["price"]:
                price = disc[j]["price"]
            multiple_price_discount = price - (discount * price) / 100  # formula for calculate price
            lst.append(multiple_price_discount)  # import calculated price in list with name lst

    print("Most expensive price is: ", max(lst))
    max_in_list = max(lst)
    index_of_max_in_list = lst.index(max_in_list)
    print("Index of most expensive is: ", index_of_max_in_list)
    most_expensive_product = disc[index_of_max_in_list]["title"]
    print("The most expensive is: ", most_expensive_product)


def missed_non_missed():
    row = 0
    global data
    disc = data["products"]
    print("Reading from parquet file and print data from parquet file")
    df = pd.read_parquet(f'../python-assessment-task/data/product_prices_calculated.parquet')
    df.to_json(f'../python-assessment-task/data/dragan.json', orient='records')  # store as json here

    f = open('../python-assessment-task/data/dragan.json')  # store parquet as json in dragan.json

    dat = json.load(f)
    #  print parquet file
    print(dat)

    expected_list_of_title = []  # lists from storing data
    actual_list_of_title = []
    missed_expected_items = []
    non_missed_expected_items = {}

    for i in range(len(dat)):  # this is from parquet
        expected_list_of_title.append(dat[i]["title"])

    for k in range(len(disc)):  # this is from link
        actual_list_of_title.append(disc[k]["title"])

    for i in range(len(dat)):
        if dat[i]["title"] not in actual_list_of_title:
            missed_expected_items.append(dat[i]["title"])
            print("missed is:", dat[i]["title"])

    for i in range(len(disc)):
        for j in range(len(dat)):
            if disc[i]["title"] == dat[j]["title"]:
                # here I compared that difference from price from parquet and from calculated is <0.5
                # that asked in question about calculated and expected to compare
                if lst[i] - dat[j]["final_price"] < 0.5:
                    row = row + 1
                    print("same actual as expected -", dat[j]["title"], " ", "calculated with formula:", lst[i],
                          "final "
                          "price from"
                          " expected "
                          "parquet "
                          "file:",
                          dat[j]["final_price"])
                    print("Number of rows with same calculated as expected data: ", row)  # number of rows
    # Closing file
    f.close()


# assert response.status_code == 200
def main():
    print("Hello this is a task for Python!")

    get_actual_data()
    most_expensive()
    missed_non_missed()


if __name__ == "__main__":
    main()
