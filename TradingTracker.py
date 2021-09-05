import json
import pandas as pd
from pandas import json_normalize 

from esipy import EsiClient
from esipy import EsiApp

import requests

from multiprocessing import Pool

import time
start_time = time.time()

# User set constants, all are per order
MAX_VOLUME = 60 * 1000 # 60 DST # 19 Iteron
MIN_PROFIT_PER_M3 = 2000
TAXSKILL = .95
MAXCOST = 500 * 1000000
MIN_NET_PROFIT = 5 * 1000000
NUM_SELL_ORDERS_TO_LOOK_AT = 3
MIN_ROI = 25.0
IGNORED_IDS = []

region_ids = json.load(open('region_ids.json'))
region_ids_df = pd.DataFrame(region_ids['regions'])

with open('item_volumes.json') as item_volumes_file:
    item_volumes = json.load(item_volumes_file)

def region_thread(argument_list):
    current_r_name = argument_list[0]
    region_orders_df = argument_list[1]
    forgedf = argument_list[2]
    print(current_r_name + ' region thread started')
    result_df = pd.DataFrame(columns = ['Net Profit',
                                        'Item Name',
                                        'Item Id',
                                        'Region',
                                        'System',
                                        'Profit Per',
                                        'Profit Per m3',
                                        'ROI',
                                        'Net Cost',
                                        'Max Quantity',
                                        'Top Buy Order Quantity',
                                        'Volume Per',
                                        'Total Volume',
                                        'Jita Buy Order Price',
                                        'Region Sell Order Price'
                                        ])

    # iterate through region orders
    for current_order_index, current_order_row in region_orders_df.iterrows():
        item_id = current_order_row['type_id']

        if current_order_row['type_id'] in IGNORED_IDS:
            continue

        buyorders = forgedf[forgedf['type_id'] == item_id]
        if buyorders.empty:
            continue

        # Set vars
        quantity_remaining = current_order_row['volume_remain']
        buyorders = buyorders.sort_values(by=['price'], ascending=False)
        max_buy_order = buyorders.iloc[0]

        if max_buy_order['volume_remain'] >= quantity_remaining:
            untaxed_buy_order_price = max_buy_order['price']
        else:
            quantity_calculated = 0
            average_add = 0
            average_quant = 0
            for buyorder_index, buyorder in buyorders.iterrows():
                if quantity_calculated + buyorder['volume_remain'] >= quantity_remaining:
                    num_needed = (quantity_remaining - quantity_calculated)
                    average_add = average_add + (num_needed * buyorder['price'])
                    break
                quantity_calculated = quantity_calculated + buyorder['volume_remain']
                average_add = average_add + (buyorder['volume_remain'] * buyorder['price'])
            untaxed_buy_order_price = average_add / quantity_remaining

        taxed_buy_order_price = untaxed_buy_order_price * TAXSKILL
        sell_order_price = current_order_row['price']
        current_region_name = current_r_name
        roi = ((taxed_buy_order_price / sell_order_price) * 100.0) - 100.0
        profit_per_item = taxed_buy_order_price - sell_order_price

        # CHECK: ROI
        if roi < float(MIN_ROI):
            continue

        # CHECK: NPC ORDER
        if current_order_row['duration'] > 90:
            continue
            
        # CHECK: NET PROFIT
        # Initial net profit check to reduce time spent getting volume per item
        if quantity_remaining * profit_per_item < MIN_NET_PROFIT:
            continue
        # Get volume per item and profit per m3
        volume_per_item = 0
        for volume_tuple in item_volumes:
            if volume_tuple[0] == item_id:
                volume_per_item = volume_tuple[1]
                continue
        if volume_per_item == 0:
            reqstr = 'http://esi.evetech.net/latest/universe/types/' + str(item_id) + '/'
            response = requests.get(reqstr)
            if not response.status_code == requests.codes.ok:
                print('bad volume request ' + response.status_code)
                continue
            typedatadf = json_normalize(response.json())
            volume_per_item = typedatadf['volume'].iloc[0]
            item_volumes.append((item_id,volume_per_item))
        profit_per_m3 = profit_per_item / volume_per_item
            
        # CHECK: PROFIT PER M3
        if profit_per_m3 < MIN_PROFIT_PER_M3:
            continue

        # CHECK: QUANTITY FITTABLE
        if volume_per_item * quantity_remaining > MAX_VOLUME:
            max_quantity = int(MAX_VOLUME / volume_per_item)
        else:
            max_quantity = quantity_remaining
        # Update net profit/cost if whole order cant be fit
        net_profit = (taxed_buy_order_price - sell_order_price) * max_quantity
        net_cost = max_quantity * sell_order_price

        # CHECK: NET COST
        if net_cost > MAXCOST: # this could be improved
            continue

        # CHECK: NET PROFIT
        if net_profit < MIN_NET_PROFIT:
            continue

        print('match found') # console debugging

        # Get item name
        rdata = '[' + str(item_id) + ']'
        response = requests.post('https://esi.evetech.net/latest/universe/names/', data=rdata)
        item_name = ""
        if response.status_code == requests.codes.ok:
            typeiddf = json_normalize(response.json())
            item_name = typeiddf['name'].iloc[0]

        # Get system name
        reqstr = 'http://esi.evetech.net/latest/universe/systems/' + str(current_order_row['system_id']) + '/'
        response = requests.get(reqstr)
        system_name = ""
        if response.status_code == requests.codes.ok:
            systemdatadf = json_normalize(response.json())
            system_name = systemdatadf['name'].iloc[0]
        else:
            print('bad system name request ' + str(response.status_code))

        # Add result to df
        result_df = result_df.append({'Net Profit' : net_profit,
                                        'Item Name' : item_name,
                                        'Item Id' : item_id,
                                        'Region' : current_region_name,
                                        'System' : system_name,
                                        'Profit Per' : profit_per_item,
                                        'Profit Per m3' : profit_per_m3,
                                        'ROI' : roi,
                                        'Net Cost' : net_cost,
                                        'Max Quantity' : max_quantity,
                                        'Top Buy Order Quantity' : max_buy_order['volume_remain'],
                                        'Volume Per' : volume_per_item,
                                        'Total Volume' : volume_per_item * max_quantity,
                                        'Jita Buy Order Price' : untaxed_buy_order_price,
                                        'Region Sell Order Price' : sell_order_price,
                                        }, ignore_index = True)
        
    print(current_r_name + ' region thread ended')
    return result_df

def master():
    resultsfile = open('results.txt', 'w')

    esi_app = EsiApp()
    app = esi_app.get_latest_swagger

    client = EsiClient(
        retry_requests=True,
        headers={'User-Agent': 'Just forthelolz'},
        raw_body_only=True,
    )

    # GET ALL FORGE BUY ORDERS

    # get first page
    forge_order_op = app.op['get_markets_region_id_orders'](
        region_id=10000002,
        order_type='buy'
    )
    response = client.request(forge_order_op)
    forgedf = pd.DataFrame(json.loads(response.raw))

    operations = []

    # get following pages
    for i in range(response.header['X-Pages'][0] - 1):
        operations.append(
            app.op['get_markets_region_id_orders'](
                region_id=10000002,
                order_type='buy',
                page=i+2
            )
        )

    responses = client.multi_request(operations)
    for response in responses:
        forgedf2 = pd.DataFrame(json.loads(response[1].raw))
        forgedf = forgedf.append(forgedf2)

    print("forge " + str(len(forgedf.index)))

    # ITERATE THROUGH REGIONS

    argument_list_list = []
    results = []
    test = 0
    for region_ids_index, row in region_ids_df.iterrows():
        # get first page
        market_order_op = app.op['get_markets_region_id_orders'](
            region_id=row['id'],
            order_type='sell'
        )
        response = client.request(market_order_op)
        region_orders_df = pd.DataFrame(json.loads(response.raw))

        # get following pages
        operations = []
        for i in range(response.header['X-Pages'][0] - 1):
            operations.append(
                app.op['get_markets_region_id_orders'](
                    region_id=row['id'],
                    order_type='sell',
                    page=i+2
                )
            )

        responses = client.multi_request(operations)
        for response in responses:
            regiondf2 = pd.DataFrame(json.loads(response[1].raw))
            region_orders_df = region_orders_df.append(regiondf2)

        region_orders_df = region_orders_df.sort_values(['price', 'type_id'])

        print()
        print()
        print(row['name'] + ': ' + str(len(region_orders_df.index)))
        print()
        print()

        argument_list_list.append((row['name'],region_orders_df,forgedf))

    #future_list.append(executor.submit(region_thread, argument_list))
    with Pool(processes=6) as pool:
        results = pool.map(region_thread, argument_list_list)

    for result_df in results:
        if result_df is None:
            continue

        region_name_written = False

        # Write to results file
        big_net_profit = 0
        big_net_cost = 0
        big_net_volume = 0
        result_df = result_df.sort_values(by=['Profit Per m3'], ascending=False)
        for result_data_index, result_data_row in result_df.iterrows():
            # Write a regions name at beginning of its orders
            if not region_name_written:
                region_name_written = True
                resultsfile.write('\n\n' + result_data_row['Region'] + '\n')

            if big_net_volume + int(result_data_row['Total Volume']) > MAX_VOLUME:
                resultsfile.write('\n\n' +
                                  'Big Net Profit: ' + f"{big_net_profit:,}" + '\n' +
                                  'Big Net Cost: ' + f"{big_net_cost:,}" + '\n' +
                                  'Big Net Volume: ' + f"{big_net_volume:,}" + '\n' +
                                  '\n')
            big_net_profit = big_net_profit + result_data_row['Net Profit']
            big_net_cost = big_net_cost + result_data_row['Net Cost']
            big_net_volume = big_net_volume + result_data_row['Total Volume']
            resultsfile.write('\n\n' +
                              'Net Profit: ' + f"{result_data_row['Net Profit']:,}" + '\n' +
                              'Item Name: ' + result_data_row['Item Name'] + '\n' +
                              'Item Id: ' + str(result_data_row['Item Id']) + '\n' +
                              'Region: ' + result_data_row['Region'] + '\n' +
                              'System: ' + result_data_row['System'] + '\n' +
                              'Profit Per: ' + f"{result_data_row['Profit Per']:,}" + '\n' +
                              'Profit Per m3: ' + f"{result_data_row['Profit Per m3']:,}" + '\n' +
                              'ROI: ' + str(result_data_row['ROI']) + '%' + '\n' + 
                              'Net Cost: ' + f"{result_data_row['Net Cost']:,}" + '\n' + 
                              'Max Quantity: ' + str(result_data_row['Max Quantity']) + '\n' +
                              'Top Buy Order Quantity: ' + str(result_data_row['Top Buy Order Quantity']) + '\n' +
                              'Volume Per: ' + str(result_data_row['Volume Per']) + '\n' +
                              'Total Volume: ' + str(result_data_row['Total Volume']) + '\n' +
                              'Jita Buy Order Price: ' + f"{result_data_row['Jita Buy Order Price']:,}" + '\n' +
                              'Region Sell Order Price: ' + f"{result_data_row['Region Sell Order Price']:,}" + '\n' +
                              '\n')

        if not big_net_profit == 0:
            resultsfile.write('\n\n' +
                              'Big Net Profit: ' + f"{big_net_profit:,}" + '\n' +
                              'Big Net Cost: ' + f"{big_net_cost:,}" + '\n' +
                              'Big Net Volume: ' + f"{big_net_volume:,}" + '\n' +
                              '\n')

        #break# Uncomment to test just Aridia
    resultsfile.close()
    with open('item_volumes.json', 'w') as item_volumes_file:
        json.dump(item_volumes, item_volumes_file)

    print("--- %s minutes ---" % ((time.time() - start_time) / 60))

if __name__ == '__main__':
    master()