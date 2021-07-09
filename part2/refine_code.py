import pandas as pd
import numpy as np

item_metadata = pd.read_json('axie_item_metadata_v2.jsonl', lines = True)
financial_events = pd.read_json('axie_financial_events_20210115_v2.jsonl', lines = True)

events_len = [len(i) for i in financial_events['erc20s']]
sum([i == 0 for i in events_len]) ## 346455
financial_events_nonzero = financial_events.loc[[i != 0 for i in events_len], :].reset_index(drop = True)
financial_events_nonzero.shape ## 512209, 22

sum([i == 1 for i in events_len])
sum([i > 1 for i in events_len]) ## 109350 

def get_usd(x):    
    tmp_inx = ['sent_usd' in i.keys() and 'received_usd' in i.keys() for i in x]
    keys_usd = ['sent_usd', 'received_usd']
    x_usd = [x_tmp for x_tmp, i in zip(x, tmp_inx) if i == True]
    if len(x_usd) == 0:
        return([np.nan, np.nan])  
    elif len(x_usd) == 1:
        return(list(map(x_usd[0].get, keys_usd)))
    else:
        return(list(np.sum([list(map(i.get, keys_usd)) for i in x_usd], axis = 0)))

financial_usd_list = [get_usd(x) for x in financial_events_nonzero['erc20s']]
usd_df = pd.DataFrame(np.array(financial_usd_list), columns = ['sent_usd', 'received_usd'])
financial_events_inx = np.abs(np.sum(usd_df, axis = 1)) > 1e-8

usd_df = usd_df.loc[financial_events_inx,:].copy().reset_index(drop = True)
financial_events_nonzero = financial_events_nonzero.loc[financial_events_inx,:].copy().reset_index(drop = True)

sum([len(i) for i in financial_events_nonzero.items_received]) + sum([len(i) for i in financial_events_nonzero.items_sent]) ## 512542

item_value_list = []
for i in range(len(financial_events_nonzero)):
    item_received_tmp = financial_events_nonzero.items_received[i]
    item_sent_tmp = financial_events_nonzero.items_sent[i]
    
    item_id = [x.get('item_id') for x in item_received_tmp] + [x.get('item_id') for x in item_sent_tmp]
    if len(item_id) == 0:
        continue
    
    item_value = [[i] + [j] + [i / len(item_id) for i in list(usd_df.iloc[i,:])] for j in item_id]
    for j in range(len(item_value)):
        item_value_list.append(item_value[j])    
    
item_value_df = pd.DataFrame(item_value_list, columns = ['index', 'item_id', 'sent_usd', 'received_usd'])
item_value_df['value'] = (item_value_df['sent_usd'] + item_value_df['received_usd']).abs()

financial_events_nonzero_tmp = financial_events_nonzero.reset_index(drop = False)[['index', 'block_timestamp']].copy()
item_value_timestamp_df = item_value_df[['index', 'item_id', 'value']].merge(financial_events_nonzero_tmp, on = 'index').copy()
item_value_timestamp_df['block_date'] = [i[:10] for i in item_value_timestamp_df['block_timestamp']]
item_value_timestamp_df = item_value_timestamp_df.drop('block_timestamp', axis = 1).copy()

item_value_mean_date_df = item_value_timestamp_df.drop('index', axis = 1).groupby(['item_id', 'block_date']).agg('mean').reset_index(drop = False).sort_values(['item_id', 'block_date'], ascending = [True, True])


shifted_value1 = item_value_mean_date_df.groupby(['item_id']).shift(1)
shifted_value1.loc[np.isnan(shifted_value1['value']),:] = item_value_mean_date_df[['block_date', 'value']].loc[np.isnan(shifted_value1['value']),:]

shifted_value2 = item_value_mean_date_df.groupby(['item_id']).shift(2)
shifted_value2.loc[np.isnan(shifted_value2['value']),:] = shifted_value1[['block_date', 'value']].loc[np.isnan(shifted_value2['value']),:]

# shifted_value3 = item_value_mean_date_df.groupby(['item_id']).shift(3).drop('block_date', axis = 1)

shifted_value1 = shifted_value1.rename({'value': 'value_lag1', 'block_date':'block_date_lag1'}, axis = 1)
shifted_value2 = shifted_value2.rename({'value': 'value_lag2', 'block_date':'block_date_lag2'}, axis = 1)

shifted_df = pd.concat([shifted_value1, shifted_value2], axis = 1)
time_df = pd.concat([item_value_mean_date_df['block_date'], shifted_df[['block_date_lag1', 'block_date_lag2']]], axis = 1).apply(pd.to_datetime)

time_df['time_diff1'] = (time_df['block_date_lag1'] - time_df['block_date']).apply(lambda x: x.total_seconds()/86400)
time_df['time_diff2'] = (time_df['block_date_lag2'] - time_df['block_date']).apply(lambda x: x.total_seconds()/86400)
time_df = time_df.drop(['block_date', 'block_date_lag1', 'block_date_lag2'], axis = 1)

item_value_mean_date_df = pd.concat([item_value_mean_date_df, shifted_df, time_df], axis = 1)

item_value_mean_date_df

# item_metadata_nunique = item_metadata.groupby('item_id').nunique()
# tmp = item_metadata_nunique.drop(['block_timestamp', 'block_date'], axis = 1)
# tmp = (np.array(tmp == 0) + np.array(tmp == 1))[0]
# tmp.all() ## block_date 빼고 stat이 변하지 않음

item_metadata_unique = item_metadata.groupby('item_id').first().drop(['block_timestamp', 'block_date'], axis = 1)
df = item_value_mean_date_df.merge(item_metadata_unique, on = 'item_id').sort_values(['item_id', 'block_date'], ascending = [True, True])

df_completed = df.dropna(axis = 0, how = 'any')

drop_cols = ['item_id', 'block_date', 'block_date_lag1', 'block_date_lag2', 'father', 'mother']
df_X = df_completed.drop(drop_cols, axis = 1)
df_X_dummy = pd.get_dummies(df_X)

df_X_dummy