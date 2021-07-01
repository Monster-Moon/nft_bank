#######################################################
###################### refine #########################
#######################################################

rm(list = ls())
gc(reset = T)

setwd('Documents/nft_bank/')
if(!require(jsonlite)) install.packages('jsonlite'); require(jsonlite)
if(!require(dplyr)) install.packages('dplyr'); require(dplyr)
if(!require(stringr)) install.packages('stringr'); require(stringr)
if(!require(fastDummies)) install.packages('fastDummies'); require(fastDummies)
if(!require(data.table)) install.packages('data.table'); require(data.table)

erc20 = stream_in(file('erc20_exchange_rates.jsonl'))
axie_financial_events = stream_in(file('axie_financial_events_20210115_v2.jsonl'))
axie_item = stream_in(file('axie_item_metadata_v2.jsonl'))

# save.image('R_.Rdata')

table(axie_financial_events$event_type)
table(axie_financial_events$item_receive_transfer_type)
table(axie_financial_events$item_send_transfer_type)

financial_refined = axie_financial_events[lapply(axie_financial_events$erc20s, function(x) nrow(x) != 0) %>% do.call('c', .), ]
financial_value = lapply(financial_refined$erc20s, 
            function(x) x %>% select(sent_usd, received_usd) %>% colSums()) ## USD

financial_value_df = do.call('rbind', financial_value) %>% as.data.frame()
financial_inx = abs(rowSums(financial_value_df)) >= 1e-8 & !is.na(rowSums(financial_value_df))

financial_refined_df = financial_refined[financial_inx, ] ## 507211
financial_value_df = financial_value_df[financial_inx, ]

item_fun = function(i, df_, value_df_)
{
  sent_df = df_[i, ]$items_sent[[1]]
  if(nrow(sent_df) > 0)
  {
    sent_df = cbind(i, sent_df, value_df_[i, ], inx = 0)
  }
  received_df = df_[i, ]$items_received[[1]]
  if(nrow(received_df) > 0)
  {
    received_df = cbind(i, received_df, value_df_[i, ], inx = 1)
  }
  return(rbind(sent_df, received_df))
}

item_list = lapply(1:nrow(financial_refined_df), item_fun, df_ = financial_refined_df, value_df_ = financial_value_df)
item_df = do.call('rbind', item_list) %>% as.data.frame()

financial_refined_tmp = cbind(i = 1:nrow(financial_refined_df), financial_refined_df) %>%
  select(i, block_timestamp, event_type)
item_df = item_df %>% left_join(financial_refined_tmp, by = 'i')


item_refined_df = item_df %>% filter(item_id %in% axie_item$item_id)
mean_usd = item_refined_df %>% group_by(i) %>% 
  summarise(n = n(), sent = sent_usd / n, received = received_usd / n)
item_refined_df = item_refined_df %>% left_join(mean_usd, by = 'i')

head(item_refined_df)
item_financial = item_refined_df %>% 
  select(i, item_id, block_timestamp, event_type, sent, received, inx) 
# save(item_financial, file = 'item_df.Rdata')

value_df = item_financial %>% mutate(value = abs(sent + received)) %>% 
  select(i, item_id, block_timestamp, value) %>% 
  left_join(axie_item, by = 'item_id') %>%
  mutate(timestamp_diff = abs(as.Date(block_timestamp.x) - as.Date(block_timestamp.y))) %>% 
  group_by(i) %>% 
  arrange(timestamp_diff) %>%
  slice(1) %>% 
  ungroup()

value_df = value_df %>% 
  select(-i, -block_timestamp.y, -block_date) %>%
  rename(block_timestamp = block_timestamp.x)

logical_cols = c('breedable', 'is_mouth_mystic', 'is_mouth_bionic', 'is_mouth_xmas',
                 'is_horn_mystic', 'is_horn_bionic', 'is_horn_xmas', 
                 'is_back_mystic', 'is_back_bionic', 'is_back_xmas',
                 'is_tail_mystic', 'is_tail_bionic', 'is_tail_xmas', 
                 'is_eyes_mystic', 'is_eyes_bionic', 'is_eyes_xmas', 
                 'is_ears_mystic', 'is_ears_bionic', 'is_ears_xmas',
                 'is_having_mystic_part', 'is_having_xmas_part', 'is_having_bionic_part')

numeric_cols = c('value', 'father', 'mother', 'stage', 'level', 'exp',
                 'breedCount', 'skill', 'morale', 'speed', 'hp',
                 'all_parts_total_points', 
                 paste0('mouth_move_', c('stage', 'total_points', 'attack', 'defense', 'accuracy')),
                 paste0('horn_move_', c('stage', 'total_points', 'attack', 'defense', 'accuracy')),
                 paste0('back_move_', c('stage', 'total_points', 'attack', 'defense', 'accuracy')),
                 paste0('tail_move_', c('stage', 'total_points', 'attack', 'defense', 'accuracy')),
                 'pureness', 'total_mystic_parts_count', 'total_xmas_parts_count', 'total_bionic_parts_count',
                 'timestamp_diff')

logical_df = value_df %>% select(logical_cols) %>%
  mutate_all(as.numeric)
  
numeric_df = value_df %>% select(numeric_cols) %>%
  mutate_all(as.numeric)

char_df = value_df %>% 
  select(colnames(value_df)[!colnames(value_df) %in% c(logical_cols, numeric_cols)])

lm_df = cbind(char_df, numeric_df, logical_df)

# save(value_df, file = 'value_df.Rdata')
# save(lm_df, file = 'lm_df.Rdata')
# load('lm_df.Rdata')

lag_1 = function(x)
{
  tmp_x = dplyr::lag(x)
  tmp_x[1] = x[1]
  return(tmp_x)
}

lm_date_df = lm_df %>%
  select(item_id, block_timestamp, value, class,
         exp, breedCount, skill, morale, speed, hp, 
         ends_with(c('_class', 'attack', 'defense', 'accuracy')),
         pureness, breedable)

lm_date_df = lm_date_df[complete.cases(lm_date_df), ]

lm_date_dummy = dummy_cols(lm_date_df, 
                           select_columns = paste0(c('', 'mouth_', 'horn_', 'back_', 'tail_', 'eyes_', 'ears_'), 'class'),
                           remove_selected_columns = T)
lm_date_dummy_tmp = lm_date_dummy %>% group_by(item_id, block_timestamp) %>% 
  summarise_all(mean)

lm_date_dummy_lag = lm_date_dummy_tmp %>% 
  mutate(block_timestamp = as.Date(block_timestamp)) %>%
  group_by(item_id) %>% arrange(block_timestamp, .by_group = T) %>%
  mutate(time_numeric = as.numeric(block_timestamp - lag_1(block_timestamp)),
         time_numeric_end = as.numeric(as.Date(Sys.time()) - block_timestamp),
         value_lag = lag_1(value))

lm_date_dummy_lag %>% select(item_id, value, value_lag, block_timestamp, time_numeric)

# tmp = lm_date_df %>%
#   select(item_id, value, value_lag, time_diff, class, 
#          ends_with('_class'),
#          exp, breedCount, skill, morale, speed, hp, 
#          ends_with(c('attack', 'defense', 'accuracy')),
#          pureness, breedable, time_numeric, time_numeric_end)
# lm_date_df %>% select(item_id, value, value_lag, block_timestamp) %>% head(100)
# tmp_complete = tmp[complete.cases(tmp), ]
# tmp_d = dummy_cols(tmp_complete, 
#                    select_columns = paste0(c('', 'mouth_', 'horn_', 'back_', 'tail_', 'eyes_', 'ears_'), 'class'),
#                    remove_selected_columns = T)
# tmp_d %>% filter(item_id == 1000) %>% select(time_numeric_end, value, value_lag)

test_df = lm_date_dummy_lag %>% group_by(item_id) %>%
  arrange(time_numeric_end, .by_group = T) %>%
  dplyr::slice(1) %>% 
  ungroup()

train_df = lm_date_dummy_lag %>% select(-time_numeric_end, -block_timestamp)
test_df = test_df %>% select(-value, -time_numeric, -block_timestamp) %>% rename(time_numeric = time_numeric_end)


fwrite(train_df, 'train_df.csv')
fwrite(test_df, 'test_df.csv')



