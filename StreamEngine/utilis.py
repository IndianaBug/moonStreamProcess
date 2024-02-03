import numpy as np
import datetime

def booksflow_find_level(price, level_size):
    return np.ceil(price / level_size) * level_size

def booksflow_compute_percent_variation(new_value, old_value):
    try:
        percentage_difference = ((new_value - old_value) / abs(old_value)) * 100
        return percentage_difference
    except:
        return 9999999999

def booksflow_manipulate_arrays(old_levels, new_levels, new_values):
    """
      helper for dynamically dealing with new columns
    """
    new_isolated_levels = np.setdiff1d(new_levels, old_levels)
    sorted_new_values = np.array([])
    for i in range(len(old_levels)):
        index = np.where(new_levels == old_levels[i])[0]
        if len(index) != 0:
            sorted_new_values = np.append(sorted_new_values, new_values[index])
        else:
            sorted_new_values = np.append(sorted_new_values,0)
    for i in range(len(new_isolated_levels)):
        index = np.where(new_levels == new_isolated_levels[i])[0]
        if len(index) != 0:
            sorted_new_values = np.append(sorted_new_values, new_values[index])    
        else:
            sorted_new_values = np.append(sorted_new_values,0)
    return sorted_new_values

def booksflow_datatrim(current_price, dataDict, side, book_ceil_thresh):
    keys_to_remove = []
    for level in dataDict[side].keys():
        if abs(booksflow_compute_percent_variation(float(level), current_price)) > book_ceil_thresh:
            keys_to_remove.append(level)
    for level in keys_to_remove:
        del dataDict[side][level]

def calculate_option_time_to_expire_deribit(date : str):                                  
    today_day = datetime.datetime.now().timetuple().tm_yday
    today_year = datetime.datetime.now().year
    f = datetime.datetime.strptime(date, "%d%b%y")
    expiration_date = f.timetuple().tm_yday
    expiration_year = f.year
    if today_year == expiration_year:
        r = expiration_date - today_day
    if today_year == expiration_year + 1:
        r = 365 + expiration_date - today_day
    return float(r)

def calculate_option_time_to_expire_okex(date):                                  
    today_day = datetime.datetime.now().timetuple().tm_yday
    today_year = datetime.datetime.now().year
    f = datetime.datetime.strptime(date, "%y%m%d")
    expiration_date = f.timetuple().tm_yday
    expiration_year = f.year
    if today_year == expiration_year:
        r = expiration_date - today_day
    if today_year == expiration_year + 1:
        r = 365 + expiration_date - today_day
    return float(r)

def calculate_option_time_to_expire_bybit(date):
    target_date = datetime.datetime.strptime(date, '%d%b%y')
    current_date = datetime.datetime.now()
    days_left = (target_date - current_date).days
    return int(days_left)


def merge_suffixes(n):
    """
        The maximum amount of datasets to aggregate is the len(alphabet). 
        Modify this function to get more aggregation possibilities
    """
    alphabet = 'xyzabcdefghijklmnopqrstuvw'
    suffixes = [f'_{alphabet[i]}' for i in range(n)]
    return suffixes
