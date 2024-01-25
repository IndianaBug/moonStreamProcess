import numpy as np


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
    full_new_levels = np.concatenate((old_levels, np.setdiff1d(new_levels, old_levels)))
    full_new_values = np.zeros_like(full_new_levels)
    for i in range(len(full_new_levels)):
        index = np.where(new_levels == full_new_levels[i])[0]
        if len(index) > 0:
            full_new_values[i] = new_values[index[0]]
        else:
            full_new_values[i] = 0     
    return full_new_values

def booksflow_datatrim(current_price, dataDict, side, book_ceil_thresh):
    keys_to_remove = []
    for level in dataDict[side].keys():
        if abs(booksflow_compute_percent_variation(float(level), current_price)) > book_ceil_thresh:
            keys_to_remove.append(level)
    for level in keys_to_remove:
        del dataDict[side][level]