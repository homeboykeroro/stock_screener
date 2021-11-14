def get_same_size_arry_elememt_np(src_np):
    src_np_without_nan = np.array([row[~np.isnan(row)] for row in src_np], dtype=object)
    no_of_col_arry = [row.size for row in src_np_without_nan]
    max_col_no = np.amax(no_of_col_arry)
    src_np_dtype = src_np.dtype

    temp_list = []
    for idx, arry in enumerate(src_np_without_nan):
        if arry.size != max_col_no:
            nan_arry = np.empty(max_col_no - arry.size)
            nan_arry[:] = np.nan
        
            temp_list.append(np.append(arry, nan_arry))
        else:
            temp_list.append(arry)

    return np.array(temp_list, src_np_dtype)