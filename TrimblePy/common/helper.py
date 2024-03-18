def return_column_schema(df):
    '''
    Returns a dictionary of column names and their data types
    '''
    col_schema = {}
    for col in df.columns:
        top_value = df[col].value_counts().index[0]
        col_schema[col] = type(top_value)
        if col_schema[col] == list:
            try:
                idx = 0
                while df[col].value_counts().index[idx] == []:
                    idx += 1
                real_value = df[col].value_counts().index[idx]
                col_schema[col] = {type(top_value)}, "|", type(real_value[0])
            except:
                col_schema[col] = type(top_value)
    return col_schema

def keys_to_columns(keys, df, prefix, dict_column):
    for key in keys:
        df.loc[:,f'{prefix}.{key}'] = df[dict_column].apply(lambda x: x.get(key))
    df = df.drop(columns=dict_column)
    return df


# create columns_to_keys
def columns_to_keys(df, prefix):
    keys = [x for x in df.columns if x.startswith(f'{prefix}.')]
    keys_no_prefix = [x.replace(f'{prefix}.','') for x in keys]
    df[prefix] = df[keys].apply(lambda x: dict(zip(keys_no_prefix,x)),axis=1)
    return df.drop(columns=keys)