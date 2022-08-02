
def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.load(stream, Loader=yaml.Loader)
        except yaml.YAMLError as exc:
            logging.error(exc)

def col_header_val(dask_df,table_config):
    dask_df.columns = dask_df.columns.str.lower()
    dask_df.columns = dask_df.columns.str.replace('[^\w]','_',regex=True)
    dask_df.columns = list(map(lambda x: x.strip('_'), list(dask_df.columns)))
    dask_df.columns = list(map(lambda x: replacer(x,'_'), list(dask_df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    dask_df.columns =list(map(lambda x: x.lower(), list(dask_df.columns)))
    dask_df = dask_df.reindex(sorted(dask_df.columns), axis=1)
    if len(dask_df.columns) == len(expected_col) and list(expected_col)  == list(dask_df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(dask_df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(dask_df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {dask_df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0
