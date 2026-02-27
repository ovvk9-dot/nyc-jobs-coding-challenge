from user_functions import *
from pyspark.sql.functions import * 
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import *

import logging

# set up a logger for this module
logger = logging.getLogger(__name__)


def pre_process_data(
    df,
    remove_duplicates_params=None,
    rename_col_mapping_path=None,
    remove_special_chars_cols=None,
    convert_to_numeric_cols=None,
    convert_to_double_cols=None,
    convert_to_datetime_cols=None,
    convert_to_titlecase_cols=None,
    drop_columns_list=None,
):
    """
    Execute a standard cleaning pipeline on a Spark DataFrame.

    The actions are executed sequentially if the corresponding argument is
    supplied.  They include renaming, dropping, sanitising characters,
    type conversions, text normalisation and optional deduplication.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataset to transform.
    remove_duplicates_params : tuple, optional
        A 3‑tuple (partition_cols, order_cols, desc_flag) passed to
        :func:`remove_duplicates`.
    rename_col_mapping_path : str, optional
        Filepath to a JSON/CSV that maps old column names to new ones.
    remove_special_chars_cols : list, optional
        Column names whose values should be stripped of special characters.
    convert_to_numeric_cols : list, optional
        Columns to cast to integer type.
    convert_to_double_cols : list, optional
        Columns to cast to double precision.
    convert_to_datetime_cols : list, optional
        Columns to parse as timestamp/datetime values.
    convert_to_titlecase_cols : list, optional
        Columns whose text should be converted to title case.
    drop_columns_list : list, optional
        Columns to remove from the DataFrame altogether.

    Returns
    -------
    pyspark.sql.DataFrame
        The transformed DataFrame instance.
    """

    logger.info("Beginning preprocessing routine")
    logger.debug(f"Starting DataFrame: {df.count()} rows × {len(df.columns)} cols")

    if rename_col_mapping_path :
        logger.info(f"Step 1/6: Applying column mapping from {rename_col_mapping_path}")
        df = col_rename_with_mapping(df,col_mapping_path=rename_col_mapping_path)
    
    if drop_columns_list:
        logger.info(f"Step 2/6: Dropping {len(drop_columns_list)} columns")
        df = drop_columns(df,drop_columns_list)

    if remove_special_chars_cols:
        logger.info(f"Step 3/6: Removing special characters from {len(remove_special_chars_cols)} columns")
        for column_name in remove_special_chars_cols:
            df = remove_special_characters(df,column_name)
    
    if convert_to_numeric_cols:
        logger.info(f"Step 4/6: Converting {len(convert_to_numeric_cols)} columns to numeric (int)")
        for column_name in convert_to_numeric_cols:
            df = convert_to_numeric(df,column_name)
    
    if convert_to_double_cols:
        logger.info(f"Step 4/6: Converting {len(convert_to_double_cols)} columns to double")
        for column_name in convert_to_double_cols:
            df = convert_to_numeric(df,column_name,to_double=True)

    if convert_to_datetime_cols:
        logger.info(f"Step 5/6: Converting {len(convert_to_datetime_cols)} columns to datetime")
        for column_name in convert_to_datetime_cols:
            df = convert_to_datetime(df,column_name)
    
    if convert_to_titlecase_cols:
        logger.info(f"Step 6/6: Converting {len(convert_to_titlecase_cols)} columns to Title Case")
        for column_name in convert_to_titlecase_cols:
            df = convert_to_tilecase(df,column_name)
    
    if remove_duplicates_params:
        logger.info(f"Final step: Removing duplicates")
        df = remove_duplicates(
                df,
                dedup_grain = remove_duplicates_params[0],
                order_grain = remove_duplicates_params[1],
                is_desc = remove_duplicates_params[2]
        )
    
    logger.info(f"Data preprocessing completed. Final shape: {df.count()} rows, {len(df.columns)} columns")
    return df
