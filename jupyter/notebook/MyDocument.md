## Data Exploration & Analysis

### 1. Dataset Overview
This dataset comprises current job postings from the City of New York's official employment web portal.

- **Records**: 2,946
- **Fields**: 28
- **Format**: CSV

### 2. Column Analysis
The dataset combines numerical values, categorical variables, and unstructured text fields.

| Column Name         | Data Type   | Description                                              | Key Observations & Notes                                                                                             |
|---------------------|-------------|----------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Job ID              | Numerical   | Unique identifier for each position                      | Serves as primary key; useful for deduplication                                                                      |
| Agency              | Categorical | Hiring organization (e.g., DEPT OF BUSINESS SERV)       | 52 different agencies actively recruiting                                                                            |
| Posting Type        | Categorical | Classification (Internal or External)                    | Most positions appear twice—once for each posting type. Should be merged into a single indicator                     |
| # Of Positions      | Numerical   | Total number of openings per job posting                 | N/A                                                                                                                  |
| Job Category        | Categorical | Occupational classification (e.g., Engineering)         | 130 distinct categories represented                                                                                  |
| Salary Range From / To  | Numerical   | Minimum salary offered                                   | Spans $0 to $234,402 depending on pay frequency                                                                      |
| Salary Frequency    | Categorical | Pay period (Annual, Hourly, or Daily)                   | Mixed scales require standardization for comparison                                                                   |
| Civil Service Title | Categorical | Official government job classification                   | High-cardinality text field                                                                                          |
| Business Title      | Categorical | Specific role designation                                | High-cardinality text field                                                                                          |
| Preferred Skills    | Text        | Unstructured skill requirements and recommendations       | Requires text parsing to extract meaningful keywords                                                                  |
| Posting Date        | DateTime    | Job posting initiation date                              | Identifies most recent entries. Currently stored as string; requires conversion                                      |
| Post Until          | DateTime    | Posting expiration date                                  | Currently stored as string; requires conversion                                                                      |
| Posting Updated     | DateTime    | Most recent posting modification date                    | Currently stored as string; requires conversion                                                                      |
| Process Date        | DateTime    | Data extraction timestamp                                | Currently stored as string; requires conversion                                                                      |


### 3. Data Quality Assessment

| Column | Missing Count | % Missing | Recommendation |
|--------|---------------|-----------|----------------|
| Recruitment Contact | 2,946 | 100% | Remove—contains no data |
| Post Until | 2,075 | 70% | Remove—suggests positions remain open; use Posting Date instead |
| Hours/Shift | 2,062 | 70% | Remove—not relevant to key metrics |
| Work Location 1 | 1,588 | 54% | Remove—replace with Work Location column |
| Preferred Skills | 393 | 13% | Retain—clean and extract meaningful keywords |
| Job Category | 2 | <1% | Retain—minimal impact |


### 4. Key Data Issues and Solutions

#### 4.1. Duplicate Records (Internal vs. External Postings)
Each Job ID frequently appears twice in the dataset—once as an Internal posting and once as External.

- **Impact**: Aggregating data directly on the raw file will result in double-counting job openings and skewing salary averages.

- **Resolution**: We must perform a deduplication step, grouping by Job ID and merging the Posting Type into a single indicator (e.g., Internal/External).

#### 4.2. Salary Standardization
Salary data ranges from $0 to $218,587 across three different frequencies: hourly ($50/hr average), daily, and annual ($50,000/yr average). Mixed scales prevent meaningful comparison.

**Impact**: Comparing hourly rates ($30) directly with annual salaries ($50,000) produces erroneous KPIs.

**Solution**: Convert all salaries to an annual basis using standardized factors: 2,080 hours per year (hourly) and 260 days per year (daily).

#### 4.3. Unstructured Text Fields
Columns such as Minimum Qualifications and Preferred Skills contain dense, unstructured prose requiring interpretation.

**Solution**: Apply text processing techniques (tokenization and stop word removal) to extract meaningful skill keywords.


### 5. Column Removal 
Some of the columns can be dropped from the dataframe as they would not be relevant for our analysis ( This assumptions are made based on the requirements of the challenge)

| Column Name            | Reason for Dropping                                                                                                                |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| Recruitment Contact    | 100% Null. It contains no data.                                                                                                    |
| Post Until             | High Null Rate (70%) , we can use Posting Date or Posting Updated. This column is redundant and mostly empty.          |
| Hours/Shift            | High Null Rate (70%), there is no direct relation to Requested KPIs.                                              |
| Work Location 1        | High Null Rate (54%). High null rates , we can leverage Work Location column information.                                                                              |
| To Apply               | Irrelevant Text , Contains instructions like "Click here to apply," which provides no analytical value for salary or category KPIs. |
| Residency Requirement  | Irrelevant Text, Residency Legal & Eligibility text not needed for the requested analysis.                                                     |
| Additional Information | Irrelevant Text, As these are administrative notes that do not contribute to the skills or salary related analysis.                              |
| Process Date           | This is an internal system timestamp. Updating Posting Date can be leveraged instead for analysis             |


# Unit Test Cases Overview (init_test_cases_notebook.ipynb)

The `unit_test_cases_notebook.ipynb` file contains comprehensive validation tests for all data transformation functions referenced in the assessment notebook. This ensures reliable data processing throughout the pipeline.

### Test Configuration

- **Framework**: PySpark DataFrames with local Spark session
- **Logging**: Configured to track execution and results
- **Test Count**: 11 comprehensive test cases
- **Status**: All tests passed (100% success rate)

### Unit Test Cases

#### Test 1: remove_special_characters
- **Purpose**: Validates removal of special characters from text fields
- **Input Scenarios**: 
  - Text with special characters: `hello@world!` → `helloworld`
  - Mixed alphanumeric with symbols: `test#data$` → `testdata`
  - Test maintains hyphens and commas: `abc-123,456` → `abc-123,456`
- **Assertions**: Verifies special characters are removed while preserving alphanumeric characters and specific symbols

#### Test 2: convert_to_numeric
- **Sub-test 2.A**: Integer conversion
  - Converts string currency values to integers: `$100` → `100`
  - Extracts numeric values from mixed strings: `$300abc` → `300`
- **Sub-test 2.B**: Double conversion  
  - Converts decimal currency values: `$100.50` → `100.50`
  - Handles mixed decimal strings: `$300.99abc` → `300.99`
- **Purpose**: Ensures proper numeric type conversion for salary and amount fields

#### Test 3: convert_to_datetime
- **Purpose**: Validates string-to-timestamp conversion for date fields
- **Input Scenarios**: ISO 8601 formatted datetime strings (e.g., `2020-01-15T10:30:45.000`)
- **Assertions**: Verifies the resulting column has timestamp data type

#### Test 4: convert_to_tilecase
- **Purpose**: Converts text to title case (proper capitalization)
- **Input Scenarios**:
  - `hello world` → `Hello World`
  - `PYSPARK CODE` → `Pyspark Code`
  - Handles leading/trailing spaces: `  python programming  ` → `Python Programming`
- **Assertions**: Verifies proper title case formatting with correct capitalization

#### Test 5: remove_duplicates
- **Purpose**: Removes duplicate records based on dedup grain, keeping most recent based on order grain
- **Input Scenarios**: 
  - Multiple records with same ID but different dates
  - Deduplication on `id` column, ordering by `date` in descending order
- **Assertions**: 
  - Verifies row count reduced to 2 (from 4 duplicates)
  - Confirms latest dates are retained: ID 1 → `2020-01-02`, ID 2 → `2020-01-03`

#### Test 6: col_rename_with_mapping
- **Purpose**: Renames DataFrame columns based on a JSON mapping file
- **Input Scenarios**: 
  - Mapping file with old-to-new column name pairs
  - `old_col1` → `new_col1`, `old_col2` → `new_col2`
- **Assertions**: 
  - Verifies new column names exist in output
  - Confirms old column names are removed

#### Test 7: drop_columns
- **Purpose**: Removes specified columns from DataFrame
- **Input Scenarios**: DataFrame with 4 columns (id, name, title, salary)
- **Columns to drop**: `title`, `salary`
- **Assertions**: 
  - Confirms dropped columns removed (`title`, `salary`)
  - Verifies remaining columns preserved (`id`, `name`)
  - Validates final column count equals 2

#### Test 8: annualize_salary
- **Purpose**: Normalizes salary ranges from different frequencies to annual basis
- **Input Scenarios**:
  - Annual salary: `1000` remains `1000` (unchanged)
  - Hourly salary: `10` → `20800` (multiplied by 2080 hours/year)
  - Daily salary: `100` → `26000` (multiplied by 260 days/year)
- **Assertions**: Verifies correct annualization factors applied based on salary frequency

#### Test 9: create_qualification_indicator
- **Purpose**: Creates binary indicator for degree requirements in qualification text
- **Input Scenarios**:
  - Contains degree: `Bachelor's degree required` → `is_degree_req = 1`
  - No degree: `High school diploma` → `is_degree_req = 0`
  - Alternative degree: `Master's degree preferred` → `is_degree_req = 1`
- **Assertions**: Verifies correct detection of degree-related keywords in qualification requirements

#### Test 10: display
- **Purpose**: Validates basic DataFrame structure and row/column counts
- **Input Scenarios**: DataFrame with columns (id, name, value)
- **Assertions**: 
  - Confirms row count: 3
  - Confirms column count: 3

#### Test 11: export_to_csv
- **Purpose**: Exports DataFrame to CSV file with proper formatting
- **Input Scenarios**: 
  - DataFrame with 3 rows and 2 columns (id, name)
  - Export to temporary directory
- **Assertions**: 
  - Verifies output file exists at expected path
  - Confirms CSV structure: header row + 3 data rows = 4 total lines
  - Validates header contains expected column names (`id`, `name`)

### Test Execution Summary

| Test ID | Function | Status |
|---------|----------|--------|
| Test 1 | remove_special_characters | ✓ PASSED |
| Test 2A | convert_to_numeric (int) | ✓ PASSED |
| Test 2B | convert_to_numeric (double) | ✓ PASSED |
| Test 3 | convert_to_datetime | ✓ PASSED |
| Test 4 | convert_to_tilecase | ✓ PASSED |
| Test 5 | remove_duplicates | ✓ PASSED |
| Test 6 | col_rename_with_mapping | ✓ PASSED |
| Test 7 | drop_columns | ✓ PASSED |
| Test 8 | annualize_salary | ✓ PASSED |
| Test 9 | create_qualification_indicator | ✓ PASSED |
| Test 10 | display | ✓ PASSED |
| Test 11 | export_to_csv | ✓ PASSED |

**Total Tests**: 12 | **Passed**: 12 | **Failed**: 0 | **Success Rate**: 100%

# Pre Processing Functions (pre_process_functions.py)

Orchestrated data cleaning pipeline that sequences multiple transformation operations on Spark DataFrames:

## Core Function: `pre_process_data`

**Purpose**: Execute a comprehensive cleaning pipeline on a Spark DataFrame by sequentially applying transformations including renaming, dropping columns, sanitizing characters, type conversions, text normalization, and optional deduplication.

**Key Parameters**:

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `df` | pyspark.sql.DataFrame | Input dataset to transform | Yes |
| `rename_col_mapping_path` | str | Filepath to JSON/CSV mapping old column names to new ones | Optional |
| `drop_columns_list` | list | Column names to remove from DataFrame | Optional |
| `remove_special_chars_cols` | list | Column names to strip of special characters | Optional |
| `convert_to_numeric_cols` | list | Columns to convert to integer type | Optional |
| `convert_to_double_cols` | list | Columns to convert to double precision | Optional |
| `convert_to_datetime_cols` | list | Columns to parse as timestamp/datetime values | Optional |
| `convert_to_titlecase_cols` | list | Columns to convert text to title case | Optional |
| `remove_duplicates_params` | tuple | 3-tuple (partition_cols, order_cols, desc_flag) for deduplication | Optional |

**Processing Pipeline** (Sequential Execution):

| Step | Operation | Condition | Utility Function Called |
|------|-----------|-----------|------------------------|
| 1/6 | Column Renaming | If `rename_col_mapping_path` provided | `col_rename_with_mapping()` |
| 2/6 | Drop Columns | If `drop_columns_list` provided | `drop_columns()` |
| 3/6 | Remove Special Characters | If `remove_special_chars_cols` provided | `remove_special_characters()` |
| 4/6 | Convert to Numeric (Integer) | If `convert_to_numeric_cols` provided | `convert_to_numeric()` |
| 4/6 | Convert to Numeric (Double) | If `convert_to_double_cols` provided | `convert_to_numeric()` with `to_double=True` |
| 5/6 | Convert to DateTime | If `convert_to_datetime_cols` provided | `convert_to_datetime()` |
| 6/6 | Convert to Title Case | If `convert_to_titlecase_cols` provided | `convert_to_tilecase()` |
| Final | Remove Duplicates | If `remove_duplicates_params` provided | `remove_duplicates()` |

**Return Value**: Transformed pyspark.sql.DataFrame with all specified transformations applied in sequence

**Logging**: Tracks each step's execution with INFO and DEBUG level logs tracking row/column counts at each stage


# Utility Functions (user_functions.py)

Reusable PySpark utility functions for data processing:

| Function | Purpose | Key Parameters |
|----------|---------|-----------------|
| `display(df)` | Display DataFrame in tabular format | df |
| `remove_special_characters(df, col)` | Remove special chars using regex | df, column_name |
| `convert_to_numeric(df, col, to_double)` | Extract and convert numbers | df, column_name, to_double flag |
| `convert_to_datetime(df, col)` | Convert ISO 8601 strings to timestamp | df, column_name |
| `convert_to_tilecase(df, col)` | Convert text to title case | df, column_name |
| `col_rename_with_mapping(df, path)` | Rename columns from JSON mapping | df, mapping_file_path |
| `drop_columns(df, cols_list)` | Remove specified columns | df, columns_to_drop list |
| `remove_duplicates(df, dedup, order, desc)` | Deduplicate using window functions | df, dedup_grain, order_grain, is_desc |
| `annualize_salary(df)` | Normalize salary to annual basis | df (creates 4 new columns) |
| `create_qualification_indicator(df)` | Add degree requirement binary flag | df (creates is_degree_req column) |
| `export_to_csv(df, path, name)` | Export DataFrame to CSV | df, output_path, file_name |


# Assessment Notebook (assesment_notebook.ipynb)

Comprehensive data processing and analysis pipeline:

**Pipeline Stages:**
1. **Setup**: Initialize Spark, import libraries, configure logging
2. **Load**: Read CSV (2,946 rows × 28 columns)
3. **EDA**: Analyze column types, nulls, statistics using Pandas
4. **Preprocess**: Rename columns, drop irrelevant columns, clean text, convert types, deduplicate rows
5. **Feature Engineering**: Annualize salaries, add degree requirement indicator
6. **Analysis**: Run 7 analytical queries

**Analysis Highlights:**
- Top 10 job categories ranked by posting frequency
- Salary ranges segmented by job category
- Positive correlation identified between degree requirements and salary
- Highest-paying position within each agency
- Salary trend analysis (24-month period)
- Top 20 highest-paying skills (minimum 10 occurrences)

**Output**: `/dataset/cleaned/nyc-jobs-cleaned.csv` (1,640 rows × 27 columns)
