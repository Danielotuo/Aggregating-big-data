import pandas as pd

def load_data(url):
    """Load data from a specified URL into a pandas dataframe."""
    return pd.read_csv(url)

def filter_subscription_data(sub_df, chapter_id):
    """Filter subscription data for a specified chapter_id."""
    return sub_df[sub_df['chapter_id'] == chapter_id]

def merge_dataframes(cons_df, email_df, sub_df):
    """Merge constituent information, email, and subscription data using cons_id as the key."""
    people_df = cons_df.merge(email_df, on='cons_id')
    people_df = people_df[people_df['is_primary'] == 1] # keep only primary email addresses
    people_df = people_df.merge(sub_df[['cons_email_id', 'isunsub']], on='cons_email_id', how='left')
    people_df['is_unsub'] = people_df['isunsub'].fillna(0).astype(bool) # fill missing values and convert to bool
    return people_df[['email', 'source', 'is_unsub', 'create_dt_x', 'modified_dt_x']] # suffixes are added for any clashes in column names not involved in the merge operation 

def clean_data(people_df):
    """Clean up data by selecting desired columns, renaming columns, and converting date columns to datetime."""
    people_df.columns = ['email', 'code', 'is_unsub', 'created_dt', 'updated_dt']
    people_df[['created_dt', 'updated_dt']] = people_df[['created_dt', 'updated_dt']].apply(pd.to_datetime)
    return people_df

def save_data_to_csv(df, filename):
    """Save a pandas dataframe to a CSV file in the working directory."""
    df.to_csv(filename, index=False)

def extract_acquisition_counts(people_df):
    """Extract acquisition counts by date."""
    people_df['acquisition_date'] = people_df['created_dt'].dt.date
    return people_df.groupby('acquisition_date').size().reset_index(name='acquisitions')

if __name__ == '__main__':
    # URLs for data sources
    cons_url = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv'
    email_url = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv'
    sub_url = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv'

    # Load data from URLs
    cons_df = load_data(cons_url)
    email_df = load_data(email_url)
    sub_df = load_data(sub_url)

    # Filter subscription data for chapter_id = 1
    sub_df = filter_subscription_data(sub_df, 1)

    # Merge dataframes and clean up data
    people_df = merge_dataframes(cons_df, email_df, sub_df)
    people_df = clean_data(people_df)

    # Save cleaned data to CSV file
    save_data_to_csv(people_df, 'people.csv')

    # Extract acquisition counts by date and save to CSV file
    acquisition_counts = extract_acquisition_counts(people_df)
    save_data_to_csv(acquisition_counts, 'acquisition_facts.csv')
