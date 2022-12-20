import pandas as pd
import datetime

# Read the input files
with open('corp_pfd.dif', 'r') as f:
    # Initialize an empty list to store the lines
    cols = []
    data = []
    # Initialize a flag variable to False
    col_flag = False
    data_flag = False
    # Iterate over the lines in the file
    for line in f:
        # Check if the current line is 'START-OF-FIELDS'
        if line.strip() == 'START-OF-FIELDS':
            col_flag = True
        # If the flag is True, append the current line to the list
        if col_flag and not line.strip() == 'START-OF-FIELDS' and not line.strip() == 'END-OF-FIELDS' and not line.startswith('#'):
            cols.append(line.strip())
        # Check if the current line is 'END-OF-FIELDS'
        if line.strip() == 'END-OF-FIELDS':
            col_flag = False
        if line.strip() == 'START-OF-DATA':
            data_flag = True
        if data_flag:
            data.append(line.strip())
        if line.strip() == 'END-OF-DATA':
            data_flag = False


corp_pfd = pd.DataFrame([sub.split("|")[:-1] for sub in data], columns=list(filter(('').__ne__, cols)))
reference_fields = pd.read_csv('reference_fileds.csv')
reference_securities = pd.read_csv('reference_securities.csv')

# Limit the columns in the DataFrame to only those found in 'reference_fields.csv'
column_names = reference_fields['field'].tolist()
corp_pfd = corp_pfd.filter(items=column_names)
corp_pfd = corp_pfd.rename(columns={'ID_BB_GLOBAL': 'id_bb_global'})
# Compare securities in the input file with those in 'reference_securities.csv'
merged = pd.merge(corp_pfd, reference_securities, on='id_bb_global')
merged = merged.drop_duplicates()
in_both = merged[merged['id_bb_global'].isin(reference_securities['id_bb_global'])]
new_securities = merged.drop(in_both.index)
new_securities.to_csv('new_securities.csv', index=False)

# Create the 'security_data.csv' file
security_data = pd.DataFrame(columns=['id_bb_global', 'field', 'value', 'source', 'tstamp'])
for index, row in corp_pfd.iterrows():
    id_bb_global = row['id_bb_global']
    for field, value in row.items():
        if field != 'id_bb_global':
            security_data = security_data.append({'id_bb_global': id_bb_global, 'field': field, 'value': value, 'source': 'corp_pfd.dif', 'tstamp': datetime.datetime.now()}, ignore_index=True)
security_data.to_csv('security_data.csv', index=False)
