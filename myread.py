import pandas as pd
import json

excel_file = './Sachim09062023.xlsx'



def convert_df_to_json(excel_dataframe_df):
    json_str = excel_dataframe_df.to_json(orient='records')
    return json_str


def get_excel_dataframe(excel_file, sheet_name):
    dataFrame = pd.read_excel(excel_file, sheet_name=sheet_name)
    return dataFrame

def read_excel_row(excel_file, sheet_name, row_index):
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    row = df.iloc[row_index]
    return row


# Read and populate modbus data (as json)
sheet_name = 'Modbus'

modbus_df = get_excel_dataframe(excel_file,sheet_name)
modbus_columns = modbus_df.columns.tolist()

result = convert_df_to_json(modbus_df)
modbusData = json.loads(result)

mylist = []

for value in modbus_df['Misura']:
    mylist.append(value)


print(mylist)

