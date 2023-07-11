import pandas as pd
import json


excel_file = './Sachim09062023.xlsx'

def get_total_rows(excel_file, sheet_name):
    df = pd.read_excel(excel_file,sheet_name=sheet_name)
    num_rows = df.shape[0]
    return num_rows


def read_excel_row(excel_file, sheet_name, row_index):
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    row = df.iloc[row_index]
    return row


def convert_df_to_json(excel_dataframe_df):
    json_str = excel_dataframe_df.to_json(orient='records')
    return json_str


def get_excel_dataframe(excel_file, sheet_name):
    dataFrame = pd.read_excel(excel_file, sheet_name=sheet_name)
    return dataFrame
   
# changes the order of items in data according to the given output_columns
def change_order(data,output_columns):
    
    filtered_data = [d for d in data if not any(key in d for key in output_columns)]

    for item in filtered_data:
        data.pop(item,None)

    sorted_data = sorted(data.items(), key=lambda item: output_columns.index(item[0]))
    obj = {}
    for sorted_item in sorted_data:
        obj[sorted_item[0]] = sorted_item[1]
    
    return obj 

# analyze and define number of devices to compile modbus registers according to the TOT number of devices
def create_output_device_list(excel_dataframe_df):
    # Hardcoded column name to look for Device Edge
    column_name = 'Hostname Edge'

    previous_value = None
    row_number_start = 2
    row_number_end = 0
    number_of_files = 1
    device_name_list = []

    for value in excel_dataframe_df[column_name]:
        row_number_end +=1
        if previous_value is not None and value != previous_value:
            number_of_files +=1
            device_name_list.append((previous_value,row_number_start,row_number_end))
            row_number_start = row_number_end + 1

        previous_value = value

    row_number_end += 1
    device_name_list.append((previous_value,row_number_start,row_number_end))
    return device_name_list

def check_static_data_for_sheet(sheet_name,excel_file_name=excel_file):
    try:
        plant_df = get_excel_dataframe(excel_file_name,sheet_name)
        result = convert_df_to_json(plant_df)
        sheetData = json.loads(result)
        return sheetData
    except:
        return []
    
def check_column_header_against_result_column_header(input_column_headers, result_column_headers):
    non_existant = []
    for element in input_column_headers:
        if element not in result_column_headers:
            non_existant.append(element)
    #result =  all(elem in input_column_headers  for elem in result_column_headers)
    
    return non_existant

# Read and populate plant data (as json)
sheet_name = 'Plant'
plant_df = get_excel_dataframe(excel_file,sheet_name)
plant_columns = plant_df.columns.tolist()
device_list = create_output_device_list(plant_df)

result = convert_df_to_json(plant_df)
plantData = json.loads(result)


# Read and populate modbus data (as json)
sheet_name = 'Modbus'

modbus_df = get_excel_dataframe(excel_file,sheet_name)
modbus_columns = modbus_df.columns.tolist()

result = convert_df_to_json(modbus_df)
modbusData = json.loads(result)


# IN CASE modality_auto is true, the result csv file will take column names from 
# two input sheets and put all of the first sheet column names and then all of the second sheet column names
modalita_auto = False

# modality_auto is false, the customized column names should be passed
# define result table columns (combination of previous two tables)
column_names_custom = [ 'measure-id','modbus-fc' ,'modbus-id',	'modbus-address'	,'modbus-n_registers'	,'modbus-format'	
                ,'mqtt-topic'	,'mqtt-qos'	,'Codice Stabilimento'	,'Descrizione Stabilimento'	,'Edificio'	
                ,'Vettore'	,'POD'	,'Piano'	,'Reparto'	,'Quadro'	,'Descrizione Quadro'	,'Sottoquadro'	
                ,'Descrizione Sottoquadro'	,'Linea'	,'Descrizione Linea'	,'Area Funzionale ENEA'	
                ,'Cod. Funzionale TERA'	,'Tipologia Dispositivo'	,'Codice Modello Dispositivo'	,'Taglia Interruttore'
                ,'Tipologia Misura'	,'Hostname Edge'	,'ID Modbus'
                ,'Tipo Dispositivo'	,'Misura'	,'Unita di misura','group', 'ime-type' ]


if modalita_auto is True:
    column_names = plant_columns + modbus_columns
else:
    column_names = column_names_custom

# combine the two tables data in a way that for each row on the first table
# we repeat said row to the number of rows that exist on the 2nd table (So we get all the modbus registers for that modbus ID)

def cutter(startIndex, endIndex, appendix_data ,plantData, modbusData):
    size =  (endIndex - startIndex + 1) * len(modbusData)
    total_size = size + len(appendix_data)
    resultList = [{}] * total_size

    main_index = 0
    j = 0
    k = startIndex - 2

    if(len(appendix_data) > 0):
        appendix_columns = appendix_data[0].keys()
        for i in range(len(appendix_data)):
            resultList[i] = {'measure-id': i+1}
            
            for column in appendix_columns:
                resultList[i][column] = appendix_data[i][column]

            main_index += 1

    for i in range(main_index,total_size):

        resultList[i] = {'measure-id': i+1}
        for modbusColumn in modbus_columns:
            resultList[i][modbusColumn] = modbusData[j][modbusColumn]


        for plantColumn in plant_columns:
            resultList[i][plantColumn] = plantData[k][plantColumn]

        j += 1

        if j == (len(modbusData)):
            j = 0
            if (k < endIndex):
                k = k + 1

    return resultList

#------------------------------------------------------------------- #
# Write the results to a json string & Convert the json to CSV file 
# ------------------------------------------------------------------ #

for item in device_list:
    updated_list = []
    appendix_data = check_static_data_for_sheet(item[0])
    if len(appendix_data):
        #print(f"size: {data[0].keys()}")
        res = check_column_header_against_result_column_header(appendix_data[0].keys(),column_names)
        if(len(res) > 0):
            print(f"different column name/s found in sheet {item[0]}, columns: {res}")
            exit()


    result = cutter(item[1],item[2],appendix_data,plantData,modbusData)

    for resultItem in result:
        ordered_obj = change_order(resultItem,column_names)
        updated_list.append(ordered_obj)

    fileName_name = f"./output/{item[0]}.csv"
    jsonString = json.dumps(updated_list)
    df = pd.read_json(jsonString)
    df.to_csv(fileName_name,sep=';', encoding='utf-8', index=False)