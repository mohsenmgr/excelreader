import pandas as pd
import json

def get_total_rows(excel_file, sheet_name):
    df = pd.read_excel(excel_file,sheet_name=sheet_name)
    num_rows = df.shape[0]
    return num_rows


def read_excel_row(excel_file, sheet_name, row_index):
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    row = df.iloc[row_index]
    return row


def process_excel_sheet(excel_file, sheet_name):
    excel_data_df = pd.read_excel(excel_file, sheet_name=sheet_name)
    json_str = excel_data_df.to_json(orient='records')
    return json_str


# changes the order of items in data according to the given column_names
def change_order(data):
    sorted_data = sorted(data.items(), key=lambda item: column_names.index(item[0]))
    obj = {}
    for sorted_item in sorted_data:
        obj[sorted_item[0]] = sorted_item[1]
    
    return obj 
    

excel_file = './Sachim09062023.xlsx'

# Read and populate plant data (as json)
plant_columns = ['Codice Stabilimento'	,'Descrizione Stabilimento'	,'Edificio'	,'Vettore'	
           ,'POD'	,'Piano'	,'Reparto'	,'Quadro'	,'Descrizione Quadro'	
           ,'Sottoquadro'	,'Descrizione Sottoquadro'	,'Linea'	,'Descrizione Linea'	
           ,'Area Funzionale ENEA'	,'Cod. Funzionale TERA'	,'Tipologia Dispositivo'	,'Codice Modello Dispositivo'
           ,'Taglia Interruttore'	,'Tipologia Misura'	,'Hostname Edge'	,'ID Modbus'	,'Tipo Dispositivo'
           ,'mqtt-topic','modbus-id']
sheet_name = 'Plant'
result = process_excel_sheet(excel_file,sheet_name)
plantData = json.loads(result)


# Read and populate modbus data (as json)
modbus_columns = ['modbus-fc'	,'modbus-address','modbus-format'	
                  , 'Misura', 'Unita di misura',	'modbus-n_registers'	,'mqtt-qos']

sheet_name = 'Modbus'
result = process_excel_sheet(excel_file,sheet_name)
modbusData = json.loads(result)


# define result table columns (combination of previous two tables)

column_names = [ 'modbus-fc' ,'modbus-id',	'modbus-address'	,'modbus-n_registers'	,'modbus-format'	
                ,'mqtt-topic'	,'mqtt-qos'	,'Codice Stabilimento'	,'Descrizione Stabilimento'	,'Edificio'	
                ,'Vettore'	,'POD'	,'Piano'	,'Reparto'	,'Quadro'	,'Descrizione Quadro'	,'Sottoquadro'	
                ,'Descrizione Sottoquadro'	,'Linea'	,'Descrizione Linea'	,'Area Funzionale ENEA'	
                ,'Cod. Funzionale TERA'	,'Tipologia Dispositivo'	,'Codice Modello Dispositivo'	,'Taglia Interruttore'
                ,'Tipologia Misura'	,'Hostname Edge'	,'ID Modbus'
                ,'Tipo Dispositivo'	,'Misura'	,'Unita di misura' ]



# combine the two tables data in a way that for each row on the first table
# we repeat said row to the number of rows that exist on the 2nd table (So we get all the modbus registers for that modbus ID)

totalSize = len(modbusData) * len(plantData)
resultList = [{}] * totalSize


k = 0 # step counter for plantData
j = -1 # step counter for modbusData


for i in range(totalSize):
    j = j + 1
    
    dict =  object()

    resultList[i] = {}
    for modbusColumn in modbus_columns:
        resultList[i][modbusColumn] = modbusData[j][modbusColumn]
    
  
    for plantColumn in plant_columns:
        resultList[i][plantColumn] = plantData[k][plantColumn]

    if j == (len(modbusData) -1):
        j = 0
        if (k != len(plantData)-1):
             k = k + 1

       
# change the index order of result table according to the column_names

updated_list = []
for item in resultList:
    ordered_obj = change_order(item)
    updated_list.append(ordered_obj)
    print(f"\n{ordered_obj}\n")


#------------------------------------------------------------------- #
# Write the results to a json string & Convert the json to CSV file 
# ------------------------------------------------------------------ #
fileName_name = 'result.csv'
jsonString = json.dumps(updated_list)
df = pd.read_json(jsonString)
df.to_csv(fileName_name,sep=';', encoding='utf-8', index=False)


