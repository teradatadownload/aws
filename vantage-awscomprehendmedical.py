# Import Statements
import boto3
import teradataml as tdml
from teradataml import create_context, remove_context
import pandas as pd
import getpass

# Login to Teradata Vantage
create_context(host = '100.00.00.00', username = 'testUsr', password = getpass.getpass(prompt = 'Password:'))

# Retrive input data
source_data = tdml.DataFrame('testUsr.healthnotes')

# Convert to Pandas DataFrame
source_data_pd = source_data.to_pandas()

# Call Comprehend Medical Service
client = boto3.client('comprehendmedical')

# Initialize output DataFrame
dfObj=pd.DataFrame() 

# Interate through each row of input data
for i, j in source_data_pd.iterrows(): 
    # Get content of 'assessment' column
    str_val = j['assessment']
    result = client.detect_entities_v2(Text=str_val)
    
    # Get content of 'encounter_date' column
    encounter_date = j['encounter_date']
   
   # Going through each array entry to prepare output data
    entities = result['Entities']
    for entity in entities:
        # Adding patient_key and encounter_date to each entry. This step is optional
        entity.update( {'patient_key' : i} )
        entity.update( {'encounter_date' : encounter_date})
        # Finish updating each entry with patient_key and encounter_date
        
        # Convert dictionary "Traits" to string
        entity.update({'Traits': str(entity["Traits"])})
        
        # Convert dictionary "Attributes" to string
        if "Attributes" in entity:
            attr = entity["Attributes"]
        else:
            attr = 'NaN'
        entity.update({'Attributes': str(attr)})
        
        # Adding updated result entry to DataFrame
        dfObj = dfObj.append(entity, ignore_index=True)        

''' ICD10CM Method
dfObj=pd.DataFrame() 

for i, j in source_data_pd.iterrows(): 
    result = client.infer_icd10_cm(Text=j['assessment'])

    encounter_date = j['encounter_date']
   
    entities = result['Entities']

    for entity in entities:
        entity.update( {'patient_key' : i} )
        entity.update( {'encounter_date' : encounter_date})
        
        entity.update({'Traits': str(entity["Traits"])})
        
        if "Attributes" in entity:
            attr = entity["Attributes"]
        else:
            attr = 'NaN'
        entity.update({'Attributes': str(attr)})
        
        entity.update({'ICD10CMConcepts' : str(entity["ICD10CMConcepts"])})
        dfObj = dfObj.append(entity, ignore_index=True)      
'''

''' RxNorm Method
dfObj=pd.DataFrame() 

for i, j in source_data_pd.iterrows(): 
    result = client.infer_rx_norm(Text=j['assessment'])

    encounter_date = j['encounter_date']
   
    entities = result['Entities']

    for entity in entities:
        entity.update( {'patient_key' : i} )
        entity.update( {'encounter_date' : encounter_date})
        
        val = entity["Traits"]
        entity.update({'Traits': str(val)})
        
        if "Attributes" in entity:
            attr = entity["Attributes"]
        else:
            attr = 'NaN'
        entity.update({'Attributes': str(attr)})
        
        entity.update({'RxNormConcepts' : str(entity["RxNormConcepts"])})
        dfObj = dfObj.append(entity, ignore_index=True)      
'''

# Write results back to Teradata Vantage
tdml.copy_to_sql(df = dfObj, table_name = "entity_assessment", schema_name='testUsr', if_exists = "replace")

remove_context()
