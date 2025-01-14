import os
import pandas as pd


rootdir=os.path.abspath("./data")
files=[file for file in os.listdir(rootdir) if file.endswith(".xlsx")]
for file in files:
    filename=os.path.join(rootdir,file)

nodes={}
relations={}


from neo import *
db=dbneo()

datas=db.QueryNodes()
for data in datas:
    label=str(data["m"].labels).replace(":","")
    name=data["m"]["name"]
    if not label in nodes:
        nodes[label]={}

    nodes[label][name]=data["m"]

datas=db.QueryRelation()
for data in datas:
    relation_name=data["relation_name"]
    startname=data["startname"]
    endname=data["endname"]

    if not relation_name in relations:
        relations[relation_name]=[]

    if not f"{startname}***{endname}" in relations[relation_name]:
        relations[relation_name].append(f"{startname}***{endname}")



filename=os.path.join(rootdir,"2021-2023 FNDDS At A Glance - Foods and Beverages.xlsx")
df=pd.read_excel(filename,header=1)
print(df.columns)
df=df.fillna("")
#df=df.head(100)
for index,row in df.iterrows():
    
    start_label="Food"
    start_name=row["Food code"]

    if not start_label in nodes:
        nodes[start_label]={}
    if not start_name in nodes[start_label]:
        attr={"FoodCode":row["Food code"],"MainFoodDescription":row["Main food description"],"AdditionalFoodDescription":row["Additional food description"]}
        node=db.createNode(start_label,start_name,attr)
        nodes[start_label][start_name]=node

    end_label="FoodCategory"
    end_name=row["WWEIA Category number"]
    
    if not end_label in nodes:
        nodes[end_label]={}
    if not end_name in nodes[end_label]:
        attr={"WWEIACategoryNumber":row["WWEIA Category number"],"WWEIACategoryDescription":row["WWEIA Category description"]}
        node=db.createNode(end_label,end_name,attr)
        nodes[end_label][end_name]=node


    relation_name="BELONGS_TO_CATEGORY"
    if not relation_name in relations:
        relations[relation_name]=[]
    
    if not f"{start_name}***{end_name}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[end_label][end_name],{})
        relations[relation_name].append(f"{start_name}***{end_name}")

    print(index,end=" ")



filename=os.path.join(rootdir,"2021-2023 FNDDS At A Glance - FNDDS Ingredients.xlsx")
df=pd.read_excel(filename,header=1)
print(df.columns)
#df=df.head(100)
for index,row in df.iterrows():
    
    start_label="Food"
    start_name=row["Food code"]

    if not start_label in nodes:
        nodes[start_label]={}
    if not start_name in nodes[start_label]:
        attr={"FoodCode":row["Food code"],"MainFoodDescription":row["Main food description"]}
        node=db.createNode(start_label,start_name,attr)
        nodes[start_label][start_name]=node

    end_label="FoodCategory"
    end_name=row["WWEIA Category number"]
    
    if not end_label in nodes:
        nodes[end_label]={}
    if not end_name in nodes[end_label]:
        attr={"WWEIACategoryNumber":row["WWEIA Category number"],"WWEIACategoryDescription":row["WWEIA Category description"]}
        node=db.createNode(end_label,end_name,attr)
        nodes[end_label][end_name]=node

    relation_name="BELONGS_TO_CATEGORY"
    if not relation_name in relations:
        relations[relation_name]=[]
    
    if not f"{start_name}***{end_name}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[end_label][end_name],{})
        relations[relation_name].append(f"{start_name}***{end_name}")

    #成分

    label3="Ingredient"
    name3=row["Ingredient code"]
    
    if not label3 in nodes:
        nodes[label3]={}
    if not name3 in nodes[label3]:
        attr={"IngredientCode":row["Ingredient code"],
              "IngredientDescription":row["Ingredient description"]}
        node=db.createNode(label3,name3,attr)
        nodes[label3][name3]=node

    relation_name="HAS_Ingredient".upper()
    if not relation_name in relations:
        relations[relation_name]=[]
    attr={
        "SeqNum":row["Seq num"],
        "IngredientWeightg":row["Ingredient weight (g)"],
        "RetentionCode":row["Retention code"],
        "MoistureChange":row[list(df.columns)[-1]]
        }
    if not f"{start_name}***{name3}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[label3][name3],attr)
        relations[relation_name].append(f"{start_name}***{name3}")


    label4="Retention"
    name4=row["Retention code"]
    
    if not label4 in nodes:
        nodes[label4]={}
    if not name4 in nodes[label4]:
        attr={}
        node=db.createNode(label4,name4,attr)
        nodes[label4][name4]=node

    relation_name="HAS_RETENTION"
    if not relation_name in relations:
        relations[relation_name]=[]
    attr={}
    if not f"{name3}***{name4}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[label3][name3],nodes[label4][name4],attr)
        relations[relation_name].append(f"{name3}***{name4}")

    
    print(index,end=" ")




filename=os.path.join(rootdir,"2021-2023 FNDDS At A Glance - Portions and Weights.xlsx")
df=pd.read_excel(filename,header=1)
print(df.columns)
#df=df.head(100)
for index,row in df.iterrows():
    
    start_label="Food"
    start_name=row["Food code"]

    if not start_label in nodes:
        nodes[start_label]={}
    if not start_name in nodes[start_label]:
        attr={"FoodCode":row["Food code"],"MainFoodDescription":row["Main food description"]}
        node=db.createNode(start_label,start_name,attr)
        nodes[start_label][start_name]=node

    end_label="FoodCategory"
    end_name=row["WWEIA Category number"]
    
    if not end_label in nodes:
        nodes[end_label]={}
    if not end_name in nodes[end_label]:
        attr={"WWEIACategoryNumber":row["WWEIA Category number"],"WWEIACategoryDescription":row["WWEIA Category description"]}
        node=db.createNode(end_label,end_name,attr)
        nodes[end_label][end_name]=node

    relation_name="BELONGS_TO_CATEGORY"
    if not relation_name in relations:
        relations[relation_name]=[]
    
    if not f"{start_name}***{end_name}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[end_label][end_name],{})
        relations[relation_name].append(f"{start_name}***{end_name}")

    #成分

    label3="Portion"
    name3=row["Portion description"]
    
    if not label3 in nodes:
        nodes[label3]={}
    if not name3 in nodes[label3]:
        attr={"PortionDescription":row["Portion description"]}
        node=db.createNode(label3,name3,attr)
        nodes[label3][name3]=node

    relation_name="HAS_PORTION".upper()
    if not relation_name in relations:
        relations[relation_name]=[]
    attr={
        "SeqNum":row["Seq num"],
        "PortionWeight":row[list(df.columns)[-1]]
        }
    if not f"{start_name}***{name3}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[label3][name3],attr)
        relations[relation_name].append(f"{start_name}***{name3}")
    print(index,end=" ")



filename=os.path.join(rootdir,"2021-2023 FNDDS At A Glance - FNDDS Nutrient Values.xlsx")
df=pd.read_excel(filename,header=1)
print(df.columns)
#df=df.head(100)
nutrient_columns=[column for column in df.columns if column not in ["Food code","Main food description","WWEIA Category number","WWEIA Category description"]]

for index,row in df.iterrows():
    
    start_label="Food"
    start_name=row["Food code"]

    if not start_label in nodes:
        nodes[start_label]={}
    if not start_name in nodes[start_label]:
        attr={"FoodCode":row["Food code"],"MainFoodDescription":row["Main food description"]}
        node=db.createNode(start_label,start_name,attr)
        nodes[start_label][start_name]=node

    end_label="FoodCategory"
    end_name=row["WWEIA Category number"]
    
    if not end_label in nodes:
        nodes[end_label]={}
    if not end_name in nodes[end_label]:
        attr={"WWEIACategoryNumber":row["WWEIA Category number"],"WWEIACategoryDescription":row["WWEIA Category description"]}
        node=db.createNode(end_label,end_name,attr)
        nodes[end_label][end_name]=node

    relation_name="BELONGS_TO_CATEGORY"
    if not relation_name in relations:
        relations[relation_name]=[]
    
    if not f"{start_name}***{end_name}" in relations[relation_name]:
        db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[end_label][end_name],{})
        relations[relation_name].append(f"{start_name}***{end_name}")

    #成分

    for column in nutrient_columns:


        label3="NutrientValue"
        name3=column
    
        if not label3 in nodes:
            nodes[label3]={}
        if not name3 in nodes[label3]:
            attr={}
            node=db.createNode(label3,name3,attr)
            nodes[label3][name3]=node

        relation_name="HAS_NUTRITION".upper()
        if not relation_name in relations:
            relations[relation_name]=[]
        attr={
            "value":row[column]
            }
        if not f"{start_name}***{name3}" in relations[relation_name]:
            db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[label3][name3],attr)
            relations[relation_name].append(f"{start_name}***{name3}")
        
    print(index,end=" ")




filename=os.path.join(rootdir,"DR1IFF.xlsx")
df=pd.read_excel(filename,header=0)
print(df.columns)
#df=df.head(100)

for index,row in df.iterrows():
    
    start_label="DietaryRecall"
    start_name=row["SEQN"]

    #DR1IFDCD

    if not start_label in nodes:
        nodes[start_label]={}
    if not start_name in nodes[start_label]:
        attr={}
        node=db.createNode(start_label,start_name,attr)
        nodes[start_label][start_name]=node

    end_label="Food"
    end_name=row["DR1IFDCD"]
    
    if not end_label in nodes:
        nodes[end_label]={}
    # if not end_name in nodes[end_label]:
    #     attr={"WWEIACategoryNumber":row["WWEIA Category number"],"WWEIACategoryDescription":row["WWEIA Category description"]}
    #     node=db.createNode(end_label,end_name,attr)
    #     nodes[end_label][end_name]=node

    if end_name in nodes[end_label]:
        relation_name="RECORDS"
        if not relation_name in relations:
            relations[relation_name]=[]
        
        if not f"{start_name}***{end_name}" in relations[relation_name]:
            db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[end_label][end_name],{})
            relations[relation_name].append(f"{start_name}***{end_name}")

    print(index,end=" ")


filename=os.path.join(rootdir,"DEMO_L.xlsx")
df=pd.read_excel(filename,header=0)
print(df.columns)
#df=df.head(100)

for index,row in df.iterrows():
    
    start_label="Demographics"
    start_name=row["SEQN"]


    if not start_label in nodes:
        nodes[start_label]={}
    if not start_name in nodes[start_label]:
        attr={}
        node=db.createNode(start_label,start_name,attr)
        nodes[start_label][start_name]=node

    end_label="DietaryRecall"
    end_name=row["SEQN"]
    
    if not end_label in nodes:
        nodes[end_label]={}

    if end_name in nodes[end_label]:
        relation_name="PARTICIPATES_IN"
        if not relation_name in relations:
            relations[relation_name]=[]
        
        if not f"{start_name}***{end_name}" in relations[relation_name]:
            db.createRelationFromStartToEnd(relation_name,nodes[start_label][start_name],nodes[end_label][end_name],{})
            relations[relation_name].append(f"{start_name}***{end_name}")

    print(index,end=" ")

  


    

