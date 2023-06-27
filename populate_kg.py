from neo4j import GraphDatabase
import os
import pandas as pd
#from config import neo4j_data
host = os.getenv('neo4j_host', default='neo4j://localhost:7687')
password = os.getenv('neo4j_pass', default='neo4j')
user = os.getenv('neo4j_user', default='neo4j')
class Neo4j:
    def __init__(self, localhost = host, password = password, user = user) -> None:
        self.localhost = localhost
        self.password = password
        self.user = user
        self.driver = GraphDatabase.driver(localhost, auth=(user, password))
    
    
        
    def query_kg(self, query, parameters = {}):
        with self.driver.session() as session:
            result = session.run(query, parameters=parameters)
            if result:
                data = result.to_df()
            else:
                data = None
            self.driver.close()
            return data

    def populate_entity_sentence_nodes(self, data_file):
        query = """
            LOAD CSV WITH HEADERS FROM $file AS row
            WITH row WHERE NOT row.entity_1_type IS NULL AND NOT row.entity_2_type IS NULL

            MERGE (sentence:Sentence {text: COALESCE(row.sentence, '')})
            MERGE (entity1:Entity {text: COALESCE(row.entity1, ''), type: COALESCE(row.entity_1_type, '')})
            MERGE (entity2:Entity {text: COALESCE(row.entity2, ''), type: COALESCE(row.entity_2_type, '')})

            MERGE (sentence)-[:HAS_ENTITY]->(entity1)
            MERGE (sentence)-[:HAS_ENTITY]->(entity2)

            MERGE (entity1)-[:RELATION {name:  COALESCE(row.relation, '') }]->(entity2)
        """
        parameters = {'file': f"file:///{data_file}"}
        self.query_kg(query=query, parameters=parameters)
    def populate_entity_properties(self, entities_file):
        query = """LOAD CSV WITH HEADERS FROM $file AS row
                MATCH (e:Entity {text: COALESCE(row.Entity, '')})
                SET e.images =COALESCE (row.Images, '')
                SET e.definition = COALESCE(row.Definition,'')
        
                """
        parameters = {'file': f"file:///{entities_file}"}
        self.query_kg(query=query, parameters=parameters)
        
        
    def process_data_after_population(self):
        query = "MATCH (s:Sentence) SET s.text = rtrim(s.text);"
        self.query_kg(query)

def iterate_files(root_dir):
    db = Neo4j(host, password,user)
    
    for folder_name in os.listdir(root_dir):
        folder_path = os.path.join(root_dir, folder_name)
        
        # Check if it's a directory
        if os.path.isdir(folder_path):
            print(f"Processing folder: {folder_name}")
            
            entities_file = os.path.join(folder_path, "entities.csv")
            data_file = os.path.join(folder_path, "data.csv")
            
            
            if os.path.isfile(data_file):
                print(f"Found data file: {data_file}")
                # Perform operations on data.csv file here
                db.populate_entity_sentence_nodes(data_file)
            if os.path.isfile(entities_file):
                print(f"Found data file: {entities_file}")
                db.populate_entity_properties(entities_file)
                # Perform operations on entities.csv file here
    db.process_data_after_population()
    print("Population Complete...!!")
            

            

    
        
    
if __name__ == "__main__":
    # iterate_files(r'Neo4j_Data//Data')
    db = Neo4j(host, password,user)
    db.process_data_after_population()
    
    
    
    # db = Neo4j("neo4j://65.0.30.45:7687", "neo4j","neo4j")
    
    # data = db.query_kg()
    # print(data)
    
    
