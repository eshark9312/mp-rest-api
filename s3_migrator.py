import pymongo
import gzip
import json
import os

from pymongo.collection import Collection
from typing import Union, TextIO, BinaryIO
from arg_enums import ExportTypes, Bundle_col

client = pymongo.MongoClient()
db = client.matproj

class Matproj_db_migrator :
    """
        This class migrates the downloaded database data from files on disk 
        into collections of the mongodb database
    """
    client : None
    db : None

    def __init__(self) -> None:
        """
            Initialize the migrator with db
        """
        self.client = pymongo.MongoClient()
        self.db = self.client.matproj
        self.db_s3 = self.client.matproj_s3
        return

    def add_s3_collections_to_db(self, collection_name:str = 'summary', collection_base_path:str = 'collections', sub_path:str = ''):
        collection = self.db_s3[collection_name]
        num_docs_added = 0
        for folder in os.listdir(f"{collection_base_path}/{collection_name}/{sub_path}"):
            if not folder.startswith('manifest'):
                for gz_file in os.listdir(f"{collection_base_path}/{collection_name}/{sub_path}/{folder}"):
                    if gz_file.endswith('gz'):
                        path_to_file = f"{collection_base_path}/{collection_name}/{sub_path}/{folder}/{gz_file}"
                        num_docs_added = self.add_data_to_db(path_to_file, ExportTypes.Gzip, collection, False, 1000, num_docs_added)
        print("")
        print(f"{num_docs_added} documents added to {collection_name}")
        return

    def add_data_to_db(self,
            path_to_file : str,
            dataType : ExportTypes,
            collection : Collection,
            from_scratch : bool = False,
            add_by_docs_num : int = 1000,
            skip_docs_num : int = 0
    ) -> None:
        """
            This function reads data from file and add those into db
        """
        if from_scratch:
            collection.drop()
        f = None
        match dataType:
            case ExportTypes.Json:
                f = open(path_to_file, 'r')
            case ExportTypes.Gzip:
                f = gzip.open(path_to_file, 'rb')
            case _:
                print("Unsupported file type of data")
                return
        json_list = []
        num_docs = skip_docs_num
        for line in f:
            num_docs += 1
            print(f"Number of Imported Documents : {num_docs}\r", end="")
            json_data = json.loads(line)
            # del json_data['_id']
            json_list.append(json_data)
            if len(json_list) == add_by_docs_num:
                collection.insert_many(json_list)
                json_list = []
        if len(json_list) > 0:
            collection.insert_many(json_list)
        f.close()
        return num_docs

def migrate_s3_collections():
    collections_list = [
        # {"collection_name": "absorption",            "collection_path": "collections"},
        # {"collection_name": "alloys",                "collection_path": "collections"},
        # {"collection_name": "bonds",                 "collection_path": "collections"},
        # {"collection_name": "chemenv",               "collection_path": "collections"},
        # {"collection_name": "conversion-electrodes", "collection_path": "collections"},
        # {"collection_name": "dielectric",            "collection_path": "collections"},
        # {"collection_name": "elasticity",            "collection_path": "collections"},
        # {"collection_name": "electronic-structure",  "collection_path": "collections"},
        # {"collection_name": "grain-boundaries",      "collection_path": "collections"},
        # {"collection_name": "insertion-electrodes",  "collection_path": "collections"},
        # {"collection_name": "magnetism",             "collection_path": "collections"},
        {"collection_name": "materials",             "collection_path": "mp_collections_2024_12_28"},
        # {"collection_name": "molecules",             "collection_path": "collections"},
        # {"collection_name": "oxi-states",            "collection_path": "collections"},
        # {"collection_name": "piezoelectric",         "collection_path": "collections"},
        # {"collection_name": "provenance",            "collection_path": "collections"},
        # {"collection_name": "robocrys",              "collection_path": "mp_collections_2024_12_28"},
        # {"collection_name": "similarity",            "collection_path": "collections"},
        # {"collection_name": "summary",               "collection_path": "mp_collections_2024_12_28"},
        # {"collection_name": "synth-descriptions",    "collection_path": "collections"},
        # {"collection_name": "thermo",                "collection_path": "collections", "sub_path": "thermo_type=GGA_GGA+U"},
        # {"collection_name": "thermo",                "collection_path": "collections", "sub_path": "thermo_type=GGA_GGA+U_R2SCAN"},
        # {"collection_name": "thermo",                "collection_path": "collections", "sub_path": "thermo_type=R2SCAN"},
        # {"collection_name": "xas",                   "collection_path": "collections", "sub_path": "spectrum_type=EXAFS"},
        # {"collection_name": "xas",                   "collection_path": "collections", "sub_path": "spectrum_type=XAFS"},
        # {"collection_name": "xas",                   "collection_path": "collections", "sub_path": "spectrum_type=XANES"},
    ]
    migrator = Matproj_db_migrator()
    for collection in collections_list:
        migrator.add_s3_collections_to_db(collection_name=collection['collection_name'],
                                          collection_base_path=collection['collection_path'],
                                          sub_path=collection['sub_path'] if 'sub_path' in collection else '')
    return

def migrate_props(path2props_dir : str) -> None :
    props_list = [
        'summary',
        # 'entries',
        # 'provenance',
        # 'magnetism',
        # 'electronic_structure',
        # 'dielectric',
        # 'piezo',
        # 'elasticity',
        # 'phonon',
        # 'eos'
        ]
    migrator = Matproj_db_migrator()
    for prop in props_list:
        collection = migrator.db[prop]
        path2file = path2props_dir + prop + '.json'
        migrator.add_data_to_db(path2file,ExportTypes.Json,collection,True)
        print("Added data : " + prop)
    return

def migrate_bundles(path2dir : str, col: Bundle_col = Bundle_col.Bandstructure, bundles_list : list = range(91)) -> None :
    migrator = Matproj_db_migrator()
    collection = migrator.db[col.value]
    for i in bundles_list:
        print('Reading ' + col.value + '_bundle_' + str(i))
        file_name = col.value + "_bundle_0" + str(i) + ".gz" if i < 10 else col.value + "_bundle_" + str(i) + ".gz" 
        path2file = path2dir + file_name
        if i == 0 :
            print("Initializing the collection : " + col.value)
            migrator.add_data_to_db(path2file,ExportTypes.Gzip,collection,True)
        else :
            migrator.add_data_to_db(path2file,ExportTypes.Gzip,collection,False)
        print("Added " + col.value + "_bundel_" + str(i) + " into " + col.value)
    return 

def main() : 
    # migrate several props data
    migrate_s3_collections()
    # path2props_dir = 'db_rar/'
    # migrate_props(path2props_dir)
    
    # migrate bandstrcture data
    path2bs_dir = 'db_rar/bs_bundle/'
    path2dos_dir = 'db_rar/dos_bundle/'
    path2pdos_dir = 'db_rar/pdos_bundle/'
    #migrate_bundles(path2dir = path2bs_dir, col = Bundle_col.Bandstructure, bundles_list=[0,1])
    #migrate_bundles(path2dir = path2dos_dir, col = Bundle_col.DOS, bundles_list=[0,1])
    # migrate_bundles(path2dir = path2pdos_dir, col = Bundle_col.PDOS, bundles_list=[0,1])

if __name__ == "__main__":
    main()