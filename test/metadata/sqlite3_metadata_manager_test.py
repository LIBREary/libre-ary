from libreary.metadata.sqlite3 import SQLite3MetadataManager

config = {
        "db_file": "/Users/ben/Desktop/libre-ary/libreary/metadata/md_index.db"
        }

mm = SQLite3MetadataManager(config)
print(mm)