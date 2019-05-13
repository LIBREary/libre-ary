# LIBREary Metadata Database Schema

## Schema

### Table 1: Resources

Table name: resources
Fields: INT id, TEXT path, TEXT level, TEXT name, TEXT checksum

### Table 2: Criticality Levels

Table name: levels
Fields: INT id, TEXT name, TEXT frequency, TEXT adapters, INT copies

### Table 3: Entries per resource

Each copy of a resource will get an entry in this table

Table name: copies
Fields: INT copy_id, INT resource_id, TEXT adapter, TEXT locator, TEXT checksum

Eventually, we will need to work out how we're going to back up the metadata db across different places

We should be clever and find a way to use the adapters we already have to back up. The problem is that we can't really trust our backups because it's so frequently updated. 

NOTE: look into synchronization of databases across different services. (won't happen for a while)


