LIBRE-ary Quickstart Guide
==================

This document is designed to help you with set up your first LIBRE-ary. We'll cover installation, configuration, level setup, and using the pythonic LIBREary API.

Installation
------------------
LIBRE-ary requires Python 3. I think any version after 3.4 should work fine, but it's only tested on python 3.6+.

You can install LIBRE-ary two ways: from source, or from `pip`

To install from source, first, obtain the repo:
```
git clone https://github.com/benhg/libre-ary
```
Navigate to the directory
```
cd libre-ary
```
And install with the setup script
```
python3 setup.py install
```
If you want to install with `pip`, the process is somewhat simpler:

```
python3 -m pip install libreary
```

After installing, verify your installation as possible:
```
$ python3
Python 3.7.6 (default, Dec 30 2019, 19:38:28) 
[Clang 11.0.0 (clang-1100.0.33.16)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import libreary
>>> libreary.VERSION
'0.0.1'
```

Configuration
------------------

Most of the configuration process of Libreary is simply making decisions. In this case, we will work with a simple configuration in which there are three adapters and two levels. The three adapters will consist of two LocalAdapters and one S3Adapter. The two levels will be `low` and `medium`, with `low` objects being stored in only the two LocalAdapters, and `medium` objects being stored in one of the LocalAdapters and also in an S3Adapter.

The first decisions you have to make are where to put three critical directories. These are the `config_dir`, the `dropbox_dir`, and the `output_dir`. 

The `config_dir` is the directory in which LIBRE-ary expects configuration files to be stored. Each adapter requires its own config file, so we specify a config directory. Within the config directory, there are some required naming conventions: the main config file should be named `config.json`. This may eventually change, but for now, these conventions are required. All adapter configs should be named `{adapter_id}_config.json`. More detail will be provided on this in the next section of this document.

The `dropbox_dir` and `output_dir`  are both directories that LIBREary will place and expect files in. Objects you wish to ingest should be stored in the `dropbox_dir`, and files retrieved will be stored to the `output_dir`. In principle, they can be the same directory, if you would prefer that. **These directories are both relatively volatile, as LIBREary will frequently overwrite files in these directories. Please do not use these directories to store files as you would on a normal computer system.**

I suggest putting all of these necessary directories within a single directory that you name something informative. 

Next, get a blank copy of the medatada database. This can be found at <URL>. You should be able to download it by simply running the command `wget <URL>`. Save this within your main LIBREary directory. You shouldn't need to open it yourself, or at least not often. LIBREary should handle all of that itself. 

Next, you're ready to write the config file. The config file should be named `<path/to/config/dir>/config.json`. It should be structured as follows:

```{json}
{
    "metadata": {
        "db_file": "Path to local copy of database file"
    },
    "adapters":[ # List of adapter descriptors. We will cover this in the adapter setup section.
    ],
    "options": {
        "dropbox_dir": "path to dropbox directory",
        "output_dir": "Path to output dir, may be same as dropbox",
        "config_dir": "path to config directory"
    },
    "canonical_adapter":"Adapter ID for canonical adapter"
}
```
My config for this example looks like this:
```{json}
{
    "metadata": {
        "db_file": "/Users/ben/desktop/libre-ary/libreary/metadata/md_index.db"
    },
    "adapters":[{
        "type":"LocalAdapter",
        "id": "local2"
    },{
        "type":"LocalAdapter",
        "id": "local1"
    },
    {
        "type":"S3Adapter",
        "id": "s3"
    }
    ],
    "options": {
        "dropbox_dir": "/Users/ben/Desktop/dropbox",
        "output_dir": "/Users/ben/Desktop/retrieval",
        "config_dir": "/Users/ben/desktop/libre-ary/config"
    },
    "canonical_adapter":"local1"
}
```


Adapter and Level Setup
------------------
Next, we have to decide on what adapters we want to use. In the config file, decide on adapter types and adapter identifiers for each one. Each of them is a json object which looks like this:
```{json}
{
"type":"Adapter Type",
"id": "Adapter Name"
}
``` 
The `type` field must contain the name of an adapter that can be instantiated. Currently, the types of adapters included in this library are `LocalAdapter`, `S3Adapter`, and `GoogleDriveAdapter`. Feel free to extend this library (and send us pull requests!) Give each adapter an identifier that is descriptive and easy to remember, and one that contains no spaces.

Next, create a JSON file for each adapter, named `<path/to/config/dir>/{adapter_identifier}.json`. These will all have the same overall structure, but won't necessarily be exactly the same for each adapter. The main structure looks something like this:

```{json}
{
        "metadata": {
            "db_file": "path to db file"
        },
        "adapter": {
            "adapter_identifier": "local1",
            "adapter_type": "LocalAdapter"
            # Other adapter specific info will go here too
        },
        "options": {
            "dropbox_dir": "Path to dropbox dir",
            "output_dir": "Path to output dir"
        },
        "canonical":true # boolean, true if adapter is canonical
    }
```



The `LocalAdapter` requires a `storage_dir` entry in the `adapter` section, which specifies the location on the local system that LIBREary objects should be stored.

The `S3Adapter` requires entries for `region`, `key_file` (path to your AWS keys), `bucket_name` (name of S3 Bucket **You must create this bucket yourself before using the S3Adapter**). My S3Adapter config is below:

This file would be stored as `/path/to/config/dir/s3_config.json`
```{json}
{
        "metadata": {
            "db_file": "/Users/ben/desktop/libre-ary/libreary/metadata/md_index.db"
        },
        "adapter": {
            "bucket_name": "libreary-bucket",
            "adapter_identifier": "s3",
            "adapter_type": "S3Adapter",
            "region": "us-west-2",
            "key_file":"/Users/ben/desktop/aws_keys.json"
        },
        "options": {
            "dropbox_dir": "dropbox",
            "output_dir": "retrieval"
        },
        "canonical":false
    }
```
Now that we have our adapters ready, we need to add some levels. 

First, think about the four parameters we need to create a level: `name, frequency, adapters,` and `copies`. The `name` is the name of your level, `frequency` is a (currently unused) check frequency, `adapters` is a JSON list representing which adapters objects at that level should be stored at (example below), and `copies`is an integer representing how many copies should be stored in that adapter (currently, only 1 is accepted).

An example of an `adapters` entry is below:
```{json}
[
    {
    "id": "local1",
    "type":"LocalAdapter"
    },
    {
    "id": "local2",
    "type":"LocalAdapter"
    }
]
```
(This should be passed in as a python dict)

To do this, we instantiate LIBREary and use the `Libreary.add_level() function`. An example is below:
```
$ python3
Python 3.7.6 (default, Dec 30 2019, 19:38:28) 
[Clang 11.0.0 (clang-1100.0.33.16)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import libreary
>>> l = libreary.Libreary("path/to/config/dir")
>>> levels_dict = [
{
"id": "local1",
"type":"LocalAdapter"
},
{
"id": "local2",
"type":"LocalAdapter"
}
]
>>> l.add_level("low", "1", levels_dict, copies=1)
```
You can now exit Python3. The levels have been added.

Interacting with LIBREary
------------------
Now that your instance of LIBREary is configured, you can start to use it! This guide will go over some of the basic features of LIBREary, but not all of them. All of the functionality is documented in our [ReadTheDocs site](https://LIBRE-ary.readthedocs.io).

This guide goes over interacting with LIBREary as a python object. In the works are a LIBREary web interface and a LIBREary CLI, both of which will provide nice frontends for this sort of thing, and they will have their own quickstart guides. Under the hood, they will be doing things like this.

#### Object Setup

First, we need a LIBREary object. Do the following:
```{python}
$ python3
Python 3.7.6 (default, Dec 30 2019, 19:38:28) 
[Clang 11.0.0 (clang-1100.0.33.16)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import libreary
>>> l = libreary.Libreary("path/to/config/dir")
```
Libreary will have saved your levels from above, if your config dir is the same one.

#### Object Ingestion

First, let's go over ingesting an object. To ingest an object, it must be on the system you're calling LIBREary from. We suggest putting it in the `dropbox_dir`, though this is not strictly required. If you don't put it in the `dropbox_dir`, it will be copied there anyways. 

Libreary-web will always upload to the `dropbox_dir`, but that's a story for another time.

The parameters we need are as follows:
```
:param current_file_path - the current path to the file you wish to ingest
:param levels - a list of names of levels. These levels must exist in the
            `levels` table in the metadata db
:param description - a description of this object. This is useful when you
            want to search for objects later
:param delete_after_store - Boolean. If True, the Ingester will delete the object from dropbox directory after it's stored.
```
This function returns the object's new LIBREary UUID. This can be used for later retrieval and modification. Don't worry if you don't save it now, you can always search for it later.
Using the object we created above:

```{python}
obj_uuid = l.ingest("/path/to/object", ["low"], "Picture of me and my dad", True)
print(obj_uuid)
```

And, that's it! It will now be sent to the adapters that the `low` level contains. In this case, it's the two local adapters.

#### Object Retrieval

Next, we'll describe how to retrieve an object. Object retrieval is simple and easy. You just need to know the object's UUID. To get this, you can save it when ingested, or search later. We'll cover `search()` soon.

To retrieve an object:
```{python}
uuid = "1277ccb6-051c-458d-9250-570b6e085d79"
new_path = l.retrieve(uuid)
print(new_path)
```
This function returns the new path of the object after ithas been retrieved. It will place it in the path `</path/to/output/dir>/{original_filename}.{original_extension}`. **Note that this overwrites anything in the output dir that is named as this object should be. I cannot stress enough how important it is that you don't rely on the output_dir and dropbox_dir as saving their contents.** Objects that have been ingested already, of course, can always be restored using the `Libreary.retrieve()` function.

