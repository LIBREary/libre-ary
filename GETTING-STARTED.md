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

Interacting with LIBREary
------------------
