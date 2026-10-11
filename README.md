# Data Helper

A Python CLI for uploading and downloading Azure Blob datasets, saving dataset snapshot metadata, and viewing YOLO datasets with FiftyOne.

Requires Python 3.12.

```sh
pip install git+https://github.com/selectqoma/data_helper.git
data_helper connection-string
data_helper --help
```

Connection settings are stored in `~/.data_helper_config.json`, including the Azure connection string in plain text. Keep that file private.
