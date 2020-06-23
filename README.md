## Memcached 
Memcached + Multi-threading with multiple threads.



### Prerequisites

- python3
- memcached

#### Install memcached
```
# macOS
brew install memcached
# Unix
sudo apt install memcached
# Run
memcached -l 0.0.0.0:33013,0.0.0.0:33014,0.0.0.0:33015,0.0.0.0:33016
```

### Running the script

A step by step series of examples that tell you how to get a development env running

```
python3 memc_load.py --pattern= data/*.tsv.gz --dry 
```

## Authors

* **Daniil Chepenko** 

## License

This project is licensed under the MIT License