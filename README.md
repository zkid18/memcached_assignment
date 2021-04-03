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

## Requirements
- Python 3.x
- Memcached


### Running the script

A step by step series of examples that tell you how to get a development env running

```
python3 memc_load.py --pattern data/*.tsv.gz --dry 

optional arguments:
  -h, --help            show this help message and exit
  -t, --test            
  -l LOG, --log=LOG     
  --dry                 
  --pattern=PATTERN     
  --idfa=IDFA           
  --gaid=GAID           
  --adid=ADID           
  --dvid=DVID           
  -w WORKERS, --workers=WORKERS
  -a ATTEMPTS, --attempts=ATTEMPTS

```

### Install dependecies

```
pip3 install -r requirements.txt
```

Generate `appsinstalled_pb2.py`:

```
cd appsinstalled
protoc --python_out=. appsinstalled.proto
```

### Test run

```
python3 memc_load.py --pattern data/*.tsv.gz --dry  740.15s user 17.93s system 64% cpu 19:38.90 total
```

## Authors

* **Daniil Chepenko** 

## License

This project is licensed under the MIT License