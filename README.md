# Orna Codex Indexer

## Installion

```shell
pip install -r requirements.txt
```

## Usage

```shell
$ python3 indexer.py -h
usage: Orna Codex Indexer [-h] [--clean] [--lang LANG]
                          [--data-dir DATA_DIR]
                          [--fetch-meta | --fetch-codex | --parse-codex | --check-miss | --build-index | --all]

options:
  -h, --help           show this help message and exit
  --clean              remove data before fetch
  --lang LANG          download language
  --data-dir DATA_DIR  data directory
  --fetch-meta         fetch meta data
  --fetch-codex        fetch codex data
  --parse-codex        parse codex data
  --check-miss         check missing codex
  --build-index        build codex index
  --all                fetch and parse all data
```

## License

MIT License
