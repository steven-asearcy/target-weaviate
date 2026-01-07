# target-weaviate

Singer target for [Weaviate](https://weaviate.io/) vector database.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting                      | Required | Default      | Description |
|:-----------------------------|:--------:|:------------:|:------------|
| weaviate_url                 | True     | None         | Weaviate instance URL (e.g., https://my-cluster.weaviate.network) |
| weaviate_api_key             | False    | None         | Weaviate API key for authentication. Required for Weaviate Cloud. |
| collection_name              | False    | None         | Weaviate collection name. If not provided, uses the stream name. |
| load_method                  | False    | append-only  | Load method: `append-only`, `upsert`, or `overwrite`. |
| primary_key                  | False    | None         | List of property names to use as composite primary key for upsert operations. Required when load_method is `upsert`. Example: `["id"]` or `["user_id", "timestamp"]` |
| batch_size                   | False    | 100          | Maximum number of records to write in one batch. |
| add_record_metadata          | False    | None         | Additional metadata to add to all records. |
| vectorizer                   | False    | None         | Vectorizer to use when creating a new collection (e.g., `text2vec-cohere`, `text2vec-openai`, `none`). Only used if the collection doesn't exist. |
| create_collection_if_missing | False    | True         | Automatically create the collection if it doesn't exist. |
| stream_maps                  | False    | None         | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config            | False    | None         | User-defined config values to be used within map expressions. |
| flattening_enabled           | False    | None         | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth         | False    | None         | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `target-weaviate --about`

## Load Methods

### `append-only` (Default)
Simply appends all records to the collection. No deduplication or updates.

**Use case**: Event logs, time-series data, append-only datasets

### `upsert`
Updates existing records based on `primary_key` and inserts new records.

**Requirements**: Must specify `primary_key` in configuration.

**Use case**: Dimensional data, master records, updating existing datasets

**Example configuration**:
```yaml
target-weaviate:
  config:
    load_method: upsert
    primary_key: [user_id, timestamp]
```

### `overwrite`
Deletes all existing records in the collection before inserting new data.

**Use case**: Full refresh, small datasets, snapshot replacements

**Warning**: This will delete all data in the collection before loading!

## Supported Python Versions

* 3.9
* 3.10
* 3.11
* 3.12

## Installation

### From PyPI (when published)

```bash
pipx install target-weaviate
```

### From GitHub

```bash
pipx install git+https://github.com/yourusername/target-weaviate.git
```

### For Development

```bash
git clone https://github.com/yourusername/target-weaviate.git
cd target-weaviate
poetry install
```

## Usage

### Executing the Target Directly

```bash
target-weaviate --version
target-weaviate --help

# Test with sample data
cat sample_data.singer | target-weaviate --config config.json
```

### Configuration File Example

```json
{
  "weaviate_url": "https://my-cluster.weaviate.network",
  "weaviate_api_key": "your-api-key",
  "collection_name": "MyCollection",
  "load_method": "upsert",
  "primary_key": ["id"],
  "batch_size": 100,
  "vectorizer": "text2vec-cohere"
}
```

### With Meltano

Add to your `meltano.yml`:

```yaml
plugins:
  loaders:
  - name: target-weaviate
    namespace: target_weaviate
    pip_url: target-weaviate
    executable: target-weaviate
    config:
      weaviate_url: ${WEAVIATE_URL}
      weaviate_api_key: ${WEAVIATE_API_KEY}
      collection_name: MyCollection
      load_method: upsert
      primary_key: [id]
```

Then run:

```bash
meltano run tap-example target-weaviate
```

## Examples

### Example 1: Simple Append

```yaml
config:
  weaviate_url: http://localhost:8080
  collection_name: Documents
  load_method: append-only
```

### Example 2: Upsert with Composite Key

```yaml
config:
  weaviate_url: https://my-cluster.weaviate.network
  weaviate_api_key: ${WEAVIATE_API_KEY}
  collection_name: UserEvents
  load_method: upsert
  primary_key: [user_id, event_type, timestamp]
  batch_size: 200
```

### Example 3: Overwrite with Vectorizer

```yaml
config:
  weaviate_url: https://my-cluster.weaviate.network
  weaviate_api_key: ${WEAVIATE_API_KEY}
  collection_name: Articles
  load_method: overwrite
  vectorizer: text2vec-openai
  create_collection_if_missing: true
```

## Developer Resources

### Initialize Development Environment

```bash
poetry install
```

### Run Tests

```bash
poetry run pytest
```

### Linting

```bash
poetry run ruff check target_weaviate
poetry run ruff format target_weaviate
```

### Testing with Meltano

```bash
# Install meltano
pipx install meltano

# Initialize meltano within this directory
meltano install

# Test invocation
meltano invoke target-weaviate --version

# Run a test pipeline
meltano run tap-example target-weaviate
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache License 2.0

## Resources

- [Weaviate Documentation](https://weaviate.io/developers/weaviate)
- [Singer SDK Documentation](https://sdk.meltano.com)
- [Meltano Documentation](https://docs.meltano.com)

