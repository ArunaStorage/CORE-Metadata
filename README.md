# CORE-Metadata Search
This is the implementation of the Metadata search for the NFDI [CORE-Storage](https://github.com/ScienceObjectsDB/Documentation).
Its purpose is to provide an easy way for searching Metadata attached to CORE-Storage elements like Datasets or ObjectGroups.

## Notes
This is an early draft of the search component which has several limitations.
- Due to the missing AAI within the NFDI project, we are currently only able to index the metadata of a particular configured CORE-Storage project.
- Currently, only valid JSON-Objects are allowed to be indexed.
- The search API is a simple proof-of-concept due to missing specs.

## Architecture
The metadata search consists of two processes, both backed by a MongoDB. The first process is the loader which is
responsible for syncing the MongoDB instance with the actual CORE-Storage content. The second process is the search server, 
which answers search requests.

## MongoDB
The structure of the MongoDB instance is flat. Currently the search uses a single Collection that stores all metadata. The
documents of this collection follow a defined schema:
```
{
  resource_id: String,         // The id of the resource this metadatum belongs to
  resource_type: ResourceType, // The resource type (e.g., Dataset, ObjectGroup, ...)
  key: String,                 // The key used to store the metadata
  labels: Vec<Label>,          // An optional set of key-value pairs
  metadata: {...},             // The actual metadata JSON. 
}
```
This allows as apply a variety of filter combinations easily.
The connection string as well as the database and collection name are configured in `config.toml` 
in the section `[mongo]`.


## Loader
The loader listens to updates in the CORE-Storage and synchronizes the metadata with the MongoDB instance. Therefore,
it subscribes to changes via the [CORE-Storage notifications API](https://github.com/ScienceObjectsDB/core-storage-notifications-example)

The API endpoint, the required API-token as well as the notification group id are configured in `config.toml` in the
section `[notifications]`.

---

**NOTE:**
Due to the early stage of development, the group-id may be set to the value `"CREATE"` which creates a new notification
group upon startup. If set to create, the config also requires a valid `project_id` to subscribe to. 

---

For each received batch of notifications, the loader performs the following actions:
- It starts a MongoDB transaction
- It loops the messages
- For each message it fetches the corresponding CORE-Storage resource (e.g., the dataset)
- From that resource it collects all metadata and transforms it into the internal representation
- It deletes all old metadata stored for the resource and re-inserts the new data
- If this succeeds for all messages, it *commits* the transaction and acknowledges the message batch to the notifications API.
- Otherwise it *aborts* the transaction and sends no acknowledgement.

## Search Server
The search server is a simple gRPC server built using [Tonic](https://docs.rs/tonic/latest). The protobuf definition
of the service is found under `proto/search.proto`.
Currently the server only defines one method (Search). Search receives a `SearchRequest` and returns a `SearchReply`.
The request includes pagination information and possibly filters on one of the following elements:
- key: An exact match on the Metadata Key
- resource_type: An exact match on the CORE-Storage resource type
- A list of so-called field queries.

A field query is basically a key/value pair. The key refers to a field within the meta-data json (e.g., 'provenance.license')
and the value a valid mongo-db query (e.g., `"MIT"` or `{"$regex": ".*GPL.*", "$options": "i"}`).

---

**NOTE:** All filters are combined via a logical AND operations. This means all returned results satisfy all of the
filter conditions.

**NOTE2:** The search server contains several tests that require a running mongodb instance. You can change the test configuration in `settings_test.toml`.

---




