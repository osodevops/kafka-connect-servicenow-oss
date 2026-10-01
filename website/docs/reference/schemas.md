---
title: "Schemas"
description: "Schemaless, string and typed schema modes; converter matrix."
sidebar_position: 5
---

# Schemas

The Table API returns every field as a string, whether the dictionary type is an integer,
a boolean, a date or a reference. The source connector therefore does not guess types
from responses; it offers three explicit modes, chosen with `snow.source.schema.mode`,
and the sink accepts whatever a converter hands it.

## Source schema modes

| Mode | Value type | Schema | Use when |
|---|---|---|---|
| `schemaless` (default) | `Map<String, Object>` with no schema | None | You use `JsonConverter` with `schemas.enable=false`, or you want the exact API response |
| `strings` | Connect `Struct` | Every field optional `STRING`; grows, never shrinks | You want a Schema Registry subject and Confluent Platform source parity (all strings) |
| `typed` (experimental) | Connect `Struct` | Fields typed by `snow.source.typed.fields`; the rest optional `STRING` | You need numbers, booleans and timestamps typed at the source |

The record key is always the `sys_id` as a `STRING` schema. The record timestamp is the
cursor timestamp, and the headers `snow.table`, `snow.instance`,
`snow.source.operation`, `snow.extracted_at` and `snow.schema.mode` are set in every mode.

### `schemaless`

The value is the row as returned, with `null` and absent preserved as such:

```json
{
  "sys_id": "8f4bc0d1c611227a0100e2d3f8a6b9e1",
  "number": "INC0010042",
  "short_description": "Printer on fire",
  "active": "true",
  "urgency": "1",
  "opened_at": "2026-09-29 07:29:10",
  "sys_updated_on": "2026-09-29 07:30:42",
  "sys_mod_count": "3",
  "caller_id": {"link": "https://acme.service-now.com/api/now/table/sys_user/6816f79c...", "value": "6816f79c..."},
  "resolved_at": ""
}
```

Reference fields arrive as `{link, value}` objects unless
`snow.table.<alias>.exclude.reference.link=true`, which flattens them to the `sys_id`
string. An empty field is an empty string, as the API returns it.

### `display.value=all`

With `snow.table.<alias>.display.value=all` every field becomes an object with a stable
shape, in every mode:

```json
{
  "urgency": {"value": "1", "display_value": "1 - High"},
  "caller_id": {"value": "6816f79c...", "display_value": "Abel Tuter", "link": "https://acme.service-now.com/api/now/table/sys_user/6816f79c..."},
  "opened_at": {"value": "2026-09-29 07:29:10", "display_value": "29/09/2026 08:29:10"}
}
```

`link` is present only for reference fields and only when
`exclude.reference.link=false`. In `strings` and `typed` modes the object is a nested
`Struct` with optional `value`, `display_value` and `link` fields. `display.value=true`
replaces each value with its display value and keeps the flat shape; note that display
values depend on the integration user's locale and time zone.

### `strings`

Every field the connector has ever seen for the table becomes an optional `STRING` field
of the value `Struct`. The schema is built from the first page and **only grows**: a field
that stops appearing (because it was removed from the projection or hidden by an ACL)
stays in the schema and is `null` on new records, so registered schemas stay backward
compatible. Adding a field to `snow.table.<alias>.fields` adds an optional field, which
is also backward compatible under Schema Registry's default compatibility.

### `typed`

`snow.source.typed.fields` lists the fields to type, as a comma-separated
`field:type` mapping; every field not listed stays an optional `STRING`:

```properties
snow.source.schema.mode=typed
snow.source.typed.fields=active:boolean,urgency:int32,sys_mod_count:int32,reassignment_count:int64,opened_at:timestamp,resolved_at:timestamp,closed_at:date,business_duration:string
```

| Type | Connect schema | Coercion from the API string |
|---|---|---|
| `string` | optional `STRING` | as is |
| `boolean` | optional `BOOLEAN` | `true`/`false`, `1`/`0` |
| `int32`, `int64` | optional `INT32`, `INT64` | decimal integer; empty string is `null` |
| `float64` | optional `FLOAT64` | decimal number |
| `decimal` | optional `Decimal` logical type | decimal string; scale from the value |
| `timestamp` | optional `Timestamp` logical type (epoch millis) | `yyyy-MM-dd HH:mm:ss` in UTC |
| `date` | optional `Date` logical type | `yyyy-MM-dd` |

`snow.source.schema.evolution` (default `fail`) governs what happens when a value does
not fit its declared type or a typed field disappears:

- `fail`: the row is a bad row (see `snow.source.bad.row.behavior` in
  [Error handling](error-handling.md)).
- `backward`: the field is emitted as `null` and the schema is unchanged; a warning names
  the field once per table.
- `permissive`: the field is re-declared as optional `STRING` in a new schema version and
  the raw value is emitted; consumers must tolerate the type change.

The mapping is explicit by design. A dictionary-driven snapshot (typing every field from
`sys_dictionary`) is planned for a later release; until then `typed` is marked
experimental because a mapping mistake surfaces as bad rows rather than at validation.

### The envelope

`snow.source.emit.envelope=true` wraps the value in a change-event style envelope in every
mode:

```json
{
  "before": null,
  "after": {"sys_id": "8f4bc0d1...", "number": "INC0010042", "...": "..."},
  "source": {"table": "incident", "instance": "acme.service-now.com"},
  "op": "u",
  "ts_ms": 1790667042000
}
```

`before` is always `null` and `op` is always `u`, because the Table API cannot supply a
prior image or distinguish a create from an update at read time. The envelope exists for
consumers that already understand this shape from other connectors.

## Sink input handling

The sink maps whatever the value converter produces into a JSON body for the Table API:

| Input value | Handling |
|---|---|
| Connect `Struct` (Avro, JSON Schema, Protobuf converters, or `JsonConverter` with `schemas.enable=true`) | Each field becomes a body property; logical types are rendered as ServiceNow expects |
| `Map` (`JsonConverter` with `schemas.enable=false`) | Each entry becomes a body property; values are rendered by their runtime type |
| Primitive (a `String` holding a JSON object) | Parsed as a JSON object; any other primitive is a record error |
| `null` value | A `DELETE` in `key_value` mode; a record error in `fixed` mode unless the fixed operation is `DELETE` |

Field-level rules, applied in this order:

1. `snow.sink.field.rename` (`from:to,...`), then `snow.sink.field.allowlist` or
   `snow.sink.field.denylist`. Custom field names starting with `u_` are passed through
   unchanged; the sink never strips or adds the prefix.
2. Reserved fields are stripped: `sys_id` (it is the path, not the body),
   `sys_created_on`, `sys_updated_on`, `sys_mod_count`, `sys_created_by`,
   `sys_updated_by`, and the operation header's name if it appears as a field.
3. `snow.sink.null.behavior` (default `omit`): `omit` leaves the property out of the body
   (a `PATCH` leaves the field untouched); `clear` sends an empty string, which clears the
   field in ServiceNow; `reject` makes a null a record error.
4. `snow.sink.nested.behavior` (default `reject`): a `Struct`, `Map` or array value is
   rejected with the full field path in the error; `flatten` joins nested keys with
   `snow.sink.nested.flatten.delimiter` (default `_`), so `location.city` becomes
   `location_city`; `stringify` sends the nested value as a JSON string. The default is
   `reject` because ServiceNow silently stores an unrecognised object as a Java map
   string, which is never what anyone wants.
5. `snow.sink.unknown.field.behavior` (default `fail`): fields not in the target table's
   dictionary (looked up through `TableMetadataClient`, cached) are a record error,
   `drop`ped, `report`ed to the error reporter while the rest of the record is written, or
   `passthrough` sent as is, for instances where the user cannot read `sys_dictionary`.

Logical types render as: `Timestamp` to `yyyy-MM-dd HH:mm:ss` in UTC, `Date` to
`yyyy-MM-dd`, `Time` to `HH:mm:ss`, `Decimal` to a plain decimal string, booleans to
`true`/`false`, numbers to their decimal representation, bytes to Base64.

## Converter matrix

| Converter | `schemas.enable` | Source output | Sink input |
|---|---|---|---|
| `org.apache.kafka.connect.json.JsonConverter` | `false` | `schemaless`: plain JSON object. `strings`/`typed`: the Struct rendered as plain JSON (schema dropped) | `Map` path |
| `org.apache.kafka.connect.json.JsonConverter` | `true` | `strings`/`typed`: `{"schema": ..., "payload": ...}` envelope. `schemaless`: rejected, the converter needs a schema | `Struct` path (round-trips the source's `strings` output) |
| `org.apache.kafka.connect.storage.StringConverter` | n/a | Not suitable for values; fine for the `sys_id` key | A `String` holding JSON: parsed |
| `io.confluent.connect.avro.AvroConverter` | n/a | `strings`/`typed` registered as Avro; `schemaless` rejected | `Struct` path |
| `io.confluent.connect.json.JsonSchemaConverter` | n/a | as Avro | `Struct` path |
| `io.confluent.connect.protobuf.ProtobufConverter` | n/a | as Avro | `Struct` path |

The Avro, JSON Schema and Protobuf converters are published by Confluent under the
Confluent Community Licence and are **not bundled** in the plugin ZIPs; install them on
the worker separately, or use Apicurio Registry's Apache-2.0 converters, which behave the
same way. All of them deliver a `Struct` with a `Schema` to the sink, so the sink's
`Struct` path covers every registry format without special cases. The
`ConverterMatrixTest` in the sink module runs the schemaless `Map`, `Struct` with
`Schema`, and `JsonConverter` round-trip paths with logical types on every build.
