// DO NOT EDIT : This file is generated by chevron
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// presto_protocol.prolog.cpp
//

// This file is generated DO NOT EDIT @generated

#include "presto_cpp/presto_protocol/connector/clp/presto_protocol_clp.h"
using namespace std::string_literals;

namespace facebook::presto::protocol::clp {

void to_json(json& j, const ClpTransactionHandle& p) {
  j = json::array();
  j.push_back(p._type);
  j.push_back(p.instance);
}

void from_json(const json& j, ClpTransactionHandle& p) {
  j[0].get_to(p._type);
  j[1].get_to(p.instance);
}
} // namespace facebook::presto::protocol::clp
namespace facebook::presto::protocol::clp {
ClpColumnHandle::ClpColumnHandle() noexcept {
  _type = "clp";
}

void to_json(json& j, const ClpColumnHandle& p) {
  j = json::object();
  j["@type"] = "clp";
  to_json_key(
      j, "columnName", p.columnName, "ClpColumnHandle", "String", "columnName");
  to_json_key(
      j,
      "originalColumnName",
      p.originalColumnName,
      "ClpColumnHandle",
      "String",
      "originalColumnName");
  to_json_key(
      j, "columnType", p.columnType, "ClpColumnHandle", "Type", "columnType");
  to_json_key(j, "nullable", p.nullable, "ClpColumnHandle", "bool", "nullable");
}

void from_json(const json& j, ClpColumnHandle& p) {
  p._type = j["@type"];
  from_json_key(
      j, "columnName", p.columnName, "ClpColumnHandle", "String", "columnName");
  from_json_key(
      j,
      "originalColumnName",
      p.originalColumnName,
      "ClpColumnHandle",
      "String",
      "originalColumnName");
  from_json_key(
      j, "columnType", p.columnType, "ClpColumnHandle", "Type", "columnType");
  from_json_key(
      j, "nullable", p.nullable, "ClpColumnHandle", "bool", "nullable");
}
} // namespace facebook::presto::protocol::clp
namespace facebook::presto::protocol::clp {
ClpSplit::ClpSplit() noexcept {
  _type = "clp";
}

void to_json(json& j, const ClpSplit& p) {
  j = json::object();
  j["@type"] = "clp";
  to_json_key(j, "path", p.path, "ClpSplit", "String", "path");
  to_json_key(j, "kqlQuery", p.kqlQuery, "ClpSplit", "String", "kqlQuery");
}

void from_json(const json& j, ClpSplit& p) {
  p._type = j["@type"];
  from_json_key(j, "path", p.path, "ClpSplit", "String", "path");
  from_json_key(j, "kqlQuery", p.kqlQuery, "ClpSplit", "String", "kqlQuery");
}
} // namespace facebook::presto::protocol::clp
namespace facebook::presto::protocol::clp {
ClpTableHandle::ClpTableHandle() noexcept {
  _type = "clp";
}

void to_json(json& j, const ClpTableHandle& p) {
  j = json::object();
  j["@type"] = "clp";
  to_json_key(
      j,
      "schemaTableName",
      p.schemaTableName,
      "ClpTableHandle",
      "SchemaTableName",
      "schemaTableName");
  to_json_key(
      j, "tablePath", p.tablePath, "ClpTableHandle", "String", "tablePath");
}

void from_json(const json& j, ClpTableHandle& p) {
  p._type = j["@type"];
  from_json_key(
      j,
      "schemaTableName",
      p.schemaTableName,
      "ClpTableHandle",
      "SchemaTableName",
      "schemaTableName");
  from_json_key(
      j, "tablePath", p.tablePath, "ClpTableHandle", "String", "tablePath");
}
} // namespace facebook::presto::protocol::clp
namespace facebook::presto::protocol::clp {
ClpTableLayoutHandle::ClpTableLayoutHandle() noexcept {
  _type = "clp";
}

void to_json(json& j, const ClpTableLayoutHandle& p) {
  j = json::object();
  j["@type"] = "clp";
  to_json_key(
      j, "table", p.table, "ClpTableLayoutHandle", "ClpTableHandle", "table");
  to_json_key(
      j, "kqlQuery", p.kqlQuery, "ClpTableLayoutHandle", "String", "kqlQuery");
  to_json_key(
      j,
      "metadataFilterQuery",
      p.metadataFilterQuery,
      "ClpTableLayoutHandle",
      "String",
      "metadataFilterQuery");
}

void from_json(const json& j, ClpTableLayoutHandle& p) {
  p._type = j["@type"];
  from_json_key(
      j, "table", p.table, "ClpTableLayoutHandle", "ClpTableHandle", "table");
  from_json_key(
      j, "kqlQuery", p.kqlQuery, "ClpTableLayoutHandle", "String", "kqlQuery");
  from_json_key(
      j,
      "metadataFilterQuery",
      p.metadataFilterQuery,
      "ClpTableLayoutHandle",
      "String",
      "metadataFilterQuery");
}
} // namespace facebook::presto::protocol::clp
