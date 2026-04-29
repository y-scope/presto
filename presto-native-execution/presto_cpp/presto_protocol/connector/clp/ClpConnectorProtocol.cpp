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

#include "presto_cpp/presto_protocol/connector/clp/ClpConnectorProtocol.h"

#include <folly/lang/Bits.h>
#include <sstream>

namespace facebook::presto::protocol::clp {

namespace {

// Java's CodecUtils.writeUtf8String: 2-byte big-endian length + standard UTF-8 bytes.
std::string readUTF(std::istringstream& in) {
  uint16_t length;
  in.read(reinterpret_cast<char*>(&length), sizeof(length));
  VELOX_CHECK(false == in.fail(), "Failed to read UTF length: insufficient bytes");
  length = folly::Endian::big(length);

  std::string result(length, '\0');
  in.read(&result[0], length);
  VELOX_CHECK(false == in.fail(), "Failed to read UTF body: insufficient bytes");
  return result;
}

int32_t readInt(std::istringstream& in) {
  uint32_t value;
  in.read(reinterpret_cast<char*>(&value), sizeof(value));
  VELOX_CHECK(false == in.fail(), "Failed to read int: insufficient bytes");
  value = folly::Endian::big(value);
  return static_cast<int32_t>(value);
}

bool readBoolean(std::istringstream& in) {
  char byte;
  in.read(&byte, sizeof(byte));
  VELOX_CHECK(false == in.fail(), "Failed to read boolean: insufficient bytes");
  return byte != 0;
}

} // namespace

// ClpColumnHandle
// Wire format (Java ClpColumnHandleCodec):
//   columnName          : CodecUtils.writeUtf8String
//   originalColumnName  : CodecUtils.writeUtf8String
//   columnTypeSignature : CodecUtils.writeUtf8String

void ClpConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ColumnHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<ClpColumnHandle>();

  handle->columnName = readUTF(in);
  handle->originalColumnName = readUTF(in);
  handle->columnType = readUTF(in);
  handle->_type = "clp";

  proto = handle;
}

// ClpSplit
// Wire format (Java ClpSplitCodec):
//   path            : CodecUtils.writeUtf8String
//   typeOrdinal     : writeInt (0 = ARCHIVE, 1 = IR)
//   kqlQueryPresent : writeBoolean
//   [kqlQuery]      : CodecUtils.writeUtf8String, if present

void ClpConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorSplit>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<ClpSplit>();

  handle->path = readUTF(in);
  int typeOrdinal = readInt(in);
  VELOX_CHECK(
      typeOrdinal >= 0 && typeOrdinal <= 1,
      "Invalid ClpSplit type ordinal: {}",
      typeOrdinal);
  handle->type = static_cast<SplitType>(typeOrdinal);
  if (readBoolean(in)) {
    handle->kqlQuery = std::make_shared<String>(readUTF(in));
  }
  handle->_type = "clp";

  proto = handle;
}

// ClpTableHandle
// Wire format (Java ClpTableHandleCodec):
//   schemaName : CodecUtils.writeUtf8String
//   tableName  : CodecUtils.writeUtf8String
//   tablePath  : CodecUtils.writeUtf8String

static void deserializeClpTableHandle(std::istringstream& in, ClpTableHandle& table) {
  table.schemaTableName.schema = readUTF(in);
  table.schemaTableName.table = readUTF(in);
  table.tablePath = readUTF(in);
}

void ClpConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorTableHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<ClpTableHandle>();

  deserializeClpTableHandle(in, *handle);
  handle->_type = "clp";

  proto = handle;
}

// ClpTableLayoutHandle
// Wire format (Java ClpTableLayoutHandleCodec):
//   [table handle]       : deserialized by deserializeClpTableHandle
//   kqlQueryPresent      : writeBoolean
//   [kqlQuery]           : CodecUtils.writeUtf8String, if present
//   metadataSqlPresent   : writeBoolean
//   [metadataSql]        : CodecUtils.writeUtf8String, if present

void ClpConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorTableLayoutHandle>& proto) const {
  std::istringstream in(binaryData);
  auto handle = std::make_shared<ClpTableLayoutHandle>();

  deserializeClpTableHandle(in, handle->table);

  // kqlQuery (optional)
  if (readBoolean(in)) {
    handle->kqlQuery = std::make_shared<String>(readUTF(in));
  }

  // metadataFilterQuery (optional)
  if (readBoolean(in)) {
    handle->metadataFilterQuery =
        std::make_shared<String>(readUTF(in));
  }
  handle->_type = "clp";

  proto = handle;
}

// ClpTransactionHandle
// Wire format (Java ClpTransactionHandleCodec): 0 bytes (empty payload).

void ClpConnectorProtocol::deserialize(
    const std::string& binaryData,
    std::shared_ptr<ConnectorTransactionHandle>& proto) const {
  auto handle = std::make_shared<ClpTransactionHandle>();
  handle->instance = {};
  handle->_type = "clp";

  proto = handle;
}

} // namespace facebook::presto::protocol::clp
