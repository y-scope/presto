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

#include <folly/lang/Bits.h>
#include <gtest/gtest.h>
#include <cstring>
#include <sstream>
#include "presto_cpp/presto_protocol/connector/clp/ClpConnectorProtocol.h"

using namespace facebook::presto;
using namespace facebook::presto::protocol;
using namespace facebook::presto::protocol::clp;

namespace {

void writeUtf8String(std::ostringstream& oss, const std::string& s) {
  uint16_t len = folly::Endian::big(static_cast<uint16_t>(s.length()));
  oss.write(reinterpret_cast<const char*>(&len), 2);
  oss.write(s.data(), s.length());
}

void writeInt(std::ostringstream& oss, int32_t v) {
  uint32_t u = folly::Endian::big(static_cast<uint32_t>(v));
  oss.write(reinterpret_cast<const char*>(&u), 4);
}

void writeBoolean(std::ostringstream& oss, bool b) {
  char byte = b ? 1 : 0;
  oss.write(&byte, 1);
}

} // namespace

class ClpConnectorProtocolTest : public ::testing::Test {
 protected:
  ClpConnectorProtocol protocol;
};

TEST_F(ClpConnectorProtocolTest, TestColumnHandleDeserialization) {
  std::ostringstream oss;
  writeUtf8String(oss, "c_custkey");
  writeUtf8String(oss, "customer_key");
  writeUtf8String(oss, "bigint");

  std::string binaryData = oss.str();

  std::shared_ptr<ColumnHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto clpHandle = std::dynamic_pointer_cast<ClpColumnHandle>(handle);
  ASSERT_NE(clpHandle, nullptr);
  EXPECT_EQ(clpHandle->columnName, "c_custkey");
  EXPECT_EQ(clpHandle->originalColumnName, "customer_key");
  EXPECT_EQ(clpHandle->columnType, "bigint");
}

TEST_F(ClpConnectorProtocolTest, TestTableHandleDeserialization) {
  std::ostringstream oss;
  writeUtf8String(oss, "clp");
  writeUtf8String(oss, "logs");
  writeUtf8String(oss, "/data/clp/logs");

  std::string binaryData = oss.str();

  std::shared_ptr<ConnectorTableHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto clpHandle = std::dynamic_pointer_cast<ClpTableHandle>(handle);
  ASSERT_NE(clpHandle, nullptr);
  EXPECT_EQ(clpHandle->schemaTableName.schema, "clp");
  EXPECT_EQ(clpHandle->schemaTableName.table, "logs");
  EXPECT_EQ(clpHandle->tablePath, "/data/clp/logs");
}

TEST_F(ClpConnectorProtocolTest, TestTableLayoutHandleWithoutOptionalFields) {
  std::ostringstream oss;
  // Embedded ClpTableHandle fields
  writeUtf8String(oss, "clp");
  writeUtf8String(oss, "logs");
  writeUtf8String(oss, "/data/clp/logs");

  // kqlQuery absent, metadataFilterQuery absent
  writeBoolean(oss, false);
  writeBoolean(oss, false);

  std::string binaryData = oss.str();

  std::shared_ptr<ConnectorTableLayoutHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto clpHandle = std::dynamic_pointer_cast<ClpTableLayoutHandle>(handle);
  ASSERT_NE(clpHandle, nullptr);
  EXPECT_EQ(clpHandle->table.schemaTableName.schema, "clp");
  EXPECT_EQ(clpHandle->table.schemaTableName.table, "logs");
  EXPECT_EQ(clpHandle->table.tablePath, "/data/clp/logs");
  EXPECT_EQ(clpHandle->kqlQuery, nullptr);
  EXPECT_EQ(clpHandle->metadataFilterQuery, nullptr);
}

TEST_F(ClpConnectorProtocolTest, TestTableLayoutHandleWithOptionalFields) {
  std::ostringstream oss;
  // Embedded ClpTableHandle fields
  writeUtf8String(oss, "clp");
  writeUtf8String(oss, "logs");
  writeUtf8String(oss, "/data/clp/logs");

  // kqlQuery present
  writeBoolean(oss, true);
  writeUtf8String(oss, "level == 'error'");

  // metadataFilterQuery present
  writeBoolean(oss, true);
  writeUtf8String(oss, "timestamp > 1000");

  std::string binaryData = oss.str();

  std::shared_ptr<ConnectorTableLayoutHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto clpHandle = std::dynamic_pointer_cast<ClpTableLayoutHandle>(handle);
  ASSERT_NE(clpHandle, nullptr);
  EXPECT_EQ(clpHandle->table.schemaTableName.schema, "clp");
  ASSERT_NE(clpHandle->kqlQuery, nullptr);
  ASSERT_NE(clpHandle->metadataFilterQuery, nullptr);
  EXPECT_EQ(*clpHandle->kqlQuery, "level == 'error'");
  EXPECT_EQ(*clpHandle->metadataFilterQuery, "timestamp > 1000");
}

TEST_F(ClpConnectorProtocolTest, TestSplitDeserialization) {
  std::ostringstream oss;
  writeUtf8String(oss, "/data/archive.clps");
  writeInt(oss, 0); // ARCHIVE
  writeBoolean(oss, false); // kqlQuery absent

  std::string binaryData = oss.str();

  std::shared_ptr<ConnectorSplit> handle;
  protocol.deserialize(binaryData, handle);

  auto clpSplit = std::dynamic_pointer_cast<ClpSplit>(handle);
  ASSERT_NE(clpSplit, nullptr);
  EXPECT_EQ(clpSplit->path, "/data/archive.clps");
  EXPECT_EQ(clpSplit->type, SplitType::ARCHIVE);
  EXPECT_EQ(clpSplit->kqlQuery, nullptr);
}

TEST_F(ClpConnectorProtocolTest, TestTransactionHandleDeserialization) {
  std::string binaryData; // empty

  std::shared_ptr<ConnectorTransactionHandle> handle;
  protocol.deserialize(binaryData, handle);

  auto clpHandle = std::dynamic_pointer_cast<ClpTransactionHandle>(handle);
  ASSERT_NE(clpHandle, nullptr);
  EXPECT_EQ(clpHandle->instance, "");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
