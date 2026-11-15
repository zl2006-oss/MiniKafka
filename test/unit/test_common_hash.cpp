#include "test_common.h"

#include <cstdint>
#include <string>

#include "minikafka/common/hash.h"

void TestHash() {
  using minikafka::common::Fnv1a32;
  using minikafka::common::PartitionForKey;

  MK_EXPECT_EQ(Fnv1a32(""), 0x811C9DC5u);
  MK_EXPECT_EQ(Fnv1a32("a"), 0xE40C292Cu);
  MK_EXPECT_EQ(Fnv1a32("b"), 0xE70C2DE5u);
  MK_EXPECT_EQ(Fnv1a32("foobar"), 0xBF9CF968u);
  MK_EXPECT_EQ(Fnv1a32("hello"), 0x4F9F2CABu);
  MK_EXPECT_EQ(Fnv1a32("test"), 0xAFD071E5u);

  const std::string utf8_e("\xC3\xA9", 2);
  MK_EXPECT_EQ(Fnv1a32(utf8_e), 0x1E9DE8C1u);

  const char data[] = {'a', 'b', 'c'};
  MK_EXPECT_EQ(Fnv1a32(data, sizeof(data)), Fnv1a32(std::string("abc")));

  MK_EXPECT_EQ(PartitionForKey("a", 4u), 0u);
  MK_EXPECT_EQ(PartitionForKey("a", 16u), 12u);
  MK_EXPECT_EQ(PartitionForKey("foobar", 4u), 0u);
  MK_EXPECT_EQ(PartitionForKey("hello", 4u), 3u);
  MK_EXPECT_EQ(PartitionForKey("", 4u), 1u);
  MK_EXPECT_EQ(PartitionForKey("", 16u), 5u);
  MK_EXPECT_EQ(PartitionForKey("x", 1u), 0u);
  MK_EXPECT_EQ(PartitionForKey("x", 0u), 0u);

  MK_EXPECT_EQ(PartitionForKey("stable-key", 16u),
               PartitionForKey("stable-key", 16u));
  MK_EXPECT_EQ(Fnv1a32("stable-key"), Fnv1a32(std::string("stable-key")));
}
