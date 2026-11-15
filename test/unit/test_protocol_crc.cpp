#include "test_common.h"

#include <cstdint>
#include <string>

#include "minikafka/common/crc32.h"

void TestCrc32() {
  using minikafka::common::Crc32;
  using minikafka::common::Crc32Finalize;
  using minikafka::common::Crc32Update;
  using minikafka::common::kCrc32Init;

  MK_EXPECT_EQ(Crc32("123456789", 9u), 0xCBF43926u);
  MK_EXPECT_EQ(Crc32(std::string("123456789")), 0xCBF43926u);
  MK_EXPECT_EQ(Crc32("abc", 3u), 0x352441C2u);
  MK_EXPECT_EQ(Crc32("", 0u), 0u);
  MK_EXPECT_EQ(Crc32(static_cast<const void*>(nullptr), 0u), 0u);

  std::uint32_t state = kCrc32Init;
  state = Crc32Update(state, "1234", 4u);
  state = Crc32Update(state, "56789", 5u);
  MK_EXPECT_EQ(Crc32Finalize(state), 0xCBF43926u);

  state = kCrc32Init;
  state = Crc32Update(state, "", 0u);
  MK_EXPECT_EQ(Crc32Finalize(state), 0u);
}
