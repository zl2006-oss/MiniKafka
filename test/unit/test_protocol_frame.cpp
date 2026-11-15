#include "test_common.h"

#include <cstdint>
#include <string>

#include "minikafka/common/crc32.h"
#include "minikafka/protocol/frame.h"
#include "test_protocol_util.h"

void TestFrame() {
  using minikafka::common::StatusCode;
  using minikafka::protocol::EncodeFrame;
  using minikafka::protocol::Frame;
  using minikafka::protocol::FrameDecoder;
  using minikafka::protocol::kFrameHeaderSize;
  using minikafka::protocol::kFrameMagic;
  using minikafka::protocol::kFrameOverhead;
  using minikafka::protocol::kFrameVersion;
  using minikafka::protocol::kMaxFramePayload;
  using mk_protocol_test::ExpectOk;
  using mk_protocol_test::ExpectStatus;
  using mk_protocol_test::ReadU16At;
  using mk_protocol_test::ReadU32At;
  using mk_protocol_test::ReadU64At;

  Frame frame;
  frame.msg_type = 0x0003u;
  frame.request_id = 0x0102030405060708ULL;
  frame.payload = "hello";

  std::string bytes;
  ExpectOk(EncodeFrame(frame, &bytes));
  MK_EXPECT_EQ(bytes.size(), kFrameOverhead + frame.payload.size());
  MK_EXPECT_EQ(ReadU32At(bytes, 0u), kFrameMagic);
  MK_EXPECT_EQ(ReadU16At(bytes, 4u), kFrameVersion);
  MK_EXPECT_EQ(ReadU16At(bytes, 6u), frame.msg_type);
  MK_EXPECT_EQ(ReadU64At(bytes, 8u), frame.request_id);
  MK_EXPECT_EQ(ReadU32At(bytes, 16u),
               static_cast<std::uint32_t>(frame.payload.size()));
  MK_EXPECT_EQ(bytes.substr(kFrameHeaderSize, frame.payload.size()),
               frame.payload);

  const std::uint32_t stored_crc =
      ReadU32At(bytes, kFrameHeaderSize + frame.payload.size());
  MK_EXPECT_EQ(stored_crc,
               minikafka::common::Crc32(
                   bytes.data(), kFrameHeaderSize + frame.payload.size()));

  FrameDecoder decoder;
  ExpectOk(decoder.Feed(bytes));
  Frame decoded;
  bool has_frame = false;
  ExpectOk(decoder.Next(&decoded, &has_frame));
  MK_EXPECT_TRUE(has_frame);
  MK_EXPECT_EQ(decoded.msg_type, frame.msg_type);
  MK_EXPECT_EQ(decoded.request_id, frame.request_id);
  MK_EXPECT_EQ(decoded.payload, frame.payload);
  MK_EXPECT_EQ(decoder.buffered(), static_cast<std::size_t>(0));

  Frame empty_frame;
  empty_frame.msg_type = 0x8004u;
  empty_frame.request_id = 0u;
  std::string empty_bytes;
  ExpectOk(EncodeFrame(empty_frame, &empty_bytes));
  MK_EXPECT_EQ(empty_bytes.size(), kFrameOverhead);

  FrameDecoder empty_decoder;
  ExpectOk(empty_decoder.Feed(empty_bytes));
  Frame empty_decoded;
  has_frame = false;
  ExpectOk(empty_decoder.Next(&empty_decoded, &has_frame));
  MK_EXPECT_TRUE(has_frame);
  MK_EXPECT_EQ(empty_decoded.msg_type, empty_frame.msg_type);
  MK_EXPECT_EQ(empty_decoded.request_id, empty_frame.request_id);
  MK_EXPECT_TRUE(empty_decoded.payload.empty());

  Frame large;
  large.payload.assign(static_cast<std::size_t>(kMaxFramePayload) + 1u, 'x');
  std::string unused;
  ExpectStatus(EncodeFrame(large, &unused), StatusCode::kRequestTooLarge);

  ExpectStatus(EncodeFrame(frame, nullptr), StatusCode::kInternalError);
}
