#include "test_common.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/protocol/frame.h"
#include "test_protocol_util.h"

void TestFrameDecoder() {
  using minikafka::common::StatusCode;
  using minikafka::protocol::EncodeFrame;
  using minikafka::protocol::Frame;
  using minikafka::protocol::FrameDecoder;
  using minikafka::protocol::kFrameMagic;
  using minikafka::protocol::kFrameVersion;
  using minikafka::protocol::kMaxFramePayload;
  using mk_protocol_test::AppendU16;
  using mk_protocol_test::AppendU32;
  using mk_protocol_test::AppendU64;
  using mk_protocol_test::ExpectOk;
  using mk_protocol_test::ExpectStatus;

  Frame frame;
  frame.msg_type = 0x0001u;
  frame.request_id = 7u;
  frame.payload = "half-packet";
  std::string bytes;
  ExpectOk(EncodeFrame(frame, &bytes));

  FrameDecoder decoder;
  Frame decoded;
  bool has_frame = false;
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    ExpectOk(decoder.Feed(&bytes[i], 1u));
    ExpectOk(decoder.Next(&decoded, &has_frame));
    if (i + 1u < bytes.size()) {
      MK_EXPECT_FALSE(has_frame);
    } else {
      MK_EXPECT_TRUE(has_frame);
    }
  }
  MK_EXPECT_EQ(decoded.msg_type, frame.msg_type);
  MK_EXPECT_EQ(decoded.request_id, frame.request_id);
  MK_EXPECT_EQ(decoded.payload, frame.payload);
  MK_EXPECT_EQ(decoder.buffered(), static_cast<std::size_t>(0));

  std::string concatenated;
  std::string second_frame;
  for (std::uint64_t id = 1u; id <= 3u; ++id) {
    Frame current;
    current.msg_type = 0x0004u;
    current.request_id = id;
    current.payload = "payload-" + std::to_string(id);
    std::string encoded;
    ExpectOk(EncodeFrame(current, &encoded));
    concatenated += encoded;
    if (id == 2u) {
      second_frame = encoded;
    }
  }

  FrameDecoder sticky_decoder;
  ExpectOk(sticky_decoder.Feed(concatenated));
  std::size_t frame_count = 0;
  while (true) {
    Frame current;
    bool has = false;
    ExpectOk(sticky_decoder.Next(&current, &has));
    if (!has) {
      break;
    }
    ++frame_count;
    MK_EXPECT_EQ(current.request_id, static_cast<std::uint64_t>(frame_count));
    MK_EXPECT_EQ(current.payload,
                 std::string("payload-") + std::to_string(frame_count));
  }
  MK_EXPECT_EQ(frame_count, static_cast<std::size_t>(3));
  MK_EXPECT_EQ(sticky_decoder.buffered(), static_cast<std::size_t>(0));

  FrameDecoder partial_decoder;
  const std::size_t split = second_frame.size() / 2u;
  ExpectOk(partial_decoder.Feed(second_frame.substr(0, split)));
  Frame partial_frame;
  bool partial_has = true;
  ExpectOk(partial_decoder.Next(&partial_frame, &partial_has));
  MK_EXPECT_FALSE(partial_has);
  MK_EXPECT_EQ(partial_decoder.buffered(), split);
  ExpectOk(partial_decoder.Feed(second_frame.substr(split)));
  ExpectOk(partial_decoder.Next(&partial_frame, &partial_has));
  MK_EXPECT_TRUE(partial_has);
  MK_EXPECT_EQ(partial_frame.request_id, static_cast<std::uint64_t>(2));

  std::string oversized_header;
  AppendU32(&oversized_header, kFrameMagic);
  AppendU16(&oversized_header, kFrameVersion);
  AppendU16(&oversized_header, 0x0003u);
  AppendU64(&oversized_header, 1u);
  AppendU32(&oversized_header, kMaxFramePayload + 1u);
  MK_EXPECT_EQ(oversized_header.size(), static_cast<std::size_t>(20));

  FrameDecoder oversized_decoder;
  ExpectOk(oversized_decoder.Feed(oversized_header));
  Frame oversized_frame;
  bool oversized_has = true;
  ExpectStatus(oversized_decoder.Next(&oversized_frame, &oversized_has),
               StatusCode::kRequestTooLarge);
  MK_EXPECT_FALSE(oversized_has);
  MK_EXPECT_TRUE(oversized_decoder.failed());
  ExpectStatus(oversized_decoder.Next(&oversized_frame, &oversized_has),
               StatusCode::kRequestTooLarge);

  std::string crc_bytes;
  ExpectOk(EncodeFrame(frame, &crc_bytes));
  crc_bytes[crc_bytes.size() - 1u] = static_cast<char>(
      static_cast<unsigned char>(crc_bytes[crc_bytes.size() - 1u]) ^ 0xFFu);
  FrameDecoder crc_decoder;
  ExpectOk(crc_decoder.Feed(crc_bytes));
  Frame crc_frame;
  bool crc_has = false;
  ExpectStatus(crc_decoder.Next(&crc_frame, &crc_has),
               StatusCode::kFrameCrcError);
  MK_EXPECT_FALSE(crc_has);
  MK_EXPECT_TRUE(crc_decoder.failed());

  std::string magic_bytes;
  ExpectOk(EncodeFrame(frame, &magic_bytes));
  magic_bytes[0] = static_cast<char>(0);
  FrameDecoder magic_decoder;
  ExpectOk(magic_decoder.Feed(magic_bytes));
  Frame magic_frame;
  bool magic_has = false;
  ExpectStatus(magic_decoder.Next(&magic_frame, &magic_has),
               StatusCode::kInvalidFrame);

  std::string version_bytes;
  ExpectOk(EncodeFrame(frame, &version_bytes));
  version_bytes[4] = static_cast<char>(0);
  version_bytes[5] = static_cast<char>(2);
  FrameDecoder version_decoder;
  ExpectOk(version_decoder.Feed(version_bytes));
  Frame version_frame;
  bool version_has = false;
  ExpectStatus(version_decoder.Next(&version_frame, &version_has),
               StatusCode::kUnsupportedVersion);

  FrameDecoder reset_decoder;
  ExpectOk(reset_decoder.Feed(crc_bytes.substr(0, 10u)));
  ExpectOk(reset_decoder.Next(&crc_frame, &crc_has));
  MK_EXPECT_FALSE(crc_has);
  reset_decoder.Reset();
  MK_EXPECT_FALSE(reset_decoder.failed());
  MK_EXPECT_EQ(reset_decoder.buffered(), static_cast<std::size_t>(0));
  ExpectOk(reset_decoder.Feed(bytes));
  ExpectOk(reset_decoder.Next(&crc_frame, &crc_has));
  MK_EXPECT_TRUE(crc_has);

  FrameDecoder empty_decoder;
  ExpectOk(empty_decoder.Feed(static_cast<const void*>(nullptr), 0u));
  ExpectOk(empty_decoder.Feed(std::string()));
  MK_EXPECT_FALSE(empty_decoder.failed());
}
