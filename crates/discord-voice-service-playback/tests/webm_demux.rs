use discord_voice_service_test_support::fixtures::load_fixture_bytes;

use bytes::Bytes;
use discord_voice_service_playback::media::webm_demux::WebmOpusDemux;
use webm_iterable::WebmWriter;
use webm_iterable::matroska_spec::{BlockLacing, Frame, Master, MatroskaSpec, SimpleBlock};

#[test]
fn demux_extracts_multiple_opus_packets_from_split_fixture() {
    let fixture = load_fixture_bytes("audio-itag250.webm");
    let split_sizes = [1usize, 2, 3, 4, 64, 128, 254, 255, 256, 257, 512, 678];
    let expected_packets = demux_fixture_in_chunks(&fixture, fixture.len());

    for split_size in split_sizes {
        let packets = demux_fixture_in_chunks(&fixture, split_size);

        assert!(
            packets.len() > 10,
            "split size {split_size} produced too few packets"
        );
        assert!(
            packets.iter().all(|packet| !packet.data.is_empty()),
            "split size {split_size} produced empty packet data"
        );
        assert!(
            packets.iter().all(|packet| packet.duration_ms > 0),
            "split size {split_size} produced zero-duration packet"
        );
        assert!(
            packets.windows(2).all(|window| {
                let left = &window[0];
                let right = &window[1];
                left.timestamp_ms <= right.timestamp_ms
            }),
            "split size {split_size} produced non-monotonic timestamps"
        );
        assert!(
            packets.windows(2).all(|window| window[0] != window[1]),
            "split size {split_size} produced duplicate adjacent packets"
        );
        assert_eq!(
            packets, expected_packets,
            "split size {split_size} diverged from one-shot demux"
        );
    }
}

#[test]
fn demux_split_stream_matches_one_shot_for_long_fixture() {
    let fixture = load_fixture_bytes("audio-long.webm");
    let expected_packets = demux_fixture_in_chunks(&fixture, fixture.len());

    for split_size in [127usize, 255, 256, 257, 511, 512, 1024, 4093] {
        let packets = demux_fixture_in_chunks(&fixture, split_size);
        assert_eq!(
            packets, expected_packets,
            "split size {split_size} diverged from one-shot demux"
        );
    }
}

#[test]
fn demux_fractional_laced_simple_block_preserves_sample_timestamps() {
    let fixture = fractional_laced_webm();
    let packets = demux_fixture_in_chunks(&fixture, 5);

    assert_eq!(packets.len(), 3);
    assert_eq!(packets[0].timestamp_samples, 0);
    assert_eq!(packets[0].timestamp_ms, 0);
    assert_eq!(packets[0].duration_samples, 120);
    assert_eq!(packets[0].duration_ms, 2);
    assert_eq!(packets[1].timestamp_samples, 120);
    assert_eq!(packets[1].timestamp_ms, 2);
    assert_eq!(packets[1].duration_samples, 960);
    assert_eq!(packets[1].duration_ms, 20);
    assert_eq!(packets[2].timestamp_samples, 1_080);
    assert_eq!(packets[2].timestamp_ms, 22);
    assert_eq!(packets[2].duration_samples, 120);
    assert_eq!(packets[2].duration_ms, 2);
}

fn demux_fixture_in_chunks(
    fixture: &Bytes,
    split_size: usize,
) -> Vec<discord_voice_service_playback::media::webm_demux::DemuxedPacket> {
    let mut demux = WebmOpusDemux::default();
    let mut packets = Vec::new();

    for chunk in fixture.chunks(split_size) {
        demux.push_bytes(Bytes::copy_from_slice(chunk));
        packets.extend(
            demux
                .drain_packets()
                .unwrap_or_else(|error| panic!("split size {split_size} failed: {error}")),
        );
    }

    packets
}

fn fractional_laced_webm() -> Bytes {
    let first_fractional_packet = [0x80];
    let twenty_ms_packet = [0xF8, 0xFF, 0xFE];
    let second_fractional_packet = [0x80];
    let frames = vec![
        Frame {
            data: &first_fractional_packet,
        },
        Frame {
            data: &twenty_ms_packet,
        },
        Frame {
            data: &second_fractional_packet,
        },
    ];
    let mut block =
        SimpleBlock::new_uncheked(&[], 1, 0, false, Some(BlockLacing::Xiph), false, true);
    block.set_frame_data(&frames);

    let mut bytes = Vec::new();
    let mut writer = WebmWriter::new(&mut bytes);
    writer
        .write(&MatroskaSpec::Segment(Master::Full(vec![
            MatroskaSpec::Info(Master::Full(vec![MatroskaSpec::TimestampScale(1_000_000)])),
            MatroskaSpec::Tracks(Master::Full(vec![MatroskaSpec::TrackEntry(Master::Full(
                vec![
                    MatroskaSpec::TrackNumber(1),
                    MatroskaSpec::TrackType(2),
                    MatroskaSpec::CodecID("A_OPUS".to_owned()),
                ],
            ))])),
            MatroskaSpec::Cluster(Master::Full(vec![MatroskaSpec::Timestamp(0), block.into()])),
        ])))
        .expect("tiny fractional WebM should encode");
    writer.flush().expect("tiny fractional WebM should flush");
    drop(writer);

    Bytes::from(bytes)
}
