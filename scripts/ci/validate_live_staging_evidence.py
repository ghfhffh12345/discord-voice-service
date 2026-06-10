#!/usr/bin/env python3
"""Strict live-staging evidence validator."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM = 980_000
MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM = 1_020_000
MIN_OBSERVED_PACKET_COUNT = 120
MIN_NON_SILENT_AUDIO_MS = 1_000
MIN_STABILITY_METRIC_PACKET_COUNT = 50
PAUSE_STOP_SILENCE_FRAME_COUNT = 5
RESUME_OBSERVER_PACKET_TARGET = 4

REQUIRED_TOP_LEVEL_TRUE = (
    "validated_join_voice",
    "validated_update_voice_context",
    "validated_play",
    "validated_pause",
    "validated_resume",
    "validated_invalid_resume_ignored",
    "validated_redundant_pause_ignored",
    "observer_proved_pause",
    "observer_proved_resume",
    "observer_pause_rtp_silence_observed",
    "validated_reconnect_rollover_during_playback",
    "validated_stop",
    "validated_stop_during_playback",
    "validated_leave_voice",
    "validated_leave_voice_during_playback",
    "validated_get_state",
    "validated_get_playback_metrics",
    "validated_subscribe_events",
    "saw_voice_connecting",
    "saw_voice_ready",
    "saw_track_resolving",
    "saw_buffering",
    "saw_playing",
    "saw_paused",
    "saw_resumed_playing",
    "saw_track_ended",
    "validated_constrained_profile",
    "validated_slow_jittery_http",
)

PLAYBACK_ZERO_COUNTERS = (
    "track_fast_interval_count",
    "track_tempo_window_fast_count",
    "track_tempo_window_slow_count",
    "skipped_source_frame_count",
    "skipped_source_duration_ms",
    "skipped_source_duration_samples",
    "tempo_rebase_count",
    "frame_deficit_count",
    "dropped_frame_count",
    "late_frame_count",
    "buffer_underrun_count",
    "rebuffer_count",
    "playout_underrun_count",
    "egress_underrun_count",
    "source_underrun_count",
    "source_underrun_reached_builder_count",
    "source_underrun_reached_deadline_sender_count",
    "continuity_silence_packet_count",
    "inserted_silence_duration_ms",
    "egress_inserted_silence_duration_ms",
    "scheduled_silence_packet_count",
    "prepared_silence_packet_drop_count",
    "discarded_source_frame_count",
    "discarded_source_duration_ms",
    "discarded_source_duration_samples",
    "egress_dropped_music_frame_count",
    "egress_dropped_music_duration_ms",
    "egress_dropped_music_duration_samples",
)

RECONNECT_ZERO_COUNTERS = (
    "frame_deficit_count",
    "dropped_frame_count",
    "late_frame_count",
    "buffer_underrun_count",
    "rebuffer_count",
    "playout_underrun_count",
    "egress_underrun_count",
    "source_underrun_count",
    "inserted_silence_duration_ms",
    "egress_inserted_silence_duration_ms",
)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: validate_live_staging_evidence.py EVIDENCE_PATH", file=sys.stderr)
        return 2

    path = Path(sys.argv[1])
    failures = validate_file(path)
    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        return 1
    return 0


def validate_file(path: Path) -> list[str]:
    if not path.is_file() or path.stat().st_size == 0:
        return [f"live validation evidence artifact {path} was missing or empty"]

    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        return [f"live validation evidence artifact {path} was not valid JSON: {error}"]

    if not isinstance(evidence, dict):
        return [f"live validation evidence root was {type(evidence).__name__}; expected object"]

    return validate_evidence(evidence)


def validate_evidence(evidence: dict[str, Any]) -> list[str]:
    failures: list[str] = []

    if evidence.get("outcome") != "success":
        failures.append(f"evidence.outcome was {evidence.get('outcome')!r}; expected 'success'")

    for key in REQUIRED_TOP_LEVEL_TRUE:
        require_true(failures, evidence, key)

    require_at_least(failures, evidence, "pause_silence_packet_count", PAUSE_STOP_SILENCE_FRAME_COUNT)
    require_at_least(failures, evidence, "observer_resume_packet_count", RESUME_OBSERVER_PACKET_TARGET)
    require_at_least(failures, evidence, "observed_packet_count", MIN_OBSERVED_PACKET_COUNT)
    require_at_least(failures, evidence, "non_silent_audio_ms", MIN_NON_SILENT_AUDIO_MS)
    require_zero(failures, evidence, "observer_rtp_buffering_event_count")
    require_zero(failures, evidence, "observer_decoded_audio_tempo_window_fast_count")
    require_zero(failures, evidence, "observer_decoded_audio_tempo_window_slow_count")
    require_ratio(failures, evidence, "observer_decoded_audio_to_wall_clock_ratio_ppm")
    require_decoded_audio_duration(failures, evidence)

    if evidence.get("failure_reason") is not None:
        failures.append(f"evidence.failure_reason was {evidence.get('failure_reason')!r}; expected null")

    playback = evidence.get("playback_metrics")
    if not isinstance(playback, dict):
        failures.append("evidence.playback_metrics was not an object")
    else:
        validate_playback_metrics(failures, playback)

    reconnect = evidence.get("reconnect_probe_metrics")
    if not isinstance(reconnect, dict):
        failures.append("evidence.reconnect_probe_metrics was not an object")
    else:
        validate_reconnect_probe_metrics(failures, reconnect)

    return failures


def validate_playback_metrics(failures: list[str], metrics: dict[str, Any]) -> None:
    require_true(failures, metrics, "ended", "playback_metrics")
    require_at_least(
        failures,
        metrics,
        "track_packet_count",
        MIN_STABILITY_METRIC_PACKET_COUNT,
        "playback_metrics",
    )
    require_at_least(
        failures,
        metrics,
        "pause_media_boundary_count",
        1,
        "playback_metrics",
    )
    require_ratio(failures, metrics, "track_media_to_wall_clock_ratio_ppm", "playback_metrics")
    for key in PLAYBACK_ZERO_COUNTERS:
        require_zero(failures, metrics, key, "playback_metrics")


def validate_reconnect_probe_metrics(failures: list[str], metrics: dict[str, Any]) -> None:
    require_false(failures, metrics, "ended", "reconnect_probe_metrics")
    require_at_least(failures, metrics, "reconnect_interruptions", 1, "reconnect_probe_metrics")
    require_at_least(failures, metrics, "track_packet_count", 1, "reconnect_probe_metrics")
    require_ratio(failures, metrics, "track_media_to_wall_clock_ratio_ppm", "reconnect_probe_metrics")
    for key in RECONNECT_ZERO_COUNTERS:
        require_zero(failures, metrics, key, "reconnect_probe_metrics")


def require_decoded_audio_duration(failures: list[str], evidence: dict[str, Any]) -> None:
    expected_duration_ms = evidence.get("expected_track_duration_ms")
    decoded_audio_ms = evidence.get("decoded_audio_ms")
    if not isinstance(expected_duration_ms, int) or expected_duration_ms <= 0:
        failures.append(
            f"evidence.expected_track_duration_ms was {expected_duration_ms!r}; expected positive integer"
        )
        return
    if not isinstance(decoded_audio_ms, int):
        failures.append(f"evidence.decoded_audio_ms was {decoded_audio_ms!r}; expected integer")
        return

    tolerance_floor = max(0, expected_duration_ms - 2_000)
    ratio_floor = expected_duration_ms * 90 // 100
    required_decoded_audio_ms = min(
        expected_duration_ms,
        max(ratio_floor, tolerance_floor, min(6_000, expected_duration_ms)),
    )
    if decoded_audio_ms < required_decoded_audio_ms:
        failures.append(
            f"evidence.decoded_audio_ms was {decoded_audio_ms}; expected >= {required_decoded_audio_ms}"
        )


def require_true(
    failures: list[str], container: dict[str, Any], key: str, label: str = "evidence"
) -> None:
    if container.get(key) is not True:
        failures.append(f"{label}.{key} was {container.get(key)!r}; expected true")


def require_false(
    failures: list[str], container: dict[str, Any], key: str, label: str = "evidence"
) -> None:
    if container.get(key) is not False:
        failures.append(f"{label}.{key} was {container.get(key)!r}; expected false")


def require_zero(
    failures: list[str], container: dict[str, Any], key: str, label: str = "evidence"
) -> None:
    if container.get(key) != 0:
        failures.append(f"{label}.{key} was {container.get(key)!r}; expected 0")


def require_at_least(
    failures: list[str],
    container: dict[str, Any],
    key: str,
    minimum: int,
    label: str = "evidence",
) -> None:
    value = container.get(key)
    if not isinstance(value, int) or value < minimum:
        failures.append(f"{label}.{key} was {value!r}; expected >= {minimum}")


def require_ratio(
    failures: list[str], container: dict[str, Any], key: str, label: str = "evidence"
) -> None:
    value = container.get(key)
    if (
        not isinstance(value, int)
        or value < MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM
        or value > MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM
    ):
        failures.append(
            f"{label}.{key} was {value!r}; expected "
            f"{MEDIA_TO_WALL_CLOCK_MIN_RATIO_PPM}..={MEDIA_TO_WALL_CLOCK_MAX_RATIO_PPM}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
