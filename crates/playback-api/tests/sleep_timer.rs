use nova_playback_api::*;
mod sleep_timer_tests {
    use super::*;
    #[test]
    fn duration_and_episode_commands_have_bounded_payloads() {
        for minutes in [0, 241, u16::MAX] {
            assert!(
                !PlaybackAction::SetSleepTimer(SleepTimerMode::Minutes(minutes)).valid_payload()
            );
        }
        for minutes in [1, 15, 90, 240] {
            assert!(
                PlaybackAction::SetSleepTimer(SleepTimerMode::Minutes(minutes)).valid_payload()
            );
        }
        for episodes in [0, 11, u8::MAX] {
            assert!(
                !PlaybackAction::SetSleepTimer(SleepTimerMode::Episodes(episodes)).valid_payload()
            );
        }
        for episodes in [1, 10] {
            assert!(
                PlaybackAction::SetSleepTimer(SleepTimerMode::Episodes(episodes)).valid_payload()
            );
        }
    }
    #[test]
    fn receiver_snapshot_bounds_countdown_and_count() {
        let mut snapshot = PlaybackSnapshot::default();
        snapshot.sleep_timer = Some(SleepTimerStatus {
            mode: SleepTimerMode::Episodes(10),
            remaining_seconds: 0,
            remaining_episodes: 10,
        });
        assert!(snapshot.valid_payload());
        snapshot.sleep_timer.as_mut().unwrap().remaining_episodes = 11;
        assert!(!snapshot.valid_payload());
    }
}
