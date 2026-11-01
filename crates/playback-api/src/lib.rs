//! Ephemeral paired-device playback messages. Sources, credentials, paths and
//! provider settings stay on the receiving device; offers expose opaque IDs.
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId {
    /// Process boot epoch; never reuse sessions after a receiver restart.
    pub epoch: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaIdentity {
    pub media_type: String,
    pub title_id: String,
    pub episode_id: String,
    pub title: String,
    pub episode: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PlaybackAction {
    SetSleepTimer(SleepTimerMode),
    ExtendSleepTimer,
    CancelSleepTimer,
    QueueSkip,
    QueueStop,
    QueuePlayNow,
    TogglePause,
    SetPaused(bool),
    SeekRelative(f64),
    SeekAbsolute(f64),
    SetVolume(f64),
    VolumeStep(f64),
    SystemVolumeStep(i32),
    SetMuted(bool),
    ToggleMute,
    SetSpeed(f64),
    SpeedStep(i32),
    BeginSpeedPreview,
    EndSpeedPreview,
    SelectAudioTrack(i32),
    SelectSubtitleTrack(i32),
    ResetAudioTrack,
    ResetSubtitleTrack,
    ResetSpeed,
    ToggleFullscreen,
    SetFullscreen(bool),
    Screenshot,
    Stop,
    Resume(bool),
    CancelResume,
    NextEpisode {
        token: String,
    },
    DismissNextEpisode {
        token: String,
    },
    SetDecoder(i32),
    SetBrightness(f64),
    BrightnessStep(f64),
    SeekChapter {
        id: String,
    },
    SetSubtitleStyle {
        control: i32,
        value: f64,
    },
    ResetSubtitleStyle,
    SelectSecondarySubtitleTrack(i32),
    SetSecondarySubtitlePosition(i32),
    SetIntroSkipMode(i32),
    SkipSegment {
        token: String,
    },
    DismissSkipSegment {
        token: String,
    },
    AddBookmark {
        note: String,
    },
    EditBookmark {
        id: String,
        note: String,
    },
    DeleteBookmark {
        id: String,
    },
    SeekBookmark {
        id: String,
    },
    TryAnotherStream,
    RetrySubtitleDiscovery,
    RefreshSubtitleTracks,
    CancelSubtitleDownload {
        secondary: bool,
    },
    /// Wire-only selections bind a displayed choice, not a reusable native ID.
    SelectAudioTrackToken(String),
    SelectSubtitleTrackToken(String),
    SelectSecondarySubtitleTrackToken(String),
}

impl PlaybackAction {
    pub fn valid_payload(&self) -> bool {
        let id = |id: &str| !id.is_empty() && id.len() <= 4096 && !id.chars().any(char::is_control);
        let note = |note: &str| {
            note.len() <= MAX_REMOTE_NOTE_BYTES
                && note.chars().count() <= 280
                && !note.chars().any(char::is_control)
        };
        let range = |value: f64, low, high| value.is_finite() && (low..=high).contains(&value);
        match self {
            Self::SetSleepTimer(SleepTimerMode::Minutes(n)) => (1..=240).contains(n),
            Self::SetSleepTimer(SleepTimerMode::Episodes(n)) => (1..=10).contains(n),
            Self::SeekRelative(value) => range(*value, -86400.0, 86400.0),
            Self::SeekAbsolute(value) => range(*value, 0.0, 365.0 * 86400.0),
            Self::SetVolume(value) => range(*value, 0.0, 100.0),
            Self::VolumeStep(value) => range(*value, -100.0, 100.0),
            Self::SystemVolumeStep(value) | Self::SpeedStep(value) => (-1..=1).contains(value),
            Self::SetSpeed(value) => range(*value, 0.5, 2.0),
            Self::SetBrightness(value) => range(*value, 0.05, 1.0),
            Self::BrightnessStep(value) => range(*value, -1.0, 1.0),
            Self::SetDecoder(value) | Self::SetSecondarySubtitlePosition(value) => {
                (0..=2).contains(value)
            }
            Self::SetIntroSkipMode(value) => (-1..=2).contains(value),
            Self::SetSubtitleStyle { control, value } => match control {
                0 => range(*value, -60.0, 60.0),
                1 => range(*value, 16.0, 100.0),
                2 => range(*value, 0.0, 200.0),
                3 => range(*value, 0.0, 100.0),
                4 => range(*value, 0.0, 10.0),
                5 => range(*value, 0.0, 2.0) && value.fract() == 0.0,
                6 => matches!(*value, 0.0 | 1.0),
                7 => range(*value, 0.0, 100.0),
                _ => false,
            },
            Self::SeekChapter { id: value }
            | Self::DeleteBookmark { id: value }
            | Self::SeekBookmark { id: value }
            | Self::SkipSegment { token: value }
            | Self::DismissSkipSegment { token: value }
            | Self::SelectAudioTrackToken(value)
            | Self::SelectSubtitleTrackToken(value)
            | Self::SelectSecondarySubtitleTrackToken(value) => id(value),
            Self::NextEpisode { token } | Self::DismissNextEpisode { token } => id(token),
            Self::AddBookmark { note: value } => note(value),
            Self::EditBookmark {
                id: value,
                note: text,
            } => id(value) && note(text),
            _ => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Request {
    Snapshot,
    QueueStart {
        profile_id: String,
        item_key: Option<String>,
        expected_session: Option<SessionId>,
    },
    QueueStop {
        profile_id: String,
        run_id: u64,
        expected_session: Option<SessionId>,
    },
    Lookup(MediaIdentity),
    CurrentOffers {
        session: SessionId,
    },
    Start {
        offer_id: String,
        expected_session: Option<SessionId>,
    },
    Command {
        session: SessionId,
        action: PlaybackAction,
    },
    /// Also dispatched locally when a controller connection is lost.
    ReleasePreview,
}

impl Request {
    /// Semantic bounds are checked before a request can enter the replay cache.
    pub fn valid_payload(&self) -> bool {
        match self {
            Self::QueueStop {
                profile_id, run_id, ..
            } => {
                *run_id > 0
                    && !profile_id.is_empty()
                    && profile_id.len() <= 1024
                    && !profile_id.chars().any(char::is_control)
            }
            Self::QueueStart {
                profile_id,
                item_key,
                ..
            } => {
                !profile_id.is_empty()
                    && profile_id.len() <= 1024
                    && !profile_id.chars().any(char::is_control)
                    && item_key.as_ref().is_none_or(|key| {
                        !key.is_empty() && key.len() <= 4096 && !key.chars().any(char::is_control)
                    })
            }
            Self::Command { action, .. } => action.valid_payload(),
            Self::Start { offer_id, .. } => {
                !offer_id.is_empty() && offer_id.len() <= MAX_REMOTE_ID_BYTES
            }
            Self::Lookup(media) => {
                media.title_id.len() <= 1024
                    && media.episode_id.len() <= 1024
                    && media.media_type.len() <= 16
                    && media.title.len() <= 512
                    && media.episode.len() <= 512
            }
            _ => true,
        }
    }
}

// Keep the public callback contract ergonomic; the 256 KiB transport bound
// limits payload size independently of this small inline snapshot.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    Snapshot(PlaybackSnapshot),
    Offers(Vec<PlaybackOffer>),
    Accepted,
    Error(PlaybackError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlaybackError {
    Unavailable,
    Unauthorized,
    StaleSession,
    InvalidRequest,
    NotFound,
    Busy,
    Timeout,
    Unsupported,
    Failed(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlaybackRole {
    #[default]
    Local,
    Remote,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlaybackPhase {
    #[default]
    Idle,
    Loading,
    Playing,
    Paused,
    Ended,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlaybackCapabilities {
    pub sleep_timer: bool,
    pub sleep_timer_episodes: bool,
    pub queue: bool,
    pub pause: bool,
    pub seek: bool,
    pub volume: bool,
    pub mute: bool,
    pub speed: bool,
    pub tracks: bool,
    pub fullscreen: bool,
    pub screenshot: bool,
    pub stop: bool,
    pub resume: bool,
    pub next_episode: bool,
    pub decoder: bool,
    pub brightness: bool,
    pub chapters: bool,
    pub subtitle_style: bool,
    pub secondary_subtitles: bool,
    pub intro_skip: bool,
    pub bookmarks: bool,
    pub source_recovery: bool,
    pub source_offers: bool,
    pub subtitle_discovery: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybackTrack {
    pub id: i32,
    pub token: String,
    pub label: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlaybackChapter {
    pub id: String,
    pub label: String,
    pub time: f64,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlaybackBookmark {
    pub id: String,
    pub note: String,
    pub time: f64,
}

pub const MAX_REMOTE_TRACKS: usize = 64;
pub const MAX_REMOTE_CHAPTERS: usize = 128;
pub const MAX_REMOTE_BOOKMARKS: usize = 128;
pub const MAX_REMOTE_LABEL_BYTES: usize = 192;
pub const MAX_REMOTE_NOTE_BYTES: usize = 1120;
pub const MAX_REMOTE_ID_BYTES: usize = 64;

/// Receiver owns the deadline; controllers display only these samples.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SleepTimerMode {
    Minutes(u16),
    EndOfItem,
    Episodes(u8),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SleepTimerStatus {
    pub mode: SleepTimerMode,
    pub remaining_seconds: u32,
    pub remaining_episodes: u8,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStatus {
    pub profile_id: String,
    pub run_id: u64,
    pub item_key: String,
    pub phase: String,
    pub countdown: u32,
    pub autoplay: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlaybackSnapshot {
    /// Headless sync jobs and apps without a registered UI adapter are unavailable.
    pub sleep_timer: Option<SleepTimerStatus>,
    pub queue: Option<QueueStatus>,
    pub ready: bool,
    pub role: PlaybackRole,
    pub session: Option<SessionId>,
    pub title: String,
    pub episode: String,
    pub phase: PlaybackPhase,
    pub paused: bool,
    pub time: f64,
    pub duration: f64,
    pub volume: f64,
    pub muted: bool,
    pub speed: f64,
    pub fullscreen: bool,
    pub audio_tracks: Vec<PlaybackTrack>,
    pub subtitle_tracks: Vec<PlaybackTrack>,
    pub audio_track: i32,
    pub subtitle_track: i32,
    pub secondary_subtitle_tracks: Vec<PlaybackTrack>,
    pub secondary_subtitle_track: i32,
    pub secondary_subtitle_position: i32,
    pub subtitle_style: [f64; 8],
    pub chapters: Vec<PlaybackChapter>,
    pub bookmarks: Vec<PlaybackBookmark>,
    pub chapters_truncated: bool,
    pub bookmarks_truncated: bool,
    pub bookmark_notice: String,
    pub intro_skip_mode: i32,
    pub skip_segment_kind: i32,
    pub skip_token: String,
    pub subtitle_addon_status: i32,
    pub decoder: i32,
    pub brightness: f64,
    pub automatic_recovery: bool,
    pub resume_prompt: bool,
    pub next_episode_prompt: bool,
    pub next_episode_token: String,
    pub next_episode_media: Option<MediaIdentity>,
    pub screenshot_busy: bool,
    pub screenshot_saved: bool,
    pub screenshot_failed: bool,
    pub capabilities: PlaybackCapabilities,
    pub revision: u64,
}

impl PlaybackSnapshot {
    pub fn valid_payload(&self) -> bool {
        let id = |id: &str| {
            !id.is_empty() && id.len() <= MAX_REMOTE_ID_BYTES && !id.chars().any(char::is_control)
        };
        let label = |s: &str, max| s.len() <= max && !s.chars().any(char::is_control);
        let tracks = |rows: &[PlaybackTrack]| {
            rows.len() <= MAX_REMOTE_TRACKS
                && rows
                    .iter()
                    .all(|r| id(&r.token) && label(&r.label, MAX_REMOTE_LABEL_BYTES))
        };
        let time = |value: f64| value.is_finite() && (0.0..=365.0 * 86400.0).contains(&value);
        self.sleep_timer.as_ref().is_none_or(|timer| {
            timer.remaining_seconds <= 240 * 60
                && timer.remaining_episodes <= 10
                && PlaybackAction::SetSleepTimer(timer.mode.clone()).valid_payload()
        }) && self.queue.as_ref().is_none_or(|queue| {
            label(&queue.profile_id, 1024)
                && label(&queue.item_key, 4096)
                && label(&queue.phase, 64)
                && queue.countdown <= 60
        }) && label(&self.title, 512)
            && label(&self.episode, 512)
            && time(self.time)
            && time(self.duration)
            && self.volume.is_finite()
            && (0.0..=100.0).contains(&self.volume)
            && self.speed.is_finite()
            && (0.0..=2.0).contains(&self.speed)
            && self.brightness.is_finite()
            && (0.0..=1.0).contains(&self.brightness)
            && tracks(&self.audio_tracks)
            && tracks(&self.subtitle_tracks)
            && tracks(&self.secondary_subtitle_tracks)
            && self.chapters.len() <= MAX_REMOTE_CHAPTERS
            && self.chapters.iter().all(|r| {
                id(&r.id)
                    && label(&r.label, MAX_REMOTE_LABEL_BYTES)
                    && time(r.time)
                    && (self.duration <= 0.0 || r.time <= self.duration)
            })
            && self.bookmarks.len() <= MAX_REMOTE_BOOKMARKS
            && self.bookmarks.iter().all(|r| {
                id(&r.id)
                    && label(&r.note, MAX_REMOTE_NOTE_BYTES)
                    && time(r.time)
                    && (self.duration <= 0.0 || r.time <= self.duration)
            })
            && self.subtitle_style.iter().all(|v| v.is_finite())
            && (0.0..=100.0).contains(&self.subtitle_style[7])
            && (0..=2).contains(&self.secondary_subtitle_position)
            && (-1..=2).contains(&self.intro_skip_mode)
            && (0..=2).contains(&self.skip_segment_kind)
            && (0..=4).contains(&self.subtitle_addon_status)
            && (0..=2).contains(&self.decoder)
            && (self.skip_token.is_empty() || id(&self.skip_token))
            && label(&self.bookmark_notice, 512)
            && self.next_episode_token.len() <= 4096
            && self
                .next_episode_media
                .as_ref()
                .is_none_or(|media| Request::Lookup(media.clone()).valid_payload())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybackOffer {
    pub id: String,
    pub name: String,
    pub description: String,
    /// Receiver-formatted display size, bounded to 32 bytes; empty if unknown.
    pub size: String,
}

/// Authenticated connection identity, never supplied by the remote message.
#[derive(Clone)]
pub struct PeerContext {
    pub peer_id: String,
    pub request_id: u64,
    pub requester_epoch: u64,
    /// Receiver-local connection identity; zero denotes membership cleanup.
    pub connection_id: u64,
    authorization: Arc<dyn Fn() -> bool + Send + Sync>,
}
impl PeerContext {
    pub fn new(
        peer_id: String,
        request_id: u64,
        requester_epoch: u64,
        authorization: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        Self {
            peer_id,
            request_id,
            requester_epoch,
            connection_id: 0,
            authorization,
        }
    }
    pub fn with_connection_id(mut self, connection_id: u64) -> Self {
        self.connection_id = connection_id;
        self
    }
    /// Recheck on the UI thread and after asynchronous work: pairing can be
    /// revoked after the transport accepted the original request.
    pub fn is_authorized(&self) -> bool {
        (self.authorization)()
    }
}

pub type Completion = Box<dyn FnOnce(Response) + Send>;
/// The adapter schedules work on its owning UI thread and completes later.
/// It must return promptly, without blocking the transport runtime.
pub type PlaybackHandler = Arc<dyn Fn(PeerContext, Request, Completion) + Send + Sync>;
