use super::*;
use std::{sync::atomic::AtomicUsize, time::Duration};

#[test]
fn public_decoded_lru_reports_evictions_and_prioritizes_visible_keys_with_a_hard_cap() {
    let mut cache = DecodedCache::new(32);
    let pixels = || SharedPixelBuffer::<Rgba8Pixel>::new(4, 1);
    assert!(cache.insert("a".into(), pixels()).is_empty());
    cache.insert("b".into(), pixels());
    cache.set_protected(HashSet::from(["a".into()]));
    assert!(cache.peek("a").is_some());
    assert_eq!(cache.keys_by_recency(), ["a", "b"]);
    assert!(cache.get("b").is_some());
    assert_eq!(cache.insert("c".into(), pixels()), ["b"]);
    cache.set_protected(HashSet::from(["a".into(), "c".into()]));
    assert_eq!(cache.set_budget(16), ["a"]);
    assert_eq!(cache.stats(), (1, 16));
    assert_eq!(
        cache.insert(
            "too-large".into(),
            SharedPixelBuffer::<Rgba8Pixel>::new(8, 1)
        ),
        ["too-large"]
    );
    cache.clear();
    assert_eq!(cache.stats(), (0, 0));
    assert!(cache.protected.is_empty());
}

#[test]
fn decoded_lru_reports_multi_victim_evictions_in_policy_order() {
    let mut cache = DecodedCache::new(64);
    let pixels = || SharedPixelBuffer::<Rgba8Pixel>::new(4, 1);
    for key in ["a", "b", "c", "d"] {
        assert!(cache.insert(key.into(), pixels()).is_empty());
    }
    cache.set_protected(HashSet::from(["a".into(), "c".into()]));
    assert_eq!(cache.set_budget(16), ["b", "d", "a"]);
    assert_eq!(cache.keys_by_recency(), ["c"]);
    assert_eq!(cache.stats(), (1, 16));
    cache.set_protected(HashSet::new());
    assert_eq!(cache.set_budget(0), ["c"]);
    assert_eq!(cache.stats(), (0, 0));
}

#[test]
fn decoded_lru_reports_rejected_replacements_and_keeps_survivors() {
    let mut cache = DecodedCache::new(32);
    let pixels = || SharedPixelBuffer::<Rgba8Pixel>::new(4, 1);
    cache.insert("a".into(), pixels());
    cache.insert("b".into(), pixels());
    assert!(cache.insert("a".into(), pixels()).is_empty());
    assert_eq!(cache.keys_by_recency(), ["b", "a"]);
    assert_eq!(cache.insert("c".into(), pixels()), ["b"]);
    assert_eq!(
        cache.insert("a".into(), SharedPixelBuffer::<Rgba8Pixel>::new(9, 1)),
        ["a"]
    );
    assert_eq!(cache.stats(), (1, 16));
    assert_eq!(cache.keys_by_recency(), ["c"]);
    assert_eq!(
        cache.insert(
            "new-oversize".into(),
            SharedPixelBuffer::<Rgba8Pixel>::new(9, 1)
        ),
        ["new-oversize"]
    );
    assert_eq!(cache.stats(), (1, 16));
}

#[test]
fn decoded_lru_unpinning_restores_normal_victim_selection() {
    let mut cache = DecodedCache::new(32);
    let pixels = || SharedPixelBuffer::<Rgba8Pixel>::new(4, 1);
    cache.insert("a".into(), pixels());
    cache.insert("b".into(), pixels());
    cache.set_protected(HashSet::from(["a".into()]));
    assert_eq!(cache.insert("c".into(), pixels()), ["b"]);
    assert!(cache.set_protected(HashSet::new()).is_empty());
    assert_eq!(cache.insert("d".into(), pixels()), ["a"]);
    cache.set_protected(HashSet::from(["c".into(), "d".into()]));
    assert_eq!(cache.set_budget(0), ["c", "d"]);
    assert_eq!(cache.stats(), (0, 0));
    cache.set_budget(16);
    cache.insert("pinned".into(), pixels());
    cache.set_protected(HashSet::from(["pinned".into()]));
    assert_eq!(cache.insert("new".into(), pixels()), ["new"]);
    assert_eq!(cache.keys_by_recency(), ["pinned"]);
}

#[test]
fn decoded_lru_eviction_sets_match_previous_snapshot_reporting() {
    let mut cache = DecodedCache::new(64);
    let mut seed = 0x6a09e667f3bcc909u64;
    for step in 0..1000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("key-{}", (seed >> 32) % 11);
        let before = cache.map.keys().cloned().collect::<Vec<_>>();
        let inserted = (step % 5 <= 2).then(|| key.clone());
        let mut reported = match step % 5 {
            0..=2 => cache.insert(
                key.clone(),
                SharedPixelBuffer::<Rgba8Pixel>::new(((seed >> 48) % 24 + 1) as u32, 1),
            ),
            3 => cache.set_budget(((seed >> 40) % 96) as usize),
            _ => cache.set_protected(HashSet::from([key.clone()])),
        };
        // The former public API compared all keys before and after mutation.
        let mut expected = before
            .into_iter()
            .filter(|key| !cache.contains(key))
            .collect::<Vec<_>>();
        if let Some(key) = inserted
            && !cache.contains(&key)
            && !expected.contains(&key)
        {
            expected.push(key);
        }
        reported.sort();
        expected.sort();
        assert_eq!(reported, expected, "operation {step}");
        assert_eq!(
            cache.total_bytes,
            cache.map.values().map(|entry| entry.bytes).sum::<usize>()
        );
        assert!(cache.total_bytes <= cache.max_bytes);
    }
}

#[test]
fn guarded_atomic_publication_checks_after_private_staging_and_keeps_previous_file() {
    let _serial = serial();
    let dir = scratch("guarded-atomic");
    let path = dir.join("image.variant");
    atomic_write(&path, b"previous").unwrap();
    assert!(
        !atomic_write_if(&path, b"replacement", || {
            assert_eq!(fs::read(&path).unwrap(), b"previous");
            let staged = fs::read_dir(&dir)
                .unwrap()
                .map(Result::unwrap)
                .find(|entry| entry.path() != path)
                .unwrap();
            assert_eq!(fs::read(staged.path()).unwrap(), b"replacement");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    staged.metadata().unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
            false
        })
        .unwrap()
    );
    assert_eq!(fs::read(&path).unwrap(), b"previous");
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    fs::remove_dir_all(dir).unwrap();
}

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub(super) fn serial() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap()
}
pub(super) fn scratch(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "nova-media-{}-{label}-{}",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}
pub(super) fn source_image(w: u32, h: u32, transparent: bool) -> Vec<u8> {
    let image = image::RgbaImage::from_fn(w, h, |x, y| {
        image::Rgba([
            ((x * 17 + y * 13) % 251) as u8,
            ((x * 7 + y * 29) % 253) as u8,
            ((x * 31 + y * 3) % 249) as u8,
            if transparent && (x + y) % 2 == 0 {
                96
            } else {
                255
            },
        ])
    });
    let mut bytes = Vec::new();
    image::DynamicImage::ImageRgba8(image)
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    bytes
}
fn settings() -> CacheSettings {
    CacheSettings::default()
}

#[test]
fn avif_processed_cold_memory_disk_and_source_rewrite_use_shared_codec() {
    let _serial = serial();
    let dir = scratch("avif-shared-codec");
    let url = "https://unit.test/avif-shared";
    let bytes = source_image(32, 48, true);
    let settings = CacheSettings {
        format: CacheImageFormat::Avif,
        avif_quality: 91,
        ..settings()
    };
    let cold = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(32)), &settings, || {
        Some(bytes)
    })
    .unwrap();
    let path = recipe_disk_path(
        &dir,
        url,
        ImageRecipe::resolve(ImageUse::Fixed(Some(32)), &settings),
    );
    let original = fs::read(&path).unwrap();
    let variant = read_variant(&path).unwrap();
    assert_eq!(
        image::guess_format(&variant.encoded).unwrap(),
        image::ImageFormat::Avif
    );
    assert_eq!(variant.metadata.recipe.quality, 91);
    assert!(image_buffers_equal(
        &cold,
        &cached_pixels_with(&dir, url, ImageUse::Fixed(Some(32)), &settings, || panic!(
            "memory hit"
        ))
        .unwrap()
    ));
    decoded_cache_clear();
    assert!(image_buffers_equal(
        &cold,
        &cached_pixels_with(&dir, url, ImageUse::Fixed(Some(32)), &settings, || panic!(
            "disk hit"
        ))
        .unwrap()
    ));
    assert_eq!(fs::read(&path).unwrap(), original);
    let target = CacheSettings {
        avif_quality: 30,
        ..settings.clone()
    };
    let source = include_bytes!("../../image/tests/fixtures/tiny.avif");
    let result = rewrite_entries_with(
        &dir,
        &target,
        &AtomicBool::new(false),
        |_| {},
        |_| Some(source.to_vec()),
    );
    assert_eq!(result.converted, 1);
    let (_, variant) = disk_variants(&dir).into_iter().next().unwrap();
    assert_eq!(variant.metadata.recipe.quality, 30);
    assert_eq!((variant.metadata.width, variant.metadata.height), (16, 16));
    assert!(nova_image::decode_avif(&variant.encoded, nova_image::DecodeLimits::default()).is_ok());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn jpeg_webp_recipe_keys_remain_identical_and_avif_uses_its_own_quality() {
    for (format, name) in [
        (CacheImageFormat::Jpeg, "jpeg"),
        (CacheImageFormat::Webp, "webp"),
    ] {
        let settings = CacheSettings {
            format,
            avif_quality: 13,
            ..settings()
        };
        let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &settings);
        assert_eq!(
            recipe.key(),
            format!("{{\"max_side\":480,\"compress\":true,\"format\":\"{name}\",\"quality\":50}}")
        );
    }
    let settings = CacheSettings {
        format: CacheImageFormat::Avif,
        quality: 12,
        avif_quality: 85,
        ..settings()
    };
    assert_eq!(
        ImageRecipe::resolve(ImageUse::DetailPoster, &settings).quality,
        85
    );
    assert_eq!(settings.config_key(), "avif:q85");
    let old: CacheSettings = serde_json::from_str(r#"{"format":"webp","quality":50}"#).unwrap();
    assert_eq!(old.avif_quality, 85);
    assert_eq!(old.quality, 50);
}
fn disk_variants(dir: &Path) -> Vec<(PathBuf, DiskVariant)> {
    directory_variants(dir).collect()
}

#[test]
fn local_cache_probe_never_joins_or_fails_an_active_download() {
    let _serial = serial();
    let dir = scratch("local-cache-priority");
    let url = "https://unit.test/local-cache-priority";
    let settings = settings();
    let image_use = ImageUse::Fixed(Some(480));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_dir = dir.clone();
    let worker_settings = settings.clone();
    let download = std::thread::spawn(move || {
        cached_pixels_with(&worker_dir, url, image_use, &worker_settings, || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Some(source_image(20, 30, false))
        })
    });
    started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(
        cached_display_pixels_at(&dir, url, image_use, &settings, LoadEpoch::capture()).is_none(),
        "a cache miss must return while HTTP is still blocked"
    );
    release_tx.send(()).unwrap();
    assert!(
        download.join().unwrap().is_some(),
        "the probe must not publish a failed flight to the downloader"
    );
    decoded_cache_clear();
    let restored =
        cached_display_pixels_at(&dir, url, image_use, &settings, LoadEpoch::capture()).unwrap();
    assert_eq!((restored.width(), restored.height()), (20, 30));
    assert!(
        cached_display_pixels_at(
            &dir,
            url,
            ImageUse::Fixed(Some(120)),
            &settings,
            LoadEpoch::capture()
        )
        .is_none(),
        "a local probe must still respect the complete processing recipe"
    );
}

#[test]
fn presets_fixed_override_original_aspect_ratio_no_upscale_and_buffer_lengths() {
    let _serial = serial();
    let settings = settings();
    assert_eq!(
        ImageRecipe::resolve(ImageUse::DetailPoster, &settings).max_side,
        Some(480)
    );
    assert_eq!(
        ImageRecipe::resolve(ImageUse::DetailBackdrop, &settings).max_side,
        Some(960)
    );
    assert_eq!(
        ImageRecipe::resolve(ImageUse::EpisodeThumbnail, &settings).max_side,
        Some(360)
    );
    assert_eq!(
        ImageRecipe::resolve(ImageUse::PlayerEpisodeBackdrop, &settings).max_side,
        None
    );
    assert_eq!(
        ImageRecipe::resolve(ImageUse::Fixed(Some(77)), &settings).max_side,
        Some(77)
    );
    assert_eq!(
        ImageRecipe::resolve(ImageUse::Fixed(None), &settings).max_side,
        None
    );

    let preset_source = source_image(2000, 1000, false);
    let presets = [240, 360, 480, 720, 960, 1024, 1280, 1920, 2560];
    for side in presets {
        for image_use in [
            ImageUse::DetailPoster,
            ImageUse::DetailBackdrop,
            ImageUse::EpisodeThumbnail,
            ImageUse::PlayerEpisodeBackdrop,
        ] {
            let local = match image_use {
                ImageUse::DetailPoster => CacheSettings {
                    cache_images: false,
                    detail_poster_max_side: Some(side),
                    ..settings.clone()
                },
                ImageUse::DetailBackdrop => CacheSettings {
                    cache_images: false,
                    detail_backdrop_max_side: Some(side),
                    ..settings.clone()
                },
                ImageUse::EpisodeThumbnail => CacheSettings {
                    cache_images: false,
                    episode_thumbnail_max_side: Some(side),
                    ..settings.clone()
                },
                ImageUse::PlayerEpisodeBackdrop => CacheSettings {
                    cache_images: false,
                    player_episode_backdrop_max_side: Some(side),
                    ..settings.clone()
                },
                ImageUse::Fixed(_) => unreachable!(),
            };
            assert_eq!(ImageRecipe::resolve(image_use, &local).max_side, Some(side));
            let pixels = cached_pixels_with(
                Path::new("/unused"),
                &format!("preset-{side}-{image_use:?}"),
                image_use,
                &local,
                || Some(source_image(200, 100, false)),
            )
            .unwrap();
            assert_eq!(
                (pixels.width(), pixels.height()),
                (200, 100),
                "preset {side} must not upscale"
            );
            assert_eq!(
                pixels.as_bytes().len(),
                pixels.width() as usize * pixels.height() as usize * 4
            );
        }
    }
    for image_use in [
        ImageUse::DetailPoster,
        ImageUse::DetailBackdrop,
        ImageUse::EpisodeThumbnail,
        ImageUse::PlayerEpisodeBackdrop,
    ] {
        let local = match image_use {
            ImageUse::DetailPoster => CacheSettings {
                cache_images: false,
                detail_poster_max_side: None,
                ..settings.clone()
            },
            ImageUse::DetailBackdrop => CacheSettings {
                cache_images: false,
                detail_backdrop_max_side: None,
                ..settings.clone()
            },
            ImageUse::EpisodeThumbnail => CacheSettings {
                cache_images: false,
                episode_thumbnail_max_side: None,
                ..settings.clone()
            },
            ImageUse::PlayerEpisodeBackdrop => CacheSettings {
                cache_images: false,
                player_episode_backdrop_max_side: None,
                ..settings.clone()
            },
            ImageUse::Fixed(_) => unreachable!(),
        };
        let pixels = cached_pixels_with(
            Path::new("/unused"),
            &format!("preset-original-{image_use:?}"),
            image_use,
            &local,
            || Some(preset_source.clone()),
        )
        .unwrap();
        assert_eq!((pixels.width(), pixels.height()), (2000, 1000));
    }

    for (url, w, h, side, expected) in [
        ("portrait", 680, 1000, 480, (326, 480)),
        ("landscape", 1000, 680, 480, (480, 326)),
        ("square-ish", 813, 1446, 360, (202, 360)),
        ("no-upscale", 300, 200, 480, (300, 200)),
    ] {
        let dir = scratch(url);
        let local = CacheSettings {
            cache_images: false,
            ..settings.clone()
        };
        let bytes = source_image(w, h, false);
        let pixels = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(side)), &local, || {
            Some(bytes)
        })
        .unwrap();
        assert_eq!((pixels.width(), pixels.height()), expected, "{url}");
        assert_eq!(
            pixels.as_bytes().len(),
            pixels.width() as usize * pixels.height() as usize * 4
        );
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn cold_memory_and_disk_hits_return_identical_processed_codec_pixels() {
    let _serial = serial();
    let mut jpeg_low = None;
    let mut jpeg_high = None;
    let mut webp_low = None;
    let mut webp_high = None;
    for (format, quality) in [
        (CacheImageFormat::Jpeg, 25),
        (CacheImageFormat::Jpeg, 91),
        (CacheImageFormat::Webp, 37),
        (CacheImageFormat::Webp, 88),
    ] {
        let dir = scratch("codec");
        let url = format!("https://unit.test/{format:?}/{quality}");
        let bytes = source_image(113, 71, false);
        let local = CacheSettings {
            enabled: true,
            format,
            quality,
            detail_poster_max_side: Some(63),
            ..settings()
        };
        let first = cached_pixels_with(&dir, &url, ImageUse::DetailPoster, &local, || {
            Some(bytes.clone())
        })
        .unwrap();
        let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &local);
        let path = recipe_disk_path(&dir, &url, recipe);
        let variant = read_variant(&path).expect("processed encoded variant");
        assert!(variant_pixels(&variant, &url, recipe).is_some());
        match (format, quality) {
            (CacheImageFormat::Jpeg, 25) => jpeg_low = Some(variant.encoded.clone()),
            (CacheImageFormat::Jpeg, 91) => jpeg_high = Some(variant.encoded.clone()),
            (CacheImageFormat::Webp, 37) => webp_low = Some(variant.encoded.clone()),
            _ => webp_high = Some(variant.encoded.clone()),
        }
        let memory = cached_pixels_with(&dir, &url, ImageUse::DetailPoster, &local, || {
            panic!("memory hit should not fetch")
        })
        .unwrap();
        assert!(image_buffers_equal(&first, &memory));
        decoded_cache_clear();
        let disk = cached_pixels_with(&dir, &url, ImageUse::DetailPoster, &local, || {
            panic!("disk hit should avoid download")
        })
        .unwrap();
        assert!(image_buffers_equal(&first, &disk));
        fs::remove_dir_all(dir).unwrap();
    }
    assert_ne!(
        jpeg_low, jpeg_high,
        "JPEG quality must affect encoded bytes"
    );
    assert_ne!(
        webp_low, webp_high,
        "WebP quality must affect encoded bytes"
    );
}

#[test]
fn jpeg_alpha_falls_back_to_png_and_compression_off_stores_resized_png() {
    let _serial = serial();
    let dir = scratch("alpha");
    let url = "https://unit.test/alpha";
    let bytes = source_image(39, 22, true);
    let jpeg = CacheSettings {
        enabled: true,
        format: CacheImageFormat::Jpeg,
        quality: 70,
        detail_poster_max_side: Some(22),
        ..settings()
    };
    let _pixels = cached_pixels_with(&dir, url, ImageUse::DetailPoster, &jpeg, || {
        Some(bytes.clone())
    })
    .unwrap();
    let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &jpeg);
    let path = recipe_disk_path(&dir, url, recipe);
    let encoded = read_variant(&path).unwrap().encoded;
    assert!(encoded.starts_with(b"\x89PNG\r\n\x1a\n"));
    assert!(
        image::load_from_memory(&encoded)
            .unwrap()
            .into_rgba8()
            .pixels()
            .any(|pixel| pixel.0[3] < 255)
    );

    let plain = CacheSettings {
        enabled: false,
        detail_poster_max_side: Some(18),
        ..settings()
    };
    let plain_url = "https://unit.test/plain";
    cached_pixels_with(&dir, plain_url, ImageUse::DetailPoster, &plain, || {
        Some(bytes)
    })
    .unwrap();
    let variant = read_variant(&recipe_disk_path(
        &dir,
        plain_url,
        ImageRecipe::resolve(ImageUse::DetailPoster, &plain),
    ))
    .unwrap();
    assert!(variant.encoded.starts_with(b"\x89PNG\r\n\x1a\n"));
    assert_eq!((variant.metadata.width, variant.metadata.height), (18, 10));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn disk_off_performs_no_reads_or_writes_but_keeps_memory_cache() {
    let _serial = serial();
    let dir = scratch("disk-off");
    let url = "https://unit.test/no-disk";
    let source = source_image(32, 20, false);
    let local = CacheSettings {
        cache_images: false,
        enabled: true,
        format: CacheImageFormat::Webp,
        detail_poster_max_side: Some(16),
        ..settings()
    };
    let first =
        cached_pixels_with(&dir, url, ImageUse::DetailPoster, &local, || Some(source)).unwrap();
    assert!(fs::read_dir(&dir).unwrap().next().is_none());
    let again = cached_pixels_with(&dir, url, ImageUse::DetailPoster, &local, || {
        panic!("memory result expected")
    })
    .unwrap();
    assert!(image_buffers_equal(&first, &again));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn shared_recipe_roles_persist_together_and_settings_change_retires_use_history() {
    let _serial = serial();
    let dir = scratch("roles");
    let url = "https://unit.test/roles";
    let bytes = source_image(900, 600, false);
    let original = settings();
    // DetailBackdrop and a fixed 960 override resolve to the same recipe.
    let pixels = cached_pixels_with(&dir, url, ImageUse::DetailBackdrop, &original, || {
        Some(bytes.clone())
    })
    .unwrap();
    let same = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(960)), &original, || {
        panic!("same recipe memory hit")
    })
    .unwrap();
    assert!(image_buffers_equal(&pixels, &same));
    let recipe = ImageRecipe::resolve(ImageUse::DetailBackdrop, &original);
    let path = recipe_disk_path(&dir, url, recipe);
    let roles = read_variant(&path).unwrap().metadata.uses;
    assert!(
        roles.contains(&ImageUse::DetailBackdrop) && roles.contains(&ImageUse::Fixed(Some(960)))
    );

    let changed = CacheSettings {
        detail_backdrop_max_side: Some(720),
        ..original
    };
    let fetches = AtomicUsize::new(0);
    let changed_pixels = cached_pixels_with(&dir, url, ImageUse::DetailBackdrop, &changed, || {
        fetches.fetch_add(1, Ordering::SeqCst);
        Some(bytes.clone())
    })
    .unwrap();
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "a new recipe must download again"
    );
    assert_eq!(changed_pixels.width().max(changed_pixels.height()), 720);
    let variants = disk_variants(&dir);
    assert_eq!(
        variants.len(),
        2,
        "the old variant remains for the fixed-960 use"
    );
    assert!(
        variants
            .iter()
            .any(|(_, v)| v.metadata.uses == vec![ImageUse::Fixed(Some(960))])
    );
    assert!(
        variants
            .iter()
            .any(|(_, v)| v.metadata.uses.contains(&ImageUse::DetailBackdrop))
    );

    // The old fixed role is migrated independently; once it has a replacement,
    // the history file is removed and the new role remains registered.
    cached_pixels_with(&dir, url, ImageUse::Fixed(Some(960)), &changed, || {
        Some(bytes)
    })
    .unwrap();
    assert_eq!(disk_variants(&dir).len(), 2);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn corrupt_envelopes_checksum_and_encoded_bytes_refetch() {
    let _serial = serial();
    let dir = scratch("corrupt");
    let url = "https://unit.test/corrupt";
    let source = source_image(50, 30, false);
    let local = settings();
    let use_ = ImageUse::Fixed(Some(40));
    let recipe = ImageRecipe::resolve(use_, &local);
    cached_pixels_with(&dir, url, use_, &local, || Some(source.clone())).unwrap();
    let path = recipe_disk_path(&dir, url, recipe);
    for corruption in 0..4 {
        let valid = fs::read(&path).unwrap();
        let mut damaged = valid.clone();
        match corruption {
            0 => damaged[0] = b'X',
            1 | 2 => {
                let meta_len = u32::from_le_bytes(damaged[8..12].try_into().unwrap()) as usize;
                let mut metadata: VariantMetadata =
                    serde_json::from_slice(&damaged[12..12 + meta_len]).unwrap();
                if corruption == 1 {
                    metadata.url.push_str("/tampered");
                } else {
                    metadata.checksum ^= 1;
                }
                let bytes = serde_json::to_vec(&metadata).unwrap();
                damaged.clear();
                damaged.extend_from_slice(MAGIC);
                damaged.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                damaged.extend_from_slice(&bytes);
                damaged.extend_from_slice(&valid[12 + meta_len..]);
            }
            _ => *damaged.last_mut().unwrap() ^= 0x55,
        }
        fs::write(&path, damaged).unwrap();
        decoded_cache_clear();
        let calls = AtomicUsize::new(0);
        cached_pixels_with(&dir, url, use_, &local, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Some(source.clone())
        })
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(variant_pixels(&read_variant(&path).unwrap(), url, recipe).is_some());
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn failed_encoder_returns_source_pixels_without_claiming_recipe_or_overwriting_valid_variant() {
    let _serial = serial();
    let dir = scratch("encoder-failure");
    let url = "https://unit.test/encoder-failure";
    let bytes = source_image(46, 30, false);
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(Some(30)), settings());
    let initial = process_source(bytes.clone(), recipe).unwrap();
    let path = recipe_disk_path(&dir, url, recipe);
    persist_processed(&dir, url, &[ImageUse::Fixed(Some(30))], recipe, &initial).unwrap();
    let previous = fs::read(&path).unwrap();
    decoded_cache_clear();
    let output = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(30)), &settings(), || {
        Some(bytes.clone())
    })
    .unwrap();
    // Force an encoder failure through the injectable core seam; the decoded
    // source remains usable, while no cache recipe is published or replaced.
    let failed = process_source_with(bytes, recipe, |_, _| None).unwrap();
    assert!(failed.encoded.is_none());
    assert_eq!(failed.pixels.width().max(failed.pixels.height()), 30);
    assert!(output.width() > 0);
    assert!(persist_processed(&dir, url, &[ImageUse::Fixed(Some(30))], recipe, &failed).is_err());
    assert_eq!(fs::read(path).unwrap(), previous);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn atomic_write_failure_preserves_previous_valid_variant() {
    let _serial = serial();
    let dir = scratch("atomic-failure");
    let url = "https://unit.test/atomic";
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(Some(20)), settings());
    let path = recipe_disk_path(&dir, url, recipe);
    let source = process_source(source_image(20, 20, false), recipe).unwrap();
    persist_processed(&dir, url, &[ImageUse::Fixed(Some(20))], recipe, &source).unwrap();
    let before = fs::read(&path).unwrap();
    let sequence = TEMP_SEQUENCE.load(Ordering::Relaxed);
    let blocker = path.with_file_name(format!(
        "{}.tmp-{}-{sequence}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    fs::create_dir(&blocker).unwrap();
    assert!(write_variant(&path, &disk_variants(&dir)[0].1).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn concurrent_identical_success_and_failure_coalesce_but_distinct_recipes_do_not() {
    let _serial = serial();
    for fail in [false, true] {
        let dir = scratch("flight");
        let url = if fail {
            "https://unit.test/flight-fail"
        } else {
            "https://unit.test/flight-ok"
        };
        let local = CacheSettings {
            cache_images: false,
            ..settings()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(std::sync::Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let dir = dir.clone();
            let calls = calls.clone();
            let gate = gate.clone();
            let local = local.clone();
            workers.push(std::thread::spawn(move || {
                gate.wait();
                cached_pixels_with(&dir, url, ImageUse::Fixed(Some(24)), &local, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(40));
                    if fail {
                        None
                    } else {
                        Some(source_image(40, 30, false))
                    }
                })
            }));
        }
        gate.wait();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            results.iter().filter(|result| result.is_some()).count(),
            if fail { 0 } else { 2 }
        );
        fs::remove_dir_all(dir).unwrap();
    }
    let dir = scratch("distinct-flights");
    let url = "https://unit.test/distinct";
    let local = CacheSettings {
        cache_images: false,
        ..settings()
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for side in [17, 23] {
        let dir = dir.clone();
        let calls = calls.clone();
        let gate = gate.clone();
        let local = local.clone();
        workers.push(std::thread::spawn(move || {
            gate.wait();
            cached_pixels_with(&dir, url, ImageUse::Fixed(Some(side)), &local, || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                Some(source_image(40, 30, false))
            })
        }));
    }
    gate.wait();
    for worker in workers {
        assert!(worker.join().unwrap().is_some());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn clear_during_blocked_download_discards_completion_and_leaves_no_cache_files() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("clear-flight");
    let url = "https://unit.test/clear-flight";
    let local = settings();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let thread_dir = dir.clone();
    let worker = std::thread::spawn(move || {
        cached_pixels_with(&thread_dir, url, ImageUse::Fixed(Some(28)), &local, || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Some(source_image(44, 28, false))
        })
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    clear_poster_cache_dir(&dir);
    release_tx.send(()).unwrap();
    assert!(worker.join().unwrap().is_none());
    assert_eq!(decoded_cache_stats().0, 0);
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn maintenance_fetches_each_url_once_rebuilds_registered_uses_and_counts_cancel_or_failure() {
    let _serial = serial();
    let dir = scratch("maintenance");
    let url = "https://unit.test/maintenance";
    let bytes = source_image(1000, 600, false);
    let initial = settings();
    for image_use in [
        ImageUse::Fixed(Some(320)),
        ImageUse::DetailBackdrop,
        ImageUse::Fixed(None),
    ] {
        cached_pixels_with(&dir, url, image_use, &initial, || Some(bytes.clone())).unwrap();
    }
    let changed = CacheSettings {
        detail_backdrop_max_side: Some(720),
        ..initial
    };
    let calls = AtomicUsize::new(0);
    let progress = rewrite_entries_with(
        &dir,
        &changed,
        &AtomicBool::new(false),
        |_| {},
        |seen| {
            assert_eq!(seen, url);
            calls.fetch_add(1, Ordering::SeqCst);
            Some(bytes.clone())
        },
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(progress.processed, 1);
    assert_eq!(progress.failed, 0);
    assert_eq!(progress.converted, 3);
    let variants = disk_variants(&dir);
    assert!(variants.iter().any(
        |(_, v)| v.metadata.uses.contains(&ImageUse::Fixed(Some(320))) && v.metadata.width == 320
    ));
    assert!(variants.iter().any(
        |(_, v)| v.metadata.uses.contains(&ImageUse::DetailBackdrop) && v.metadata.width == 720
    ));
    assert!(
        variants
            .iter()
            .any(|(_, v)| v.metadata.uses.contains(&ImageUse::Fixed(None))
                && v.metadata.width == 1000)
    );
    let failed = rewrite_entries_with(&dir, &changed, &AtomicBool::new(false), |_| {}, |_| None);
    assert_eq!(failed.failed, 1);
    let cancel = AtomicBool::new(true);
    let cancelled = rewrite_entries_with(
        &dir,
        &changed,
        &cancel,
        |_| {},
        |_| panic!("cancelled before fetching"),
    );
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.processed, 0);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn maintenance_reencodes_current_quality_and_size_for_jpeg_and_webp() {
    let _serial = serial();
    for format in [CacheImageFormat::Jpeg, CacheImageFormat::Webp] {
        let dir = scratch("maintenance-codec");
        let url = format!("https://unit.test/maintenance-codec/{format:?}");
        let bytes = source_image(1000, 600, false);
        let initial = CacheSettings {
            detail_poster_max_side: Some(480),
            ..settings()
        };
        cached_pixels_with(&dir, &url, ImageUse::DetailPoster, &initial, || {
            Some(bytes.clone())
        })
        .unwrap();
        let target = CacheSettings {
            enabled: true,
            format,
            quality: 63,
            detail_poster_max_side: Some(720),
            ..initial
        };
        let calls = AtomicUsize::new(0);
        let progress = rewrite_entries_with(
            &dir,
            &target,
            &AtomicBool::new(false),
            |_| {},
            |seen| {
                assert_eq!(seen, url);
                calls.fetch_add(1, Ordering::SeqCst);
                Some(bytes.clone())
            },
        );
        assert_eq!(progress.converted, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &target);
        let variant = read_variant(&recipe_disk_path(&dir, &url, recipe)).unwrap();
        assert_eq!(
            (variant.metadata.width, variant.metadata.height),
            (720, 432)
        );
        let expected = process_source(bytes, recipe).unwrap();
        let actual = variant_pixels(&variant, &url, recipe).unwrap();
        assert!(image_buffers_equal(&expected.pixels, &actual));
        if format == CacheImageFormat::Jpeg {
            assert!(variant.encoded.starts_with(&[0xff, 0xd8, 0xff]));
        } else {
            assert!(
                variant.encoded.starts_with(b"RIFF")
                    && variant.encoded.windows(4).any(|part| part == b"WEBP")
            );
        }
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn decoded_lru_evicts_by_byte_budget() {
    let _serial = serial();
    let local = CacheSettings {
        cache_images: false,
        ..settings()
    };
    set_decoded_cache_budget(1);
    decoded_cache_clear();
    let mut returned = Vec::new();
    for index in 0..100 {
        let url = format!("https://unit.test/catalog/{index}");
        returned.push(
            cached_pixels_with(
                Path::new("/unused"),
                &url,
                ImageUse::Fixed(Some(DISPLAY_POSTER_SIDE)),
                &local,
                || Some(source_image(256, 256, false)),
            )
            .unwrap(),
        );
    }
    // The removed desktop store kept up to 400 grid buffers outside this
    // budget. This 100-poster workload fits that store; compare its retained
    // pixels with the sole shared cache after scrolling those cards away.
    let previous_store_bytes: usize = returned.iter().map(|p| p.as_bytes().len()).sum();
    let catalog = (decoded_cache_stats().0, decoded_cache_stats().1);
    assert!(catalog.0 > 0 && catalog.1 <= 1024 * 1024);
    let detail_url = "https://unit.test/detail-large";
    let detail = cached_pixels_with(
        Path::new("/unused"),
        detail_url,
        ImageUse::DetailBackdrop,
        &local,
        || Some(source_image(2000, 1000, false)),
    )
    .unwrap();
    assert_eq!(detail.width().max(detail.height()), 960);
    let player_url = "https://unit.test/player-original";
    let player = cached_pixels_with(
        Path::new("/unused"),
        player_url,
        ImageUse::PlayerEpisodeBackdrop,
        &local,
        || Some(source_image(1800, 1200, false)),
    )
    .unwrap();
    assert_eq!((player.width(), player.height()), (1800, 1200));
    let after = decoded_cache_stats();
    eprintln!(
        "100-poster retention: previous separate store {previous_store_bytes} bytes; shared cache after scroll {} bytes; after Detail/player {} bytes (1 MiB cache budget; visible buffers excluded)",
        catalog.1, after.1
    );
    assert!(after.1 < previous_store_bytes);
    assert!(after.1 <= 1024 * 1024);
    assert!(after.1 < catalog.1 + detail.as_bytes().len() + player.as_bytes().len());
    // Slint buffers held by the UI remain usable after their LRU entries are evicted.
    assert!(
        returned.iter().all(|pixels| pixels.as_bytes().len()
            == pixels.width() as usize * pixels.height() as usize * 4)
    );
    assert_eq!(
        detail.as_bytes().len(),
        detail.width() as usize * detail.height() as usize * 4
    );
    assert_eq!(
        player.as_bytes().len(),
        player.width() as usize * player.height() as usize * 4
    );
    decoded_cache_clear();
    set_decoded_cache_budget(1);
    decoded_cache_insert("lru-a", SharedPixelBuffer::<Rgba8Pixel>::new(512, 256));
    decoded_cache_insert("lru-b", SharedPixelBuffer::<Rgba8Pixel>::new(512, 256));
    assert!(decoded_cache_get("lru-a").is_some());
    decoded_cache_insert("lru-c", SharedPixelBuffer::<Rgba8Pixel>::new(512, 256));
    assert!(decoded_cache_contains("lru-a"));
    assert!(!decoded_cache_contains("lru-b"));
    assert!(decoded_cache_contains("lru-c"));
    set_decoded_cache_budget(0);
    assert_eq!(decoded_cache_stats(), (0, 0));
    decoded_cache_clear();
    set_decoded_cache_budget(128);
}

#[test]
fn identical_refreshes_share_download_and_register_each_role() {
    let _serial = serial();
    let dir = scratch("refresh-flight");
    decoded_cache_clear();
    let local = CacheSettings {
        cache_images: true,
        enabled: false,
        detail_poster_max_side: Some(240),
        ..settings()
    };
    let url = "https://unit.test/refresh-shared";
    let epoch = LoadEpoch::capture();
    let bytes = source_image(300, 150, false);
    let calls = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut joins = Vec::new();
    for image_use in [ImageUse::DetailPoster, ImageUse::Fixed(Some(240))] {
        let dir = dir.clone();
        let bytes = bytes.clone();
        let calls = calls.clone();
        let barrier = barrier.clone();
        let local = local.clone();
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            refresh_image_if_changed_with(&dir, url, image_use, &local, epoch, || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(80));
                Some(bytes)
            })
        }));
    }
    barrier.wait();
    let results: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(results.iter().all(Option::is_some));
    let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &local);
    let variant = read_variant(&recipe_disk_path(&dir, url, recipe)).unwrap();
    assert!(variant.metadata.uses.contains(&ImageUse::DetailPoster));
    assert!(variant.metadata.uses.contains(&ImageUse::Fixed(Some(240))));

    let second_calls = AtomicUsize::new(0);
    let unchanged =
        refresh_image_if_changed_with(&dir, url, ImageUse::DetailPoster, &local, epoch, || {
            second_calls.fetch_add(1, Ordering::SeqCst);
            Some(bytes)
        });
    assert!(
        unchanged.is_none(),
        "same processed pixels should not refresh UI"
    );
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn failed_refresh_flight_completes_every_waiter() {
    let _serial = serial();
    let dir = scratch("refresh-failure");
    decoded_cache_clear();
    let local = CacheSettings {
        cache_images: false,
        ..settings()
    };
    let url = "https://unit.test/refresh-failure";
    let epoch = LoadEpoch::capture();
    let calls = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut joins = Vec::new();
    for _ in 0..2 {
        let dir = dir.clone();
        let calls = calls.clone();
        let barrier = barrier.clone();
        let local = local.clone();
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            refresh_image_if_changed_with(
                &dir,
                url,
                ImageUse::Fixed(Some(240)),
                &local,
                epoch,
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(80));
                    None
                },
            )
        }));
    }
    barrier.wait();
    assert!(joins.into_iter().all(|join| join.join().unwrap().is_none()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(decoded_cache_stats().0, 0);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn settings_revision_discards_blocked_normal_and_refresh_completions() {
    let _serial = serial();
    let original = nova_config::image_settings_snapshot().0;
    decoded_cache_clear();
    let initial = CacheSettings {
        cache_images: true,
        enabled: true,
        format: CacheImageFormat::Jpeg,
        quality: 71,
        detail_poster_max_side: Some(480),
        ..settings()
    };
    nova_config::set_cache_settings(initial.clone());
    let dir = scratch("stale-settings-normal");
    let url = "https://unit.test/stale-settings-normal";
    let old_key =
        image_recipe_cache_key(url, ImageRecipe::resolve(ImageUse::DetailPoster, &initial));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let local = initial.clone();
    let worker_dir = dir.clone();
    let worker = std::thread::spawn(move || {
        cached_pixels_with(&worker_dir, url, ImageUse::DetailPoster, &local, || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Some(source_image(900, 450, false))
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    nova_config::set_cache_settings(CacheSettings {
        detail_poster_max_side: Some(720),
        ..initial.clone()
    });
    release_tx.send(()).unwrap();
    let normal_result = worker.join().unwrap();
    let normal_variant_count = disk_variants(&dir).len();
    let old_memory = decoded_cache_get(&old_key).is_some();

    // Repeat through the always-refresh path: an old recipe completion must
    // not write or publish pixels after the settings revision changes either.
    nova_config::set_cache_settings(initial.clone());
    let refresh_dir = scratch("stale-settings-refresh");
    let refresh_url = "https://unit.test/stale-settings-refresh";
    let epoch = LoadEpoch::capture();
    let (refresh_entered_tx, refresh_entered_rx) = std::sync::mpsc::channel();
    let (refresh_release_tx, refresh_release_rx) = std::sync::mpsc::channel();
    let refresh_local = initial.clone();
    let worker_dir = refresh_dir.clone();
    let refresh_worker = std::thread::spawn(move || {
        refresh_image_if_changed_with(
            &worker_dir,
            refresh_url,
            ImageUse::DetailPoster,
            &refresh_local,
            epoch,
            || {
                refresh_entered_tx.send(()).unwrap();
                refresh_release_rx.recv().unwrap();
                Some(source_image(900, 450, false))
            },
        )
    });
    refresh_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    nova_config::set_cache_settings(CacheSettings {
        detail_poster_max_side: Some(720),
        ..initial
    });
    refresh_release_tx.send(()).unwrap();
    let refresh_result = refresh_worker.join().unwrap();
    let refresh_variant_count = disk_variants(&refresh_dir).len();

    nova_config::set_cache_settings(original);
    decoded_cache_clear();
    assert!(normal_result.is_none());
    assert_eq!(normal_variant_count, 0);
    assert!(!old_memory);
    assert!(refresh_result.is_none());
    assert_eq!(refresh_variant_count, 0);
    fs::remove_dir_all(dir).unwrap();
    fs::remove_dir_all(refresh_dir).unwrap();
}

#[test]
fn newer_refresh_displaces_blocked_normal_load_even_without_pixel_retention() {
    let _serial = serial();
    for disk in [false, true] {
        for budget in [0, 128] {
            for old_download_fails in [false, true] {
                decoded_cache_clear();
                set_decoded_cache_budget(budget);
                let dir = scratch("normal-refresh-order");
                let url = "https://unit.test/normal-refresh-order";
                let local = CacheSettings {
                    cache_images: disk,
                    enabled: false,
                    ..settings()
                };
                let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &local);
                let newest_bytes = source_image(24, 24, false);
                let expected = process_source(newest_bytes.clone(), recipe).unwrap().pixels;
                let (started_tx, started_rx) = std::sync::mpsc::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let worker_dir = dir.clone();
                let worker_settings = local.clone();
                let worker = std::thread::spawn(move || {
                    cached_pixels_with(
                        &worker_dir,
                        url,
                        ImageUse::Fixed(None),
                        &worker_settings,
                        || {
                            started_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                            (!old_download_fails).then(|| source_image(32, 16, false))
                        },
                    )
                });
                started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                let refreshed = refresh_image_if_changed_with(
                    &dir,
                    url,
                    ImageUse::Fixed(None),
                    &local,
                    LoadEpoch::capture(),
                    || Some(newest_bytes),
                )
                .unwrap();
                assert!(image_buffers_equal(&refreshed, &expected));
                if budget == 0 {
                    assert_eq!(decoded_cache_stats(), (0, 0));
                }
                // The newest pixels must remain available to older work even
                // when neither the disk cache nor the decoded LRU holds them.
                assert_eq!(PUBLICATIONS.lock().unwrap().len(), 1);
                release_tx.send(()).unwrap();
                let displaced = worker.join().unwrap().unwrap();
                assert!(image_buffers_equal(&displaced, &expected));
                assert!(PUBLICATIONS.lock().unwrap().is_empty());
                if disk {
                    let variant = read_variant(&recipe_disk_path(&dir, url, recipe)).unwrap();
                    assert!(image_buffers_equal(
                        &variant_pixels(&variant, url, recipe).unwrap(),
                        &expected
                    ));
                } else {
                    assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
                }
                if budget > 0 {
                    assert!(image_buffers_equal(
                        &decoded_cache_get(&image_recipe_cache_key(url, recipe)).unwrap(),
                        &expected
                    ));
                }
                fs::remove_dir_all(dir).unwrap();
            }
        }
    }
    decoded_cache_clear();
    set_decoded_cache_budget(128);
}

#[test]
fn ordinary_load_joins_active_refresh_instead_of_returning_old_cached_pixels() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("ordinary-joins-refresh");
    let url = "https://unit.test/ordinary-joins-refresh";
    let local = CacheSettings {
        enabled: false,
        ..settings()
    };
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &local);
    cached_pixels_with(&dir, url, ImageUse::Fixed(None), &local, || {
        Some(source_image(32, 16, false))
    })
    .unwrap();
    let epoch = LoadEpoch::capture();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_dir = dir.clone();
    let worker_settings = local.clone();
    let refresh = std::thread::spawn(move || {
        refresh_image_if_changed_with(
            &worker_dir,
            url,
            ImageUse::Fixed(None),
            &worker_settings,
            epoch,
            || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Some(source_image(24, 24, false))
            },
        )
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let worker_dir = dir.clone();
    let normal = std::thread::spawn(move || {
        cached_pixels_with(&worker_dir, url, ImageUse::Fixed(None), &local, || {
            panic!("must join refresh")
        })
    });
    let flight_key = format!(
        "{}:{}:{}:{}",
        dir.display(),
        epoch.clear,
        epoch.settings,
        image_recipe_cache_key(url, recipe)
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let joined = REFRESH_IN_FLIGHT
            .lock()
            .unwrap()
            .get(&flight_key)
            .is_some_and(|flight| Arc::strong_count(flight) >= 3);
        if joined {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ordinary caller never joined active refresh"
        );
        std::thread::yield_now();
    }
    release_tx.send(()).unwrap();
    let refreshed = refresh.join().unwrap().unwrap();
    let loaded = normal.join().unwrap().unwrap();
    assert!(image_buffers_equal(&refreshed, &loaded));
    assert_eq!((loaded.width(), loaded.height()), (24, 24));
    assert!(PUBLICATIONS.lock().unwrap().is_empty());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn failed_refresh_preserves_good_memory_and_disk_entries() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("failed-refresh-preserves");
    let url = "https://unit.test/failed-refresh-preserves";
    let local = settings();
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(Some(24)), &local);
    let expected = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(24)), &local, || {
        Some(source_image(32, 16, false))
    })
    .unwrap();
    let path = recipe_disk_path(&dir, url, recipe);
    let before = fs::read(&path).unwrap();
    for source in [None, Some(vec![0, 1, 2])] {
        assert!(
            refresh_image_if_changed_with(
                &dir,
                url,
                ImageUse::Fixed(Some(24)),
                &local,
                LoadEpoch::capture(),
                || source
            )
            .is_none()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(image_buffers_equal(
            &decoded_cache_get(&image_recipe_cache_key(url, recipe)).unwrap(),
            &expected
        ));
        assert!(PUBLICATIONS.lock().unwrap().is_empty());
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn old_maintenance_snapshot_cannot_retire_a_use_rebuilt_by_newer_settings() {
    let _serial = serial();
    let original = nova_config::image_settings_snapshot().0;
    decoded_cache_clear();
    let dir = scratch("maintenance-newer-registration");
    let url = "https://unit.test/maintenance-newer-registration";
    let initial = CacheSettings {
        enabled: false,
        detail_poster_max_side: Some(24),
        ..settings()
    };
    nova_config::set_cache_settings(initial.clone());
    let bytes = source_image(80, 40, false);
    cached_pixels_with(&dir, url, ImageUse::DetailPoster, &initial, || {
        Some(bytes.clone())
    })
    .unwrap();
    let target = CacheSettings {
        detail_poster_max_side: Some(32),
        ..initial.clone()
    };
    let newer = CacheSettings {
        detail_poster_max_side: Some(40),
        ..initial
    };
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_dir = dir.clone();
    let worker_bytes = bytes.clone();
    let maintenance = std::thread::spawn(move || {
        rewrite_entries_with(
            &worker_dir,
            &target,
            &AtomicBool::new(false),
            |_| {},
            |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Some(worker_bytes.clone())
            },
        )
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    nova_config::set_cache_settings(newer.clone());
    let expected =
        cached_pixels_with(&dir, url, ImageUse::DetailPoster, &newer, || Some(bytes)).unwrap();
    let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &newer);
    let path = recipe_disk_path(&dir, url, recipe);
    let before = fs::read(&path).unwrap();
    release_tx.send(()).unwrap();
    let progress = maintenance.join().unwrap();
    nova_config::set_cache_settings(original);
    assert_eq!(progress.converted, 0);
    assert_eq!(progress.skipped, 1);
    assert_eq!(progress.failed, 0);
    assert_eq!(fs::read(path).unwrap(), before);
    let variants = disk_variants(&dir);
    assert_eq!(variants.len(), 1);
    assert!(image_buffers_equal(
        &variant_pixels(&variants[0].1, url, recipe).unwrap(),
        &expected
    ));
    assert!(PUBLICATIONS.lock().unwrap().is_empty());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn maintenance_source_cannot_overwrite_a_newer_same_recipe_refresh() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("maintenance-refresh-order");
    let url = "https://unit.test/maintenance-refresh-order";
    let local = CacheSettings {
        enabled: false,
        ..settings()
    };
    cached_pixels_with(&dir, url, ImageUse::Fixed(None), &local, || {
        Some(source_image(32, 16, false))
    })
    .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_dir = dir.clone();
    let worker_settings = local.clone();
    let maintenance = std::thread::spawn(move || {
        rewrite_entries_with(
            &worker_dir,
            &worker_settings,
            &AtomicBool::new(false),
            |_| {},
            |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Some(source_image(40, 20, false))
            },
        )
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let expected = refresh_image_if_changed_with(
        &dir,
        url,
        ImageUse::Fixed(None),
        &local,
        LoadEpoch::capture(),
        || Some(source_image(24, 24, false)),
    )
    .unwrap();
    release_tx.send(()).unwrap();
    let progress = maintenance.join().unwrap();
    assert_eq!(progress.converted, 0);
    assert_eq!(progress.skipped, 1);
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &local);
    let variant = read_variant(&recipe_disk_path(&dir, url, recipe)).unwrap();
    assert!(image_buffers_equal(
        &variant_pixels(&variant, url, recipe).unwrap(),
        &expected
    ));
    assert!(PUBLICATIONS.lock().unwrap().is_empty());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn maintenance_snapshot_keeps_newer_publication_sequence_when_disk_replacement_fails() {
    let _serial = serial();
    decoded_cache_clear();
    set_decoded_cache_budget(0);
    let dir = scratch("maintenance-transient-publication");
    let url = "https://unit.test/maintenance-transient-publication";
    let local = CacheSettings {
        enabled: false,
        ..settings()
    };
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &local);
    let old_source = source_image(32, 16, false);
    cached_pixels_with(&dir, url, ImageUse::Fixed(None), &local, || {
        Some(old_source.clone())
    })
    .unwrap();
    let path = recipe_disk_path(&dir, url, recipe);
    let before = fs::read(&path).unwrap();
    let mut refreshed = false;
    let progress = rewrite_entries_with(
        &dir,
        &local,
        &AtomicBool::new(false),
        |progress| {
            if progress.processed != 0 || refreshed {
                return;
            }
            refreshed = true;
            // Maintenance already owns this recipe's ticket at its snapshot
            // callback. Fail only the refresh's atomic disk replacement.
            let sequence = TEMP_SEQUENCE.load(Ordering::Relaxed);
            let blocker = path.with_file_name(format!(
                "{}.tmp-{}-{sequence}",
                path.file_name().unwrap().to_string_lossy(),
                std::process::id(),
            ));
            fs::create_dir(&blocker).unwrap();
            let newer = refresh_image_if_changed_with(
                &dir,
                url,
                ImageUse::Fixed(None),
                &local,
                LoadEpoch::capture(),
                || Some(source_image(24, 24, false)),
            )
            .unwrap();
            assert_eq!((newer.width(), newer.height()), (24, 24));
            assert_eq!(decoded_cache_stats(), (0, 0));
            assert_eq!(fs::read(&path).unwrap(), before);
            fs::remove_dir(blocker).unwrap();
        },
        |_| Some(old_source.clone()),
    );
    assert!(refreshed);
    assert_eq!(progress.converted, 0);
    assert_eq!(progress.skipped, 1);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(PUBLICATIONS.lock().unwrap().is_empty());
    set_decoded_cache_budget(128);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn old_maintenance_snapshot_cannot_reclaim_identical_registration_after_settings_aba() {
    let _serial = serial();
    let original = nova_config::image_settings_snapshot().0;
    decoded_cache_clear();
    let dir = scratch("maintenance-settings-aba");
    let url = "https://unit.test/maintenance-settings-aba";
    let initial = CacheSettings {
        enabled: false,
        detail_poster_max_side: Some(24),
        ..settings()
    };
    nova_config::set_cache_settings(initial.clone());
    let bytes = source_image(80, 40, false);
    cached_pixels_with(&dir, url, ImageUse::DetailPoster, &initial, || {
        Some(bytes.clone())
    })
    .unwrap();
    let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &initial);
    let path = recipe_disk_path(&dir, url, recipe);
    let captured = fs::read(&path).unwrap();
    let target = CacheSettings {
        detail_poster_max_side: Some(32),
        ..initial.clone()
    };
    let intermediate = CacheSettings {
        detail_poster_max_side: Some(40),
        ..initial.clone()
    };
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_dir = dir.clone();
    let worker_bytes = bytes.clone();
    let maintenance = std::thread::spawn(move || {
        rewrite_entries_with(
            &worker_dir,
            &target,
            &AtomicBool::new(false),
            |_| {},
            |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Some(worker_bytes.clone())
            },
        )
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    nova_config::set_cache_settings(intermediate.clone());
    cached_pixels_with(&dir, url, ImageUse::DetailPoster, &intermediate, || {
        Some(bytes.clone())
    })
    .unwrap();
    nova_config::set_cache_settings(initial.clone());
    cached_pixels_with(&dir, url, ImageUse::DetailPoster, &initial, || Some(bytes)).unwrap();
    // Recipe, source checksum and envelope bytes recur, but ownership belongs
    // to the newer successful settings load rather than the old snapshot.
    assert_eq!(fs::read(&path).unwrap(), captured);
    release_tx.send(()).unwrap();
    let progress = maintenance.join().unwrap();
    nova_config::set_cache_settings(original);
    assert_eq!(progress.converted, 0);
    assert_eq!(progress.skipped, 1);
    assert_eq!(fs::read(path).unwrap(), captured);
    assert_eq!(disk_variants(&dir).len(), 1);
    assert!(PUBLICATIONS.lock().unwrap().is_empty());
    assert!(REGISTRATION_WATCHERS.lock().unwrap().is_empty());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn queued_maintenance_keeps_only_sequences_except_while_an_older_reader_needs_pixels() {
    let _serial = serial();
    decoded_cache_clear();
    set_decoded_cache_budget(0);
    let dir = scratch("maintenance-metadata-only");
    let disk_settings = CacheSettings {
        enabled: false,
        ..settings()
    };
    let memory_settings = CacheSettings {
        cache_images: false,
        ..disk_settings.clone()
    };
    let urls: Vec<_> = (0..24)
        .map(|index| format!("https://unit.test/maintenance-metadata-only/{index}"))
        .collect();
    let old_source = source_image(32, 16, false);
    for url in &urls {
        cached_pixels_with(&dir, url, ImageUse::Fixed(None), &disk_settings, || {
            Some(old_source.clone())
        })
        .unwrap();
    }
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker_dir = dir.clone();
    let worker_url = urls[0].clone();
    let worker_settings = memory_settings.clone();
    let mut old_reader = Some(std::thread::spawn(move || {
        cached_pixels_with(
            &worker_dir,
            &worker_url,
            ImageUse::Fixed(None),
            &worker_settings,
            || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Some(source_image(32, 16, false))
            },
        )
    }));
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut published = false;
    let progress = rewrite_entries_with(
        &dir,
        &disk_settings,
        &AtomicBool::new(false),
        |progress| {
            if progress.processed != 0 || published {
                return;
            }
            published = true;
            let newest = refresh_image_if_changed_with(
                &dir,
                &urls[0],
                ImageUse::Fixed(None),
                &memory_settings,
                LoadEpoch::capture(),
                || Some(source_image(24, 24, false)),
            )
            .unwrap();
            for url in urls.iter().skip(1) {
                let result =
                    cached_pixels_with(&dir, url, ImageUse::Fixed(None), &memory_settings, || {
                        Some(source_image(24, 24, false))
                    })
                    .unwrap();
                assert_eq!((result.width(), result.height()), (24, 24));
            }
            assert_eq!(decoded_cache_stats(), (0, 0));
            {
                let publications = PUBLICATIONS.lock().unwrap();
                assert_eq!(publications.len(), urls.len());
                assert!(publications.values().all(|state| state.newest.is_some()));
                // Only the blocked reader needs a pixel handoff. The other
                // completed ordinary results leave sequence evidence alone.
                assert_eq!(
                    publications
                        .values()
                        .filter(|state| state.newest.as_ref().unwrap().pixels.is_some())
                        .count(),
                    1
                );
            }
            release_tx.send(()).unwrap();
            let displaced = old_reader.take().unwrap().join().unwrap().unwrap();
            assert!(image_buffers_equal(&displaced, &newest));
            let publications = PUBLICATIONS.lock().unwrap();
            assert_eq!(publications.len(), urls.len());
            assert!(
                publications.values().all(|state| state.readers.is_empty()
                    && state.newest.as_ref().unwrap().pixels.is_none())
            );
        },
        |_| Some(old_source.clone()),
    );
    assert!(published);
    assert_eq!(progress.converted, 0);
    assert_eq!(progress.skipped, urls.len());
    assert_eq!(progress.processed, urls.len());
    assert!(PUBLICATIONS.lock().unwrap().is_empty());
    assert!(REGISTRATION_WATCHERS.lock().unwrap().is_empty());
    set_decoded_cache_budget(128);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn settings_change_while_source_commit_waits_for_url_lock_preserves_newer_use() {
    let _serial = serial();
    let original = nova_config::image_settings_snapshot().0;
    for refresh in [false, true] {
        decoded_cache_clear();
        let dir = scratch("settings-change-url-lock");
        let url = "https://unit.test/settings-change-url-lock";
        let initial = CacheSettings {
            enabled: false,
            detail_poster_max_side: Some(24),
            ..settings()
        };
        nova_config::set_cache_settings(initial.clone());
        let old_recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &initial);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_dir = dir.clone();
        let worker = std::thread::spawn(move || {
            let download = || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Some(source_image(80, 40, false))
            };
            if refresh {
                refresh_image_if_changed_with(
                    &worker_dir,
                    url,
                    ImageUse::DetailPoster,
                    &initial,
                    LoadEpoch::capture(),
                    download,
                )
            } else {
                cached_pixels_with(&worker_dir, url, ImageUse::DetailPoster, &initial, download)
            }
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let lock = url_lock(&dir, url);
        let guard = lock.lock().unwrap();
        release_tx.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while Arc::strong_count(&lock) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "old source never reached the held URL lock"
            );
            std::thread::yield_now();
        }
        // Publish a newer use while owning the lock the old commit is waiting
        // for. This fixes the ordering without relying on mutex wake-up order.
        let newer = CacheSettings {
            enabled: false,
            detail_poster_max_side: Some(40),
            ..settings()
        };
        nova_config::set_cache_settings(newer.clone());
        let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &newer);
        let expected = process_source(source_image(80, 40, false), recipe).unwrap();
        persist_processed(&dir, url, &[ImageUse::DetailPoster], recipe, &expected).unwrap();
        decoded_cache_insert(
            &image_recipe_cache_key(url, recipe),
            expected.pixels.clone(),
        );
        let path = recipe_disk_path(&dir, url, recipe);
        let before = fs::read(&path).unwrap();
        drop(guard);
        assert!(worker.join().unwrap().is_none());
        assert_eq!(fs::read(path).unwrap(), before);
        assert_eq!(disk_variants(&dir).len(), 1);
        assert!(!recipe_disk_path(&dir, url, old_recipe).exists());
        assert!(decoded_cache_get(&image_recipe_cache_key(url, old_recipe)).is_none());
        assert!(image_buffers_equal(
            &decoded_cache_get(&image_recipe_cache_key(url, recipe)).unwrap(),
            &expected.pixels
        ));
        assert!(PUBLICATIONS.lock().unwrap().is_empty());
        fs::remove_dir_all(dir).unwrap();
    }
    nova_config::set_cache_settings(original);
    decoded_cache_clear();
}

#[test]
fn stale_follower_registration_waiting_for_url_lock_cannot_reclaim_newer_use() {
    let _serial = serial();
    let original = nova_config::image_settings_snapshot().0;
    decoded_cache_clear();
    let dir = scratch("settings-change-follower-lock");
    let url = "https://unit.test/settings-change-follower-lock";
    let initial = CacheSettings {
        enabled: false,
        detail_poster_max_side: Some(24),
        ..settings()
    };
    nova_config::set_cache_settings(initial.clone());
    let epoch = LoadEpoch::capture();
    let old_recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &initial);
    let old = process_source(source_image(80, 40, false), old_recipe).unwrap();
    // Another role keeps the old recipe available after DetailPoster moves.
    persist_processed(&dir, url, &[ImageUse::Fixed(Some(24))], old_recipe, &old).unwrap();
    let lock = url_lock(&dir, url);
    let guard = lock.lock().unwrap();
    let worker_dir = dir.clone();
    let follower = std::thread::spawn(move || {
        let _mutation = CACHE_MUTATION.read().unwrap();
        assert!(epoch.current());
        register_refreshed_use(
            &worker_dir,
            url,
            ImageUse::DetailPoster,
            old_recipe,
            &old.pixels,
            epoch,
        );
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while Arc::strong_count(&lock) < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "follower never reached the held URL lock"
        );
        std::thread::yield_now();
    }
    let newer = CacheSettings {
        detail_poster_max_side: Some(40),
        ..initial
    };
    nova_config::set_cache_settings(newer.clone());
    let recipe = ImageRecipe::resolve(ImageUse::DetailPoster, &newer);
    let expected = process_source(source_image(80, 40, false), recipe).unwrap();
    persist_processed(&dir, url, &[ImageUse::DetailPoster], recipe, &expected).unwrap();
    let path = recipe_disk_path(&dir, url, recipe);
    let before = fs::read(&path).unwrap();
    drop(guard);
    follower.join().unwrap();
    nova_config::set_cache_settings(original);
    assert_eq!(fs::read(path).unwrap(), before);
    let old_variant = read_variant(&recipe_disk_path(&dir, url, old_recipe)).unwrap();
    assert_eq!(old_variant.metadata.uses, [ImageUse::Fixed(Some(24))]);
    assert_eq!(disk_variants(&dir).len(), 2);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn jxl_recipes_keep_quality_separate_and_reuse_processed_variants() {
    let _serial = serial();
    let dir = scratch("jxl-shared-codec");
    for format in [CacheImageFormat::JxlLossless, CacheImageFormat::JxlLossy] {
        let settings = CacheSettings {
            format,
            jxl_quality: 91,
            quality: 37,
            avif_quality: 62,
            ..settings()
        };
        let url = format!("https://unit.test/{format:?}");
        let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &settings);
        assert_eq!(
            recipe.quality,
            if format == CacheImageFormat::JxlLossless {
                0
            } else {
                91
            }
        );
        let cold = cached_pixels_with(&dir, &url, ImageUse::Fixed(None), &settings, || {
            Some(source_image(32, 48, true))
        })
        .unwrap();
        let variant = read_variant(&recipe_disk_path(&dir, &url, recipe)).unwrap();
        assert!(nova_image::is_jpeg_xl(&variant.encoded));
        decoded_cache_clear();
        let disk = cached_pixels_with(&dir, &url, ImageUse::Fixed(None), &settings, || {
            panic!("shared JPEG XL disk hit")
        })
        .unwrap();
        assert!(image_buffers_equal(&cold, &disk));
        let source =
            nova_image::decode_jxl(&variant.encoded, nova_image::DecodeLimits::default()).unwrap();
        assert_eq!(source.dimensions(), (32, 48));
    }
    let old: CacheSettings = serde_json::from_str(r#"{"format":"webp","quality":50}"#).unwrap();
    assert_eq!(old.jxl_quality, 85);
    for quality in [0, 101] {
        assert!(
            serde_json::from_value::<CacheSettings>(serde_json::json!({"jxl_quality":quality}))
                .is_err()
        );
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn jpeg_transcode_is_opt_in_and_skipped_for_resizing_or_non_jpeg() {
    let source = image::RgbImage::from_fn(32, 24, |x, y| {
        image::Rgb([(x * 7) as u8, (y * 9) as u8, ((x + y) * 4) as u8])
    });
    let mut jpeg = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 85)
        .encode_image(&source)
        .unwrap();
    let settings = CacheSettings {
        enabled: true,
        format: CacheImageFormat::JxlLossy,
        jxl_quality: 35,
        jxl_lossless_jpeg: true,
        ..CacheSettings::default()
    };
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &settings);
    assert!(recipe.key().contains("jxl_lossless_jpeg"));
    let result = process_source(jpeg.clone(), recipe)
        .unwrap()
        .encoded
        .unwrap();
    assert_eq!(
        result,
        nova_image::transcode_jpeg_to_jxl(&jpeg, nova_image::DecodeLimits::default()).unwrap()
    );
    let resized = process_source(
        jpeg.clone(),
        ImageRecipe {
            max_side: Some(16),
            ..recipe
        },
    )
    .unwrap();
    assert_eq!((resized.pixels.width(), resized.pixels.height()), (16, 12));
    let normal = process_source(
        jpeg,
        ImageRecipe {
            jxl_lossless_jpeg: false,
            ..recipe
        },
    )
    .unwrap()
    .encoded
    .unwrap();
    assert_ne!(result, normal);
    assert!(
        !ImageRecipe {
            jxl_lossless_jpeg: false,
            ..recipe
        }
        .key()
        .contains("jxl_lossless_jpeg")
    );
    let png = image::RgbaImage::from_pixel(3, 2, image::Rgba([40, 80, 120, 127]));
    let mut bytes = std::io::Cursor::new(Vec::new());
    png.write_to(&mut bytes, image::ImageFormat::Png).unwrap();
    assert!(
        process_source(bytes.into_inner(), recipe)
            .unwrap()
            .encoded
            .is_some()
    );
}

fn bounded_variant_fixture(encoded: Vec<u8>) -> DiskVariant {
    DiskVariant {
        metadata: VariantMetadata {
            version: 2,
            url: "bounded-fixture".into(),
            uses: vec![ImageUse::Fixed(None)],
            width: 1,
            height: 1,
            recipe: ImageRecipe {
                max_side: None,
                compress: false,
                format: None,
                quality: 0,
                jxl_lossless_jpeg: false,
            },
            checksum: fnv1a(&encoded),
        },
        encoded,
    }
}
fn bounded_variant_envelope(variant: &DiskVariant) -> Vec<u8> {
    let metadata = serde_json::to_vec(&variant.metadata).unwrap();
    let mut data = MAGIC.to_vec();
    data.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    data.extend_from_slice(&metadata);
    data.extend_from_slice(&variant.encoded);
    data
}

#[test]
fn variant_reader_bounds_actual_payload_for_missing_and_dishonest_length_hints() {
    use std::io::Read;
    struct Counted {
        reader: std::io::Cursor<Vec<u8>>,
        consumed: usize,
    }
    impl Read for Counted {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            let count = self.reader.read(bytes)?;
            self.consumed += count;
            Ok(count)
        }
    }
    for size in [31, 32, 33] {
        let variant = bounded_variant_fixture(vec![7; size]);
        let envelope = bounded_variant_envelope(&variant);
        let overhead = envelope.len() - size;
        for hint in [None, Some(0), Some(envelope.len() as u64)] {
            let mut reader = Counted {
                reader: std::io::Cursor::new(envelope.clone()),
                consumed: 0,
            };
            let result = read_variant_body(&mut reader, hint, 32);
            assert!(reader.consumed <= overhead + 33);
            if size <= 32 {
                assert_eq!(result.unwrap().encoded, variant.encoded);
            } else {
                assert!(result.is_err());
            }
        }
    }
    let mut growing = bounded_variant_envelope(&bounded_variant_fixture(vec![7; 32]));
    let initial_size = growing.len() as u64;
    growing.extend_from_slice(&[7; 1024]);
    let mut reader = Counted {
        reader: std::io::Cursor::new(growing),
        consumed: 0,
    };
    assert!(read_variant_body(&mut reader, Some(initial_size), 32).is_err());
    assert_eq!(reader.consumed as u64, initial_size + 1);
}

#[test]
fn variant_reader_rejects_truncated_headers_metadata_and_corrupted_payloads() {
    let normal = bounded_variant_envelope(&bounded_variant_fixture(vec![7; 32]));
    for length in [0, 7, 8, 11, 12, normal.len() - 1] {
        assert!(read_variant_body(&normal[..length], None, 32).is_err());
    }
    let mut excessive_metadata = MAGIC.to_vec();
    excessive_metadata.extend_from_slice(&((MAX_VARIANT_METADATA_BYTES + 1) as u32).to_le_bytes());
    assert!(read_variant_body(excessive_metadata.as_slice(), None, 32).is_err());
    let mut corrupt = normal.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(read_variant_body(corrupt.as_slice(), None, 32).is_err());
    let mut variant = bounded_variant_fixture(vec![7; 32]);
    variant.metadata.version = 1;
    assert!(read_variant_body(bounded_variant_envelope(&variant).as_slice(), None, 32).is_err());
    variant.metadata.version = 2;
    variant.metadata.uses.clear();
    assert!(read_variant_body(bounded_variant_envelope(&variant).as_slice(), None, 32).is_err());
    assert!(read_variant_body(normal.as_slice(), None, u64::MAX).is_err());
}

#[test]
fn variant_file_reader_rejects_huge_sparse_files_without_removing_them() {
    let _serial = serial();
    let dir = scratch("bounded-variant-sparse");
    let path = dir.join("sparse.variant");
    fs::File::create(&path).unwrap().set_len(1 << 30).unwrap();
    assert!(read_variant_with_limit(&path, 32).is_none());
    assert_eq!(fs::metadata(&path).unwrap().len(), 1 << 30);
    assert!(read_variant_with_limit(&dir, 32).is_none());
    assert!(read_variant_with_limit(&dir.join("missing.variant"), 32).is_none());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn variant_writes_apply_matching_limits_and_keep_guarded_publication() {
    let _serial = serial();
    let dir = scratch("bounded-variant-write");
    let path = dir.join("normal.variant");
    for size in [31, 32] {
        let variant = bounded_variant_fixture(vec![7; size]);
        assert!(write_variant_if_with_limit(&path, &variant, 32, || true).unwrap());
        assert_eq!(
            read_variant_with_limit(&path, 32).unwrap().encoded,
            variant.encoded
        );
    }
    let previous = fs::read(&path).unwrap();
    assert!(
        write_variant_if_with_limit(&path, &bounded_variant_fixture(vec![7; 33]), 32, || true)
            .is_err()
    );
    assert!(
        !write_variant_if_with_limit(&path, &bounded_variant_fixture(vec![8; 32]), 32, || false)
            .unwrap()
    );
    assert_eq!(fs::read(&path).unwrap(), previous);
    let mut oversized_metadata = bounded_variant_fixture(vec![7; 32]);
    oversized_metadata.metadata.url = "x".repeat(MAX_VARIANT_METADATA_BYTES + 1);
    assert!(write_variant_if_with_limit(&path, &oversized_metadata, 32, || true).is_err());
    assert_eq!(fs::read(&path).unwrap(), previous);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let link = dir.join("linked.variant");
        symlink(&path, &link).unwrap();
        assert!(read_variant_with_limit(&link, 32).is_some());
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn filename_index_merges_new_publications_deletions_and_clear_epochs() {
    let _serial = serial();
    let dir = scratch("filename-merge");
    let mut variant = bounded_variant_fixture(vec![7; 8]);
    let url = variant.metadata.url.clone();
    let old = recipe_disk_path(&dir, &url, variant.metadata.recipe);
    write_variant(&old, &variant).unwrap();
    assert_eq!(indexed_url_paths(&dir, &url), [old.clone()]);
    let scan = FilenameScan::begin(filename_index(&dir));
    let observed = vec![old.file_name().unwrap().to_str().unwrap().to_owned()];
    variant.metadata.recipe.quality = 1;
    let new = recipe_disk_path(&dir, &url, variant.metadata.recipe);
    write_variant(&new, &variant).unwrap();
    fs::remove_file(&old).unwrap();
    gc::removed(&old);
    assert!(scan.publish(observed));
    assert_eq!(indexed_url_paths(&dir, &url), [new.clone()]);
    let stale = FilenameScan::begin(filename_index(&dir));
    gc::cleared(&dir);
    write_variant(&new, &variant).unwrap();
    assert!(!stale.publish(Vec::new()));
    assert_eq!(indexed_url_paths(&dir, &url), [new]);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn filename_inventory_enumerates_once_and_current_lookup_sees_later_writes() {
    let _serial = serial();
    let dir = scratch("filename-inventory");
    for i in 0..100 {
        let mut variant = bounded_variant_fixture(vec![7; 8]);
        variant.metadata.url = format!("inventory/{i}");
        write_variant(
            &recipe_disk_path(&dir, &variant.metadata.url, variant.metadata.recipe),
            &variant,
        )
        .unwrap();
    }
    FILENAME_WORK.with(|work| work.set((0, 0)));
    let cancel = AtomicBool::new(false);
    let inventory = VariantInventory::scan(
        &dir,
        &cancel,
        CACHE_GENERATION.load(Ordering::Acquire),
        |_| {},
    )
    .ok()
    .unwrap();
    assert_eq!(inventory.urls.len(), 100);
    assert_eq!(FILENAME_WORK.with(|work| work.get()), (1, 100));
    for url in &inventory.urls {
        assert!(!use_registrations(&dir, url).is_empty());
    }
    let mut later = bounded_variant_fixture(vec![8; 8]);
    later.metadata.url = "inventory/0".into();
    later.metadata.recipe.quality = 1;
    write_variant(
        &recipe_disk_path(&dir, &later.metadata.url, later.metadata.recipe),
        &later,
    )
    .unwrap();
    assert_eq!(
        use_registrations(&dir, "inventory/0")[&ImageUse::Fixed(None)].len(),
        2
    );
    assert_eq!(FILENAME_WORK.with(|work| work.get()), (1, 100));
    let path = recipe_disk_path(&dir, &later.metadata.url, later.metadata.recipe);
    fs::remove_file(&path).unwrap();
    assert_eq!(
        use_registrations(&dir, "inventory/0")[&ImageUse::Fixed(None)].len(),
        1
    );
    assert_eq!(indexed_url_paths(&dir, "inventory/0").len(), 1);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn filename_inventory_cancellation_observes_the_first_64_entry_checkpoint() {
    let _serial = serial();
    let dir = scratch("filename-cancel");
    for i in 0..128 {
        let mut variant = bounded_variant_fixture(vec![7; 8]);
        variant.metadata.url = format!("cancel/{i}");
        write_variant(
            &recipe_disk_path(&dir, &variant.metadata.url, variant.metadata.recipe),
            &variant,
        )
        .unwrap();
    }
    let cancel = AtomicBool::new(false);
    FILENAME_WORK.with(|work| work.set((0, 0)));
    let result = VariantInventory::scan(
        &dir,
        &cancel,
        CACHE_GENERATION.load(Ordering::Acquire),
        |seen| {
            if seen >= 64 {
                cancel.store(true, Ordering::Release);
            }
        },
    );
    assert!(matches!(result, Err(InventoryFailure::Cancelled)));
    assert_eq!(FILENAME_WORK.with(|work| work.get()), (1, 65));
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 128);
    FILENAME_WORK.with(|work| work.set((0, 0)));
    let report = rewrite_entries_with(
        &dir,
        &settings(),
        &cancel,
        |_| {},
        |_| panic!("cancelled inventory must not fetch"),
    );
    assert!(report.cancelled);
    assert_eq!(report.processed, 0);
    assert_eq!(FILENAME_WORK.with(|work| work.get()), (0, 0));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn indexed_retirement_preserves_collision_checks_legacy_cleanup_and_failed_replacement() {
    let _serial = serial();
    let dir = scratch("filename-retire");
    let variant = bounded_variant_fixture(vec![7; 8]);
    let url = variant.metadata.url.clone();
    let path = recipe_disk_path(&dir, &url, variant.metadata.recipe);
    write_variant(&path, &variant).unwrap();
    let mut foreign = bounded_variant_fixture(vec![8; 8]);
    foreign.metadata.url = "foreign/url".into();
    foreign.metadata.recipe.quality = 1;
    let collision = recipe_disk_path(&dir, &url, foreign.metadata.recipe);
    write_variant(&collision, &foreign).unwrap();
    let legacy = dir.join(format!("{:016x}.img", fnv1a(url.as_bytes())));
    fs::write(&legacy, b"legacy").unwrap();
    assert_eq!(
        use_registrations(&dir, &url)[&ImageUse::Fixed(None)].len(),
        1
    );
    assert!(!write_variant_if(&path, &foreign, || false).unwrap());
    retire_use_history(&dir, &url, &[ImageUse::Fixed(None)], &path);
    assert!(path.exists());
    assert!(collision.exists());
    assert!(!legacy.exists());
    assert_eq!(read_variant(&path).unwrap().metadata.url, url);
    fs::remove_dir_all(dir).unwrap();
}

fn core06_settings() -> CacheSettings {
    CacheSettings {
        cache_images: true,
        enabled: false,
        detail_poster_max_side: None,
        detail_backdrop_max_side: None,
        ..settings()
    }
}
fn core06_populate(dir: &Path, url: &str) -> (CacheSettings, ImageRecipe, Pixels) {
    let settings = core06_settings();
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), &settings);
    let pixels = cached_pixels_with(dir, url, ImageUse::Fixed(None), &settings, || {
        Some(source_image(16, 16, true))
    })
    .unwrap();
    (settings, recipe, pixels)
}
fn core06_block_next_write(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.load(Ordering::Relaxed);
    let blocker = path.with_file_name(format!(
        "{}.tmp-{}-{sequence}",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    fs::create_dir(&blocker).unwrap();
    blocker
}

#[test]
#[cfg(unix)]
fn core06_warm_same_role_has_one_fingerprint_and_no_payload_work() {
    let _serial = serial();
    for size in [32 * 1024, 1024 * 1024] {
        decoded_cache_clear();
        let dir = scratch("core06-warm");
        let url = "https://unit.test/core06-warm";
        let settings = core06_settings();
        let mut source = source_image(16, 16, true);
        source.resize(size, 0);
        let cold = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || Some(source))
            .unwrap();
        REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
        for _ in 0..32 {
            let warm = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
                panic!("warm fetch")
            })
            .unwrap();
            assert_eq!(warm.as_bytes().as_ptr(), cold.as_bytes().as_ptr());
        }
        assert_eq!(REGISTRATION_WORK.with(|work| work.get()), (32, 0, 0));
        REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
        assert!(
            decoded_cache_get(&image_recipe_cache_key(
                url,
                ImageRecipe::resolve(ImageUse::Fixed(None), settings)
            ))
            .is_some()
        );
        assert_eq!(REGISTRATION_WORK.with(|work| work.get()), (0, 0, 0));
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn core06_new_role_requires_durable_registration_then_warms() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-role");
    let url = "https://unit.test/core06-role";
    let (settings, recipe, pixels) = core06_populate(&dir, url);
    REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
    let role = cached_pixels_with(&dir, url, ImageUse::DetailPoster, &settings, || {
        panic!("new role fetch")
    })
    .unwrap();
    assert!(image_buffers_equal(&pixels, &role));
    assert!(REGISTRATION_WORK.with(|work| work.get().1) > 0);
    assert!(
        read_variant(&recipe_disk_path(&dir, url, recipe))
            .unwrap()
            .metadata
            .uses
            .contains(&ImageUse::DetailPoster)
    );
    REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
    assert!(
        cached_pixels_with(&dir, url, ImageUse::DetailPoster, &settings, || panic!(
            "registered fetch"
        ))
        .is_some()
    );
    assert_eq!(REGISTRATION_WORK.with(|work| work.get()), (1, 0, 0));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn core06_external_unlink_truncate_replace_and_rewrite_recover_current_pixels() {
    let _serial = serial();
    for mutation in 0..4 {
        decoded_cache_clear();
        let dir = scratch("core06-mutation");
        let url = "https://unit.test/core06-mutation";
        let (settings, recipe, old) = core06_populate(&dir, url);
        let path = recipe_disk_path(&dir, url, recipe);
        if mutation == 0 {
            fs::remove_file(&path).unwrap();
        } else if mutation == 1 {
            fs::write(&path, b"truncated").unwrap();
        } else {
            let processed = process_source(source_image(12, 9, false), recipe).unwrap();
            let variant = DiskVariant {
                metadata: VariantMetadata {
                    version: 2,
                    url: url.into(),
                    uses: vec![ImageUse::Fixed(None)],
                    width: 12,
                    height: 9,
                    recipe,
                    checksum: fnv1a(processed.encoded.as_ref().unwrap()),
                },
                encoded: processed.encoded.unwrap(),
            };
            let replacement = dir.join("external.variant");
            write_variant(&replacement, &variant).unwrap();
            if mutation == 2 {
                fs::rename(replacement, &path).unwrap();
            } else {
                fs::write(&path, fs::read(replacement).unwrap()).unwrap();
            }
        }
        let fetches = AtomicUsize::new(0);
        let current = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
            fetches.fetch_add(1, Ordering::Relaxed);
            Some(source_image(12, 9, false))
        })
        .unwrap();
        assert!(!image_buffers_equal(&old, &current));
        assert_eq!((current.width(), current.height()), (12, 9));
        assert_eq!(fetches.load(Ordering::Relaxed), usize::from(mutation < 2));
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn core06_generations_revoke_proof_even_when_recipe_and_file_recur() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-generation");
    let url = "https://unit.test/core06-generation";
    let (settings, recipe, _) = core06_populate(&dir, url);
    let key = image_recipe_cache_key(url, recipe);
    let (_, Some(proof)) = decoded_registration_snapshot(&key).unwrap() else {
        panic!("proof");
    };
    CACHE_GENERATION.fetch_add(1, Ordering::AcqRel);
    REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
    cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
        panic!("generation fetch")
    })
    .unwrap();
    assert!(REGISTRATION_WORK.with(|work| work.get().1) > 0);
    // Inject an older settings revision to model an A→B→A recipe recurrence.
    let (publication, Some(mut stale)) = decoded_registration_snapshot(&key).unwrap() else {
        panic!("proof");
    };
    Arc::make_mut(&mut stale).settings = stale.settings.wrapping_sub(1);
    attach_registration(&key, publication, Some(stale));
    REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
    cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
        panic!("ABA fetch")
    })
    .unwrap();
    assert!(REGISTRATION_WORK.with(|work| work.get().1) > 0);
    assert_ne!(proof.clear, CACHE_GENERATION.load(Ordering::Acquire));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn core06_failed_source_replacement_does_not_bind_new_pixels_to_old_file() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-failed-source");
    let url = "https://unit.test/core06-failed-source";
    let (_, recipe, old) = core06_populate(&dir, url);
    let path = recipe_disk_path(&dir, url, recipe);
    let before = fs::read(&path).unwrap();
    let blocker = core06_block_next_write(&path);
    let ticket =
        PublicationTicket::acquire(&dir, url, recipe, CACHE_GENERATION.load(Ordering::Acquire));
    let new = publish_processed_locked(
        &dir,
        url,
        &[ImageUse::Fixed(None)],
        true,
        &ticket,
        process_source(source_image(12, 9, false), recipe),
    )
    .unwrap();
    assert!(!image_buffers_equal(&old, &new));
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(
        decoded_registration_snapshot(&image_recipe_cache_key(url, recipe))
            .unwrap()
            .1
            .is_none()
    );
    register_refreshed_use_locked(&dir, url, ImageUse::DetailPoster, recipe, &new);
    assert!(
        !read_variant(&path)
            .unwrap()
            .metadata
            .uses
            .contains(&ImageUse::DetailPoster)
    );
    fs::remove_dir(blocker).unwrap();
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn core06_failed_role_write_never_proves_the_new_role() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-failed-role");
    let url = "https://unit.test/core06-failed-role";
    let (_, recipe, pixels) = core06_populate(&dir, url);
    let path = recipe_disk_path(&dir, url, recipe);
    let blocker = core06_block_next_write(&path);
    assert!(!register_cached_pixels_locked(
        &dir,
        url,
        ImageUse::DetailPoster,
        recipe,
        &pixels
    ));
    assert!(
        !read_variant(&path)
            .unwrap()
            .metadata
            .uses
            .contains(&ImageUse::DetailPoster)
    );
    assert!(
        decoded_registration_snapshot(&image_recipe_cache_key(url, recipe))
            .unwrap()
            .1
            .is_none()
    );
    fs::remove_dir(blocker).unwrap();
    assert!(register_cached_pixels_locked(
        &dir,
        url,
        ImageUse::DetailPoster,
        recipe,
        &pixels
    ));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn core06_memory_only_lossy_load_fetches_original_when_disk_is_enabled() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-enable-disk");
    let url = "https://unit.test/core06-enable-disk";
    let mut settings = CacheSettings {
        cache_images: false,
        enabled: true,
        format: CacheImageFormat::Jpeg,
        quality: 20,
        ..settings()
    };
    let source = source_image(40, 30, false);
    let memory = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(20)), &settings, || {
        Some(source.clone())
    })
    .unwrap();
    settings.cache_images = true;
    let fetches = AtomicUsize::new(0);
    let disk = cached_pixels_with(&dir, url, ImageUse::Fixed(Some(20)), &settings, || {
        fetches.fetch_add(1, Ordering::Relaxed);
        Some(source)
    })
    .unwrap();
    assert_eq!(fetches.load(Ordering::Relaxed), 1);
    assert!(image_buffers_equal(&memory, &disk));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn core06_directory_identity_prevents_cross_namespace_proof_reuse() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-dir-a");
    let other = scratch("core06-dir-b");
    let url = "https://unit.test/core06-dir";
    let (settings, _, old) = core06_populate(&dir, url);
    let new = cached_pixels_with(&other, url, ImageUse::Fixed(None), &settings, || {
        Some(source_image(12, 9, false))
    })
    .unwrap();
    assert!(!image_buffers_equal(&old, &new));
    let recovered = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
        panic!("first directory still durable")
    })
    .unwrap();
    assert!(image_buffers_equal(&old, &recovered));
    fs::remove_dir_all(dir).unwrap();
    fs::remove_dir_all(other).unwrap();
}

#[test]
#[cfg(unix)]
fn core06_lru_replacement_eviction_remove_and_clear_bound_evidence_lifetime() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-lifetime");
    let url = "https://unit.test/core06-lifetime";
    let (_, recipe, pixels) = core06_populate(&dir, url);
    let key = image_recipe_cache_key(url, recipe);
    let (old_id, old_proof) = decoded_registration_snapshot(&key).unwrap();
    assert!(old_proof.is_some());
    decoded_cache_insert(&key, pixels.clone());
    let (new_id, new_proof) = decoded_registration_snapshot(&key).unwrap();
    assert_ne!(old_id, new_id);
    assert!(new_proof.is_none());
    attach_registration(&key, old_id, old_proof.clone());
    assert!(decoded_registration_snapshot(&key).unwrap().1.is_none());
    attach_registration(&key, new_id, old_proof.clone());
    {
        let mut cache = DECODED_POSTERS.lock().unwrap();
        cache.remove(&key);
    }
    assert!(decoded_registration_snapshot(&key).is_none());
    decoded_cache_insert_registered(&key, pixels.clone(), old_proof.clone());
    set_decoded_cache_budget(0);
    assert!(decoded_registration_snapshot(&key).is_none());
    decoded_cache_insert_registered(&key, pixels.clone(), old_proof.clone());
    assert!(decoded_registration_snapshot(&key).is_none());
    set_decoded_cache_budget(128);
    decoded_cache_insert_registered(&key, pixels, old_proof);
    decoded_cache_clear();
    assert!(decoded_registration_snapshot(&key).is_none());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
#[cfg(unix)]
fn core06_joined_registration_reuses_proof_but_unknown_pixels_require_comparison() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-joined");
    let url = "https://unit.test/core06-joined";
    let (_, recipe, pixels) = core06_populate(&dir, url);
    REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
    register_refreshed_use_locked(&dir, url, ImageUse::Fixed(None), recipe, &pixels);
    assert_eq!(REGISTRATION_WORK.with(|work| work.get()), (1, 0, 0));
    let alien = SharedPixelBuffer::<Rgba8Pixel>::new(16, 16);
    decoded_cache_insert(&image_recipe_cache_key(url, recipe), alien.clone());
    REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
    assert!(!register_cached_pixels_locked(
        &dir,
        url,
        ImageUse::DetailPoster,
        recipe,
        &alien
    ));
    assert!(REGISTRATION_WORK.with(|work| work.get().2) > 0);
    assert!(
        decoded_registration_snapshot(&image_recipe_cache_key(url, recipe))
            .unwrap()
            .1
            .is_none()
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn core06_symlink_retargeting_or_unsupported_identity_uses_full_validation() {
    let _serial = serial();
    decoded_cache_clear();
    let dir = scratch("core06-symlink");
    let url = "https://unit.test/core06-symlink";
    let (settings, recipe, old) = core06_populate(&dir, url);
    let path = recipe_disk_path(&dir, url, recipe);
    #[cfg(unix)]
    {
        let original = dir.join("original");
        fs::rename(&path, &original).unwrap();
        std::os::unix::fs::symlink(&original, &path).unwrap();
        let first = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
            panic!("same symlink target")
        })
        .unwrap();
        assert!(image_buffers_equal(&old, &first));
        let other = dir.join("other");
        let processed = process_source(source_image(12, 9, false), recipe).unwrap();
        let variant = DiskVariant {
            metadata: VariantMetadata {
                version: 2,
                url: url.into(),
                uses: vec![ImageUse::Fixed(None)],
                width: 12,
                height: 9,
                recipe,
                checksum: fnv1a(processed.encoded.as_ref().unwrap()),
            },
            encoded: processed.encoded.unwrap(),
        };
        write_variant(&other, &variant).unwrap();
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&other, &path).unwrap();
        let current = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
            panic!("replacement symlink target")
        })
        .unwrap();
        assert!(!image_buffers_equal(&old, &current));
    }
    #[cfg(not(unix))]
    {
        assert!(file_fingerprint(&path).is_none());
        assert!(
            decoded_registration_snapshot(&image_recipe_cache_key(url, recipe))
                .unwrap()
                .1
                .is_none()
        );
        REGISTRATION_WORK.with(|work| work.set((0, 0, 0)));
        let current = cached_pixels_with(&dir, url, ImageUse::Fixed(None), &settings, || {
            panic!("full validation")
        })
        .unwrap();
        assert!(image_buffers_equal(&old, &current));
        assert!(REGISTRATION_WORK.with(|work| work.get().1) > 0);
    }
    fs::remove_dir_all(dir).unwrap();
}
