# JPEG XL fixture provenance

Newly authored 8×6 geometric samples, generated on 2026-11-01 using ffmpeg's
libjxl 0.11 encoder with `-distance 0 -effort 3`. No website images are included.
RGB: `(x*30, y*40, (x+y)*16)`. RGBA: `(40,120,200,(x+y)*19)`.
These fixtures are provided under the project's GPL-3.0-or-later license.
The application decoder is jxl-oxide 0.12.6 (MIT OR Apache-2.0), with Rayon off;
libjxl is used only to generate these fixtures and is not an app dependency.
