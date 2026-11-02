# JPEG XL fixture provenance

Newly authored 8×6 geometric samples, generated on 2026-11-01 using ffmpeg's
libjxl 0.11 encoder with `-distance 0 -effort 3`. No website images are included.
RGB: `(x*30, y*40, (x+y)*16)`. RGBA: `(40,120,200,(x+y)*19)`.
These fixtures are provided under the project's GPL-3.0-or-later license.
The application decoder is jxl-oxide 0.12.6 (MIT OR Apache-2.0), with Rayon off;
libjxl is used only to generate these fixtures and is not an app dependency.

`progressive-rgb.jxl` is a newly authored 512×384 RGB VarDCT sample generated
on 2026-11-02. The P6 PPM input has these channel values, with zero-based `x,y`:

- `R = (x*7 + (y//16 % 2)*70) % 256`
- `G = (y*11 + (x//16 % 2)*50) % 256`
- `B = ((x ^ y)*5) % 256`

Generated with FFmpeg 7.1.5 and libjxl 0.11.2:

```sh
ffmpeg -hide_banner -loglevel error -y -i authored-progressive.ppm \
  -frames:v 1 -c:v libjxl -effort 7 -distance 1.0 -threads 1 progressive-rgb.jxl
```

The 87,236-byte raw codestream has SHA-256
`47c8286d21e7a8d8f0819788edfbe35c19e7994296ef1ada898ac6c33487516d`.
The incremental tests render useful pixels before its final byte arrives, then
require final pixels identical to the existing strict decoder. Container variants
and truncated tails are built from this and the tiny fixtures in the tests.
No real gallery images or network-derived artwork are included. The same
GPL-3.0-or-later fixture license applies.
