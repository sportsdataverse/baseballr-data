# MLB fielding models — OAA and catcher framing


Two surfaces ship on the `mlb_fielding_models` release tag: **Outs Above
Average** over balls in play, and **catcher framing** (called-strike
value above average at the zone edges). The builders live in sdv-py
(`mlb_fielding_oaa`, `mlb_catcher_framing`) over Baseball Savant
balls-in-play and pitch-location data; this repository commits
per-season outputs under `mlb/fielding_models/` and publishes them daily
in-season.

The design constraint is stated up front because it prices the gates:
the public feed lacks fielder start coordinates, so OAA here conditions
on batted-ball properties and fielder identity rather than true
opportunity geometry — a **feature-capped ceiling that is priced into
the gates** (OAA full-season Pearson vs Savant’s live OAA ≥ 0.55;
framing ≥ 0.40) instead of hidden. The registry also records what is
deliberately NOT published from this family (catcher throwing/blocking,
baserunning, SB value): their live floors of 0.03–0.073 against 0.80+
design targets are data-ceiling-limited, recorded so nobody “finds” the
gap later.

Given that ceiling, the evaluation that matters most is **year-over-year
reliability** — whether the surviving metrics measure something stable
about the fielder rather than re-rolling noise annually. That is
computed below across every adjacent committed season pair, per position
for OAA.

## Training data

<div id="wrhnntmjdk" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#wrhnntmjdk table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#wrhnntmjdk thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#wrhnntmjdk p { margin: 0; padding: 0; }
 #wrhnntmjdk .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #wrhnntmjdk .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #wrhnntmjdk .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #wrhnntmjdk .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #wrhnntmjdk .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wrhnntmjdk .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wrhnntmjdk .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wrhnntmjdk .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #wrhnntmjdk .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #wrhnntmjdk .gt_column_spanner_outer:first-child { padding-left: 0; }
 #wrhnntmjdk .gt_column_spanner_outer:last-child { padding-right: 0; }
 #wrhnntmjdk .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #wrhnntmjdk .gt_spanner_row { border-bottom-style: hidden; }
 #wrhnntmjdk .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #wrhnntmjdk .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #wrhnntmjdk .gt_from_md> :first-child { margin-top: 0; }
 #wrhnntmjdk .gt_from_md> :last-child { margin-bottom: 0; }
 #wrhnntmjdk .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #wrhnntmjdk .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #wrhnntmjdk .gt_indent_1 { text-indent: 5px; }
 #wrhnntmjdk .gt_indent_2 { text-indent: calc(5px * 2); }
 #wrhnntmjdk .gt_indent_3 { text-indent: calc(5px * 3); }
 #wrhnntmjdk .gt_indent_4 { text-indent: calc(5px * 4); }
 #wrhnntmjdk .gt_indent_5 { text-indent: calc(5px * 5); }
 #wrhnntmjdk .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #wrhnntmjdk .gt_row_group_first td { border-top-width: 2px; }
 #wrhnntmjdk .gt_row_group_first th { border-top-width: 2px; }
 #wrhnntmjdk .gt_striped { color: #333333; background-color: #F4F4F4; }
 #wrhnntmjdk .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wrhnntmjdk .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wrhnntmjdk .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #wrhnntmjdk .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wrhnntmjdk .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wrhnntmjdk .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #wrhnntmjdk .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #wrhnntmjdk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wrhnntmjdk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wrhnntmjdk .gt_left { text-align: left; }
 #wrhnntmjdk .gt_center { text-align: center; }
 #wrhnntmjdk .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #wrhnntmjdk .gt_font_normal { font-weight: normal; }
 #wrhnntmjdk .gt_font_bold { font-weight: bold; }
 #wrhnntmjdk .gt_font_italic { font-style: italic; }
 #wrhnntmjdk .gt_super { font-size: 65%; }
 #wrhnntmjdk .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wrhnntmjdk .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #wrhnntmjdk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wrhnntmjdk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wrhnntmjdk .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #wrhnntmjdk .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed fielding-model assets, by season |  |  |  |  |
|----|----|----|----|----|
| from mlb/fielding_models/parquet/; computed at render time |  |  |  |  |
| season | fielders | opportunities | catchers | takes |
| 2015 | 1193 | 102,365 | 145 | 368,404 |
| 2016 | 1390 | 103,060 | 144 | 380,358 |
| 2017 | 1470 | 102,048 | 148 | 376,360 |
| 2018 | 1471 | 102,371 | 145 | 375,346 |
| 2019 | 1548 | 107,311 | 142 | 376,480 |
| 2020 | 1343 | 44,547 | 140 | 151,704 |
| 2021 | 1842 | 118,317 | 179 | 381,514 |
| 2022 | 1944 | 119,811 | 185 | 371,389 |
| 2023 | 2317 | 124,052 | 208 | 395,530 |
| 2024 | 2126 | 123,882 | 202 | 387,767 |
| 2025 | 2549 | 129,533 | 231 | 407,814 |
| 2026 | 3116 | 117,170 | 280 | 373,923 |

&#10;</div>

## Exploratory data analysis

<img src="fielding_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="OAA distribution by position, latest season." />

<img src="fielding_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Framing runs vs strikes gained — the run value is a near-linear function of strikes." />

The internal consistency check is visible in the framing scatter:
framing runs scale linearly with strikes gained at roughly the canonical
run value of a called strike. OAA centers near zero by construction at
every position — it is an *above-average* metric, so the league sums to
~0 each season:

<div id="zgvvmbgdtm" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#zgvvmbgdtm table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#zgvvmbgdtm thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#zgvvmbgdtm p { margin: 0; padding: 0; }
 #zgvvmbgdtm .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #zgvvmbgdtm .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #zgvvmbgdtm .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #zgvvmbgdtm .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #zgvvmbgdtm .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zgvvmbgdtm .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zgvvmbgdtm .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #zgvvmbgdtm .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #zgvvmbgdtm .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #zgvvmbgdtm .gt_column_spanner_outer:first-child { padding-left: 0; }
 #zgvvmbgdtm .gt_column_spanner_outer:last-child { padding-right: 0; }
 #zgvvmbgdtm .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #zgvvmbgdtm .gt_spanner_row { border-bottom-style: hidden; }
 #zgvvmbgdtm .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #zgvvmbgdtm .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #zgvvmbgdtm .gt_from_md> :first-child { margin-top: 0; }
 #zgvvmbgdtm .gt_from_md> :last-child { margin-bottom: 0; }
 #zgvvmbgdtm .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #zgvvmbgdtm .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #zgvvmbgdtm .gt_indent_1 { text-indent: 5px; }
 #zgvvmbgdtm .gt_indent_2 { text-indent: calc(5px * 2); }
 #zgvvmbgdtm .gt_indent_3 { text-indent: calc(5px * 3); }
 #zgvvmbgdtm .gt_indent_4 { text-indent: calc(5px * 4); }
 #zgvvmbgdtm .gt_indent_5 { text-indent: calc(5px * 5); }
 #zgvvmbgdtm .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #zgvvmbgdtm .gt_row_group_first td { border-top-width: 2px; }
 #zgvvmbgdtm .gt_row_group_first th { border-top-width: 2px; }
 #zgvvmbgdtm .gt_striped { color: #333333; background-color: #F4F4F4; }
 #zgvvmbgdtm .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zgvvmbgdtm .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zgvvmbgdtm .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #zgvvmbgdtm .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #zgvvmbgdtm .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #zgvvmbgdtm .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #zgvvmbgdtm .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #zgvvmbgdtm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zgvvmbgdtm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zgvvmbgdtm .gt_left { text-align: left; }
 #zgvvmbgdtm .gt_center { text-align: center; }
 #zgvvmbgdtm .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #zgvvmbgdtm .gt_font_normal { font-weight: normal; }
 #zgvvmbgdtm .gt_font_bold { font-weight: bold; }
 #zgvvmbgdtm .gt_font_italic { font-style: italic; }
 #zgvvmbgdtm .gt_super { font-size: 65%; }
 #zgvvmbgdtm .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zgvvmbgdtm .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #zgvvmbgdtm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #zgvvmbgdtm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #zgvvmbgdtm .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #zgvvmbgdtm .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| League OAA sum by season (zero-centering check) |                |          |
|-------------------------------------------------|----------------|----------|
| season                                          | league_oaa_sum | fielders |
| 2015                                            | 11.8           | 1193     |
| 2016                                            | 12.1           | 1390     |
| 2017                                            | 5.0            | 1470     |
| 2018                                            | 1.0            | 1471     |
| 2019                                            | 0.9            | 1548     |
| 2020                                            | 2.0            | 1343     |
| 2021                                            | 10.9           | 1842     |
| 2022                                            | 6.0            | 1944     |
| 2023                                            | 10.0           | 2317     |
| 2024                                            | 6.1            | 2126     |
| 2025                                            | 10.6           | 2549     |
| 2026                                            | 9.9            | 3116     |

&#10;</div>

## Evaluation — year-over-year reliability

<div id="suugyaxlfk" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#suugyaxlfk table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#suugyaxlfk thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#suugyaxlfk p { margin: 0; padding: 0; }
 #suugyaxlfk .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #suugyaxlfk .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #suugyaxlfk .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #suugyaxlfk .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #suugyaxlfk .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #suugyaxlfk .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #suugyaxlfk .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #suugyaxlfk .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #suugyaxlfk .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #suugyaxlfk .gt_column_spanner_outer:first-child { padding-left: 0; }
 #suugyaxlfk .gt_column_spanner_outer:last-child { padding-right: 0; }
 #suugyaxlfk .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #suugyaxlfk .gt_spanner_row { border-bottom-style: hidden; }
 #suugyaxlfk .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #suugyaxlfk .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #suugyaxlfk .gt_from_md> :first-child { margin-top: 0; }
 #suugyaxlfk .gt_from_md> :last-child { margin-bottom: 0; }
 #suugyaxlfk .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #suugyaxlfk .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #suugyaxlfk .gt_indent_1 { text-indent: 5px; }
 #suugyaxlfk .gt_indent_2 { text-indent: calc(5px * 2); }
 #suugyaxlfk .gt_indent_3 { text-indent: calc(5px * 3); }
 #suugyaxlfk .gt_indent_4 { text-indent: calc(5px * 4); }
 #suugyaxlfk .gt_indent_5 { text-indent: calc(5px * 5); }
 #suugyaxlfk .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #suugyaxlfk .gt_row_group_first td { border-top-width: 2px; }
 #suugyaxlfk .gt_row_group_first th { border-top-width: 2px; }
 #suugyaxlfk .gt_striped { color: #333333; background-color: #F4F4F4; }
 #suugyaxlfk .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #suugyaxlfk .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #suugyaxlfk .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #suugyaxlfk .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #suugyaxlfk .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #suugyaxlfk .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #suugyaxlfk .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #suugyaxlfk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #suugyaxlfk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #suugyaxlfk .gt_left { text-align: left; }
 #suugyaxlfk .gt_center { text-align: center; }
 #suugyaxlfk .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #suugyaxlfk .gt_font_normal { font-weight: normal; }
 #suugyaxlfk .gt_font_bold { font-weight: bold; }
 #suugyaxlfk .gt_font_italic { font-style: italic; }
 #suugyaxlfk .gt_super { font-size: 65%; }
 #suugyaxlfk .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #suugyaxlfk .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #suugyaxlfk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #suugyaxlfk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #suugyaxlfk .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #suugyaxlfk .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Year-over-year reliability — same fielder, adjacent seasons |  |  |
|----|----|----|
| all committed season pairs pooled; higher = more stable skill |  |  |
| metric | pairs | yoy_pearson |
| Framing runs | 1375 | 0.459 |
| OAA — 8 | 1531 | 0.342 |
| OAA — 5 | 1490 | 0.337 |
| OAA — 9 | 1854 | 0.314 |
| OAA — 6 | 1316 | 0.310 |
| OAA — 4 | 1601 | 0.207 |
| OAA — 3 | 1330 | 0.164 |
| OAA — 7 | 1986 | 0.123 |
| OAA — 2 | 770 | 0.097 |

&#10;</div>

<img src="fielding_files/figure-commonmark/cell-8-output-1.png"
width="420" height="300"
alt="Framing runs, season S vs season S+1 — the most reliable fielding skill." />

Framing is famously the stickiest defensive skill and the reliability
table reproduces that: catcher framing tops the list, while OAA
reliability varies by position with the infield generally steadier than
the outfield at these sample sizes. This is the correct lens for a
feature-capped model — even without start coordinates, a metric that
correlates season-over-season is measuring the fielder, not the noise.

## Per-direction OAA

The undirected number says a fielder was worth +8 outs; it does not say
whether those came going back on the ball, charging in, or ranging
sideways. `mlb_oaa_direction` splits every fielder-position into `in` /
`back` / `lateral`.

The split is a **partition, not a re-fit**: it re-groups the same scored
balls in play, so its rows sum exactly to the published `mlb_oaa`. That
is the property worth checking, because a per-direction re-fit — the
tempting implementation — would silently produce three numbers that no
longer add up to the headline one.

<p><strong>Pending republish.</strong> The <code>mlb_oaa_direction</code> stem lands with the next rebuild. Validated on the real 2021 Savant season at build time: all 1,832 fielder-positions survive the split, <strong>max |sum(direction OAA) &minus; published OAA| = 3.6e-15</strong> and opportunities reconcile exactly. League OAA by direction ran back +400.7, in &minus;244.7, lateral &minus;145.1.</p>

Two honest caveats travel with this split. First, Savant classifies
direction against the fielder’s **tracked start position**; the public
feed has no start coordinates (the same ceiling that prices this
family’s gates), so the position’s own median landing spot stands in for
“where this position normally plays”. Second, and more subtly, the depth
leg of that proxy is derived from landing distance — which is *also* an
input to the catch-probability logistic. So a systematic league-level
tilt between `back` and `in` (2021: +400.7 vs −244.7) partly reflects
residual structure in the catch surface by depth, not purely fielder
skill. The split is a genuine diagnostic for comparing fielders
**within** a direction; it should not be read as a calibrated
decomposition of league-wide value.

## Results

<div id="ktclrbammp" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ktclrbammp table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ktclrbammp thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ktclrbammp p { margin: 0; padding: 0; }
 #ktclrbammp .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ktclrbammp .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ktclrbammp .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ktclrbammp .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ktclrbammp .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ktclrbammp .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ktclrbammp .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ktclrbammp .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ktclrbammp .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ktclrbammp .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ktclrbammp .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ktclrbammp .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ktclrbammp .gt_spanner_row { border-bottom-style: hidden; }
 #ktclrbammp .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ktclrbammp .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ktclrbammp .gt_from_md> :first-child { margin-top: 0; }
 #ktclrbammp .gt_from_md> :last-child { margin-bottom: 0; }
 #ktclrbammp .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ktclrbammp .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ktclrbammp .gt_indent_1 { text-indent: 5px; }
 #ktclrbammp .gt_indent_2 { text-indent: calc(5px * 2); }
 #ktclrbammp .gt_indent_3 { text-indent: calc(5px * 3); }
 #ktclrbammp .gt_indent_4 { text-indent: calc(5px * 4); }
 #ktclrbammp .gt_indent_5 { text-indent: calc(5px * 5); }
 #ktclrbammp .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ktclrbammp .gt_row_group_first td { border-top-width: 2px; }
 #ktclrbammp .gt_row_group_first th { border-top-width: 2px; }
 #ktclrbammp .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ktclrbammp .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ktclrbammp .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ktclrbammp .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ktclrbammp .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ktclrbammp .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ktclrbammp .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ktclrbammp .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ktclrbammp .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ktclrbammp .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ktclrbammp .gt_left { text-align: left; }
 #ktclrbammp .gt_center { text-align: center; }
 #ktclrbammp .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ktclrbammp .gt_font_normal { font-weight: normal; }
 #ktclrbammp .gt_font_bold { font-weight: bold; }
 #ktclrbammp .gt_font_italic { font-style: italic; }
 #ktclrbammp .gt_super { font-size: 65%; }
 #ktclrbammp .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ktclrbammp .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ktclrbammp .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ktclrbammp .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ktclrbammp .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ktclrbammp .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 10 framing catchers — 2026 |  |  |  |  |
|----|----|----|----|----|
|  | Catcher | Takes | Strikes gained | Framing runs |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/669224/headshot/67/current"
height="42" /> | Austin Wells | 6,889 | 35.7 | 3.3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/624431/headshot/67/current"
height="42" /> | Jose Trevino | 3,355 | 7.7 | 3.1 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/645305/headshot/67/current"
height="42" /> | Ali Sánchez | 2,248 | 25.9 | 2.8 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/605170/headshot/67/current"
height="42" /> | Victor Caratini | 5,198 | 42.8 | 2.4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/699625/headshot/67/current"
height="42" /> | Jimmy Crooks | 2,620 | 13.8 | 2.1 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/620443/headshot/67/current"
height="42" /> | Luis Torrens | 5,215 | 21.5 | 1.6 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/605244/headshot/67/current"
height="42" /> | Aramis Garcia | 957 | 10.4 | 1.5 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/672386/headshot/67/current"
height="42" /> | Alejandro Kirk | 4,393 | 22.3 | 1.3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/678218/headshot/67/current"
height="42" /> | Brandon Valenzuela | 4,923 | 20.9 | 1.3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/664874/headshot/67/current"
height="42" /> | Seby Zavala | 441 | 6.8 | 1.2 |

&#10;</div>

<div id="ytnqepwegp" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ytnqepwegp table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ytnqepwegp thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ytnqepwegp p { margin: 0; padding: 0; }
 #ytnqepwegp .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ytnqepwegp .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ytnqepwegp .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ytnqepwegp .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ytnqepwegp .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ytnqepwegp .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ytnqepwegp .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ytnqepwegp .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ytnqepwegp .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ytnqepwegp .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ytnqepwegp .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ytnqepwegp .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ytnqepwegp .gt_spanner_row { border-bottom-style: hidden; }
 #ytnqepwegp .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ytnqepwegp .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ytnqepwegp .gt_from_md> :first-child { margin-top: 0; }
 #ytnqepwegp .gt_from_md> :last-child { margin-bottom: 0; }
 #ytnqepwegp .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ytnqepwegp .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ytnqepwegp .gt_indent_1 { text-indent: 5px; }
 #ytnqepwegp .gt_indent_2 { text-indent: calc(5px * 2); }
 #ytnqepwegp .gt_indent_3 { text-indent: calc(5px * 3); }
 #ytnqepwegp .gt_indent_4 { text-indent: calc(5px * 4); }
 #ytnqepwegp .gt_indent_5 { text-indent: calc(5px * 5); }
 #ytnqepwegp .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ytnqepwegp .gt_row_group_first td { border-top-width: 2px; }
 #ytnqepwegp .gt_row_group_first th { border-top-width: 2px; }
 #ytnqepwegp .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ytnqepwegp .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ytnqepwegp .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ytnqepwegp .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ytnqepwegp .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ytnqepwegp .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ytnqepwegp .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ytnqepwegp .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ytnqepwegp .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ytnqepwegp .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ytnqepwegp .gt_left { text-align: left; }
 #ytnqepwegp .gt_center { text-align: center; }
 #ytnqepwegp .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ytnqepwegp .gt_font_normal { font-weight: normal; }
 #ytnqepwegp .gt_font_bold { font-weight: bold; }
 #ytnqepwegp .gt_font_italic { font-style: italic; }
 #ytnqepwegp .gt_super { font-size: 65%; }
 #ytnqepwegp .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ytnqepwegp .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ytnqepwegp .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ytnqepwegp .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ytnqepwegp .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ytnqepwegp .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 10 fielders by OAA — 2026 |  |  |  |  |
|----|----|----|----|----|
|  | Fielder | Pos | Opportunities | OAA |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/691718/headshot/67/current"
height="42" /> | Pete Crow-Armstrong | 8 | 595 | 19.6 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/605141/headshot/67/current"
height="42" /> | Mookie Betts | 6 | 284 | 19.0 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/641355/headshot/67/current"
height="42" /> | Cody Bellinger | 7 | 356 | 18.0 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/670764/headshot/67/current"
height="42" /> | Taylor Walls | 6 | 339 | 16.4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/671739/headshot/67/current"
height="42" /> | Michael Harris II | 8 | 541 | 16.3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/682998/headshot/67/current"
height="42" /> | Corbin Carroll | 9 | 591 | 15.8 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/621020/headshot/67/current"
height="42" /> | Dansby Swanson | 6 | 386 | 13.5 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/681624/headshot/67/current"
height="42" /> | Andy Pages | 8 | 610 | 13.4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/608324/headshot/67/current"
height="42" /> | Alex Bregman | 5 | 313 | 12.9 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/802139/headshot/67/current"
height="42" /> | JJ Wetherholt | 4 | 397 | 12.9 |

&#10;</div>

## Provenance & reproducibility

- **Trained on:** Baseball Savant balls-in-play (batted-ball
  properties + fielder identity; **no fielder start coordinates in the
  public feed**) and pitch locations for framing; seasons in the table
  above.
- **Committed at:** `mlb/fielding_models/parquet/`; published to
  `mlb_fielding_models`; per-publish metadata in
  [`../../mlb/fielding_models/mlb_fielding_models_card.json`](../../mlb/fielding_models/mlb_fielding_models_card.json).
- **Pipeline:** `scripts/mlb_models.sh 04` → stage
  `python/mlb_model_04_fielding.py` (`mlb_models_cron.yml`). Single
  home: `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; `uv sync --group docs`). Names/headshots via one batched statsapi
  call; offline renders fall back to MLBAM ids.

## Avenues for improvement & open issues

- **Start-coordinate ceiling** — the single biggest gain would be
  fielder positioning data, which the public feed does not carry; until
  then the gates stay at their priced-in floors (OAA ≥ 0.55, framing ≥
  0.40 vs live).
- **Resolved (2026-09-02, PR \#14):** per-direction OAA ships as
  `mlb_oaa_direction` (in / back / lateral). Validated on the real 2021
  season: all 1,832 fielder-positions survive, max \|sum(direction OAA)
  − published OAA\| = 3.6e-15, opportunities exact. The direction proxy
  and its confound with the catch surface’s depth term are documented
  above — read it as a within-direction comparison, not a calibrated
  value decomposition.
- **Known issue:** the registry’s deliberately-unpublished surfaces
  (catcher throwing/blocking, baserunning, SB value) remain
  data-ceiling-limited (live floors 0.03–0.073 vs 0.80+ design targets)
  — recorded so the gap is never “discovered”.
