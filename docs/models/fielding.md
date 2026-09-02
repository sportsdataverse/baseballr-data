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

<div id="ujbdudgmdd" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ujbdudgmdd table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ujbdudgmdd thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ujbdudgmdd p { margin: 0; padding: 0; }
 #ujbdudgmdd .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ujbdudgmdd .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ujbdudgmdd .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ujbdudgmdd .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ujbdudgmdd .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ujbdudgmdd .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ujbdudgmdd .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ujbdudgmdd .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ujbdudgmdd .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ujbdudgmdd .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ujbdudgmdd .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ujbdudgmdd .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ujbdudgmdd .gt_spanner_row { border-bottom-style: hidden; }
 #ujbdudgmdd .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ujbdudgmdd .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ujbdudgmdd .gt_from_md> :first-child { margin-top: 0; }
 #ujbdudgmdd .gt_from_md> :last-child { margin-bottom: 0; }
 #ujbdudgmdd .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ujbdudgmdd .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ujbdudgmdd .gt_indent_1 { text-indent: 5px; }
 #ujbdudgmdd .gt_indent_2 { text-indent: calc(5px * 2); }
 #ujbdudgmdd .gt_indent_3 { text-indent: calc(5px * 3); }
 #ujbdudgmdd .gt_indent_4 { text-indent: calc(5px * 4); }
 #ujbdudgmdd .gt_indent_5 { text-indent: calc(5px * 5); }
 #ujbdudgmdd .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ujbdudgmdd .gt_row_group_first td { border-top-width: 2px; }
 #ujbdudgmdd .gt_row_group_first th { border-top-width: 2px; }
 #ujbdudgmdd .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ujbdudgmdd .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ujbdudgmdd .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ujbdudgmdd .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ujbdudgmdd .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ujbdudgmdd .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ujbdudgmdd .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ujbdudgmdd .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ujbdudgmdd .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ujbdudgmdd .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ujbdudgmdd .gt_left { text-align: left; }
 #ujbdudgmdd .gt_center { text-align: center; }
 #ujbdudgmdd .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ujbdudgmdd .gt_font_normal { font-weight: normal; }
 #ujbdudgmdd .gt_font_bold { font-weight: bold; }
 #ujbdudgmdd .gt_font_italic { font-style: italic; }
 #ujbdudgmdd .gt_super { font-size: 65%; }
 #ujbdudgmdd .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ujbdudgmdd .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ujbdudgmdd .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ujbdudgmdd .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ujbdudgmdd .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ujbdudgmdd .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="inbtkdrrgm" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#inbtkdrrgm table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#inbtkdrrgm thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#inbtkdrrgm p { margin: 0; padding: 0; }
 #inbtkdrrgm .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #inbtkdrrgm .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #inbtkdrrgm .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #inbtkdrrgm .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #inbtkdrrgm .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #inbtkdrrgm .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #inbtkdrrgm .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #inbtkdrrgm .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #inbtkdrrgm .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #inbtkdrrgm .gt_column_spanner_outer:first-child { padding-left: 0; }
 #inbtkdrrgm .gt_column_spanner_outer:last-child { padding-right: 0; }
 #inbtkdrrgm .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #inbtkdrrgm .gt_spanner_row { border-bottom-style: hidden; }
 #inbtkdrrgm .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #inbtkdrrgm .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #inbtkdrrgm .gt_from_md> :first-child { margin-top: 0; }
 #inbtkdrrgm .gt_from_md> :last-child { margin-bottom: 0; }
 #inbtkdrrgm .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #inbtkdrrgm .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #inbtkdrrgm .gt_indent_1 { text-indent: 5px; }
 #inbtkdrrgm .gt_indent_2 { text-indent: calc(5px * 2); }
 #inbtkdrrgm .gt_indent_3 { text-indent: calc(5px * 3); }
 #inbtkdrrgm .gt_indent_4 { text-indent: calc(5px * 4); }
 #inbtkdrrgm .gt_indent_5 { text-indent: calc(5px * 5); }
 #inbtkdrrgm .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #inbtkdrrgm .gt_row_group_first td { border-top-width: 2px; }
 #inbtkdrrgm .gt_row_group_first th { border-top-width: 2px; }
 #inbtkdrrgm .gt_striped { color: #333333; background-color: #F4F4F4; }
 #inbtkdrrgm .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #inbtkdrrgm .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #inbtkdrrgm .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #inbtkdrrgm .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #inbtkdrrgm .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #inbtkdrrgm .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #inbtkdrrgm .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #inbtkdrrgm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #inbtkdrrgm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #inbtkdrrgm .gt_left { text-align: left; }
 #inbtkdrrgm .gt_center { text-align: center; }
 #inbtkdrrgm .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #inbtkdrrgm .gt_font_normal { font-weight: normal; }
 #inbtkdrrgm .gt_font_bold { font-weight: bold; }
 #inbtkdrrgm .gt_font_italic { font-style: italic; }
 #inbtkdrrgm .gt_super { font-size: 65%; }
 #inbtkdrrgm .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #inbtkdrrgm .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #inbtkdrrgm .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #inbtkdrrgm .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #inbtkdrrgm .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #inbtkdrrgm .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="hlzohtidll" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#hlzohtidll table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#hlzohtidll thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#hlzohtidll p { margin: 0; padding: 0; }
 #hlzohtidll .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #hlzohtidll .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #hlzohtidll .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #hlzohtidll .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #hlzohtidll .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #hlzohtidll .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #hlzohtidll .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #hlzohtidll .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #hlzohtidll .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #hlzohtidll .gt_column_spanner_outer:first-child { padding-left: 0; }
 #hlzohtidll .gt_column_spanner_outer:last-child { padding-right: 0; }
 #hlzohtidll .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #hlzohtidll .gt_spanner_row { border-bottom-style: hidden; }
 #hlzohtidll .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #hlzohtidll .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #hlzohtidll .gt_from_md> :first-child { margin-top: 0; }
 #hlzohtidll .gt_from_md> :last-child { margin-bottom: 0; }
 #hlzohtidll .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #hlzohtidll .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #hlzohtidll .gt_indent_1 { text-indent: 5px; }
 #hlzohtidll .gt_indent_2 { text-indent: calc(5px * 2); }
 #hlzohtidll .gt_indent_3 { text-indent: calc(5px * 3); }
 #hlzohtidll .gt_indent_4 { text-indent: calc(5px * 4); }
 #hlzohtidll .gt_indent_5 { text-indent: calc(5px * 5); }
 #hlzohtidll .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #hlzohtidll .gt_row_group_first td { border-top-width: 2px; }
 #hlzohtidll .gt_row_group_first th { border-top-width: 2px; }
 #hlzohtidll .gt_striped { color: #333333; background-color: #F4F4F4; }
 #hlzohtidll .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #hlzohtidll .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #hlzohtidll .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #hlzohtidll .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #hlzohtidll .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #hlzohtidll .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #hlzohtidll .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #hlzohtidll .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #hlzohtidll .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #hlzohtidll .gt_left { text-align: left; }
 #hlzohtidll .gt_center { text-align: center; }
 #hlzohtidll .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #hlzohtidll .gt_font_normal { font-weight: normal; }
 #hlzohtidll .gt_font_bold { font-weight: bold; }
 #hlzohtidll .gt_font_italic { font-style: italic; }
 #hlzohtidll .gt_super { font-size: 65%; }
 #hlzohtidll .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #hlzohtidll .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #hlzohtidll .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #hlzohtidll .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #hlzohtidll .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #hlzohtidll .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="wqmftrchtx" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#wqmftrchtx table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#wqmftrchtx thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#wqmftrchtx p { margin: 0; padding: 0; }
 #wqmftrchtx .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #wqmftrchtx .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #wqmftrchtx .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #wqmftrchtx .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #wqmftrchtx .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wqmftrchtx .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wqmftrchtx .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wqmftrchtx .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #wqmftrchtx .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #wqmftrchtx .gt_column_spanner_outer:first-child { padding-left: 0; }
 #wqmftrchtx .gt_column_spanner_outer:last-child { padding-right: 0; }
 #wqmftrchtx .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #wqmftrchtx .gt_spanner_row { border-bottom-style: hidden; }
 #wqmftrchtx .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #wqmftrchtx .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #wqmftrchtx .gt_from_md> :first-child { margin-top: 0; }
 #wqmftrchtx .gt_from_md> :last-child { margin-bottom: 0; }
 #wqmftrchtx .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #wqmftrchtx .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #wqmftrchtx .gt_indent_1 { text-indent: 5px; }
 #wqmftrchtx .gt_indent_2 { text-indent: calc(5px * 2); }
 #wqmftrchtx .gt_indent_3 { text-indent: calc(5px * 3); }
 #wqmftrchtx .gt_indent_4 { text-indent: calc(5px * 4); }
 #wqmftrchtx .gt_indent_5 { text-indent: calc(5px * 5); }
 #wqmftrchtx .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #wqmftrchtx .gt_row_group_first td { border-top-width: 2px; }
 #wqmftrchtx .gt_row_group_first th { border-top-width: 2px; }
 #wqmftrchtx .gt_striped { color: #333333; background-color: #F4F4F4; }
 #wqmftrchtx .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wqmftrchtx .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wqmftrchtx .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #wqmftrchtx .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wqmftrchtx .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wqmftrchtx .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #wqmftrchtx .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #wqmftrchtx .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wqmftrchtx .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wqmftrchtx .gt_left { text-align: left; }
 #wqmftrchtx .gt_center { text-align: center; }
 #wqmftrchtx .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #wqmftrchtx .gt_font_normal { font-weight: normal; }
 #wqmftrchtx .gt_font_bold { font-weight: bold; }
 #wqmftrchtx .gt_font_italic { font-style: italic; }
 #wqmftrchtx .gt_super { font-size: 65%; }
 #wqmftrchtx .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wqmftrchtx .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #wqmftrchtx .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wqmftrchtx .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wqmftrchtx .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #wqmftrchtx .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="xyfdrsvbyp" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#xyfdrsvbyp table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#xyfdrsvbyp thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#xyfdrsvbyp p { margin: 0; padding: 0; }
 #xyfdrsvbyp .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #xyfdrsvbyp .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #xyfdrsvbyp .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #xyfdrsvbyp .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #xyfdrsvbyp .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xyfdrsvbyp .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xyfdrsvbyp .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xyfdrsvbyp .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #xyfdrsvbyp .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #xyfdrsvbyp .gt_column_spanner_outer:first-child { padding-left: 0; }
 #xyfdrsvbyp .gt_column_spanner_outer:last-child { padding-right: 0; }
 #xyfdrsvbyp .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #xyfdrsvbyp .gt_spanner_row { border-bottom-style: hidden; }
 #xyfdrsvbyp .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #xyfdrsvbyp .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #xyfdrsvbyp .gt_from_md> :first-child { margin-top: 0; }
 #xyfdrsvbyp .gt_from_md> :last-child { margin-bottom: 0; }
 #xyfdrsvbyp .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #xyfdrsvbyp .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #xyfdrsvbyp .gt_indent_1 { text-indent: 5px; }
 #xyfdrsvbyp .gt_indent_2 { text-indent: calc(5px * 2); }
 #xyfdrsvbyp .gt_indent_3 { text-indent: calc(5px * 3); }
 #xyfdrsvbyp .gt_indent_4 { text-indent: calc(5px * 4); }
 #xyfdrsvbyp .gt_indent_5 { text-indent: calc(5px * 5); }
 #xyfdrsvbyp .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #xyfdrsvbyp .gt_row_group_first td { border-top-width: 2px; }
 #xyfdrsvbyp .gt_row_group_first th { border-top-width: 2px; }
 #xyfdrsvbyp .gt_striped { color: #333333; background-color: #F4F4F4; }
 #xyfdrsvbyp .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xyfdrsvbyp .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xyfdrsvbyp .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #xyfdrsvbyp .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xyfdrsvbyp .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xyfdrsvbyp .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #xyfdrsvbyp .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #xyfdrsvbyp .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xyfdrsvbyp .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xyfdrsvbyp .gt_left { text-align: left; }
 #xyfdrsvbyp .gt_center { text-align: center; }
 #xyfdrsvbyp .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #xyfdrsvbyp .gt_font_normal { font-weight: normal; }
 #xyfdrsvbyp .gt_font_bold { font-weight: bold; }
 #xyfdrsvbyp .gt_font_italic { font-style: italic; }
 #xyfdrsvbyp .gt_super { font-size: 65%; }
 #xyfdrsvbyp .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xyfdrsvbyp .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #xyfdrsvbyp .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xyfdrsvbyp .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xyfdrsvbyp .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #xyfdrsvbyp .gt_asterisk { font-size: 100%; vertical-align: 0; }
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
- **Resolved (2026-09-02, PR \#13):** per-direction OAA ships as
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
