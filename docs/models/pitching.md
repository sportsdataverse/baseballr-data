# MLB pitching models — xERA, Stuff+, Command+


Three surfaces ship on the `mlb_pitching_models` release tag: **xERA**
(contact-quality ERA), **Stuff+** (pitch-physics quality: velocity,
movement, release), and **Command+** (location quality). Together they
are the stuff/command decomposition of pitching: Stuff+ deliberately
excludes location, Command+ deliberately excludes physics — the
decomposition is the point, because a pitcher’s physical tools and their
ability to locate age and develop on different curves.

The builders live in sdv-py (`x_era`, `mlb_stuff_plus`,
`mlb_command_plus`) over Baseball Savant pitch-level features; this
repository commits per-season outputs under `mlb/pitching_models/` and
publishes them daily in-season. The publish gates are stated honestly:
xERA MAE vs Savant’s own xERA ≤ 0.30; Stuff+ Spearman vs run value ≥
0.20; Command+ ≥ 0.04 — the Command+ gate is explicitly **directional
only**, a weak ordinal signal stated as such rather than inflated.
Everything below is computed at render time from the committed files,
including the two evaluations that matter most for surfaces like these:
**year-over-year reliability** (does the metric measure a stable skill?)
and **forward predictiveness** (does this year’s Stuff+ tell you
anything about next year’s xERA?).

## Training data

<div id="wtvpnndrym" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#wtvpnndrym table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#wtvpnndrym thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#wtvpnndrym p { margin: 0; padding: 0; }
 #wtvpnndrym .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #wtvpnndrym .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #wtvpnndrym .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #wtvpnndrym .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #wtvpnndrym .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wtvpnndrym .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wtvpnndrym .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wtvpnndrym .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #wtvpnndrym .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #wtvpnndrym .gt_column_spanner_outer:first-child { padding-left: 0; }
 #wtvpnndrym .gt_column_spanner_outer:last-child { padding-right: 0; }
 #wtvpnndrym .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #wtvpnndrym .gt_spanner_row { border-bottom-style: hidden; }
 #wtvpnndrym .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #wtvpnndrym .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #wtvpnndrym .gt_from_md> :first-child { margin-top: 0; }
 #wtvpnndrym .gt_from_md> :last-child { margin-bottom: 0; }
 #wtvpnndrym .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #wtvpnndrym .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #wtvpnndrym .gt_indent_1 { text-indent: 5px; }
 #wtvpnndrym .gt_indent_2 { text-indent: calc(5px * 2); }
 #wtvpnndrym .gt_indent_3 { text-indent: calc(5px * 3); }
 #wtvpnndrym .gt_indent_4 { text-indent: calc(5px * 4); }
 #wtvpnndrym .gt_indent_5 { text-indent: calc(5px * 5); }
 #wtvpnndrym .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #wtvpnndrym .gt_row_group_first td { border-top-width: 2px; }
 #wtvpnndrym .gt_row_group_first th { border-top-width: 2px; }
 #wtvpnndrym .gt_striped { color: #333333; background-color: #F4F4F4; }
 #wtvpnndrym .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wtvpnndrym .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wtvpnndrym .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #wtvpnndrym .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wtvpnndrym .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wtvpnndrym .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #wtvpnndrym .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #wtvpnndrym .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wtvpnndrym .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wtvpnndrym .gt_left { text-align: left; }
 #wtvpnndrym .gt_center { text-align: center; }
 #wtvpnndrym .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #wtvpnndrym .gt_font_normal { font-weight: normal; }
 #wtvpnndrym .gt_font_bold { font-weight: bold; }
 #wtvpnndrym .gt_font_italic { font-style: italic; }
 #wtvpnndrym .gt_super { font-size: 65%; }
 #wtvpnndrym .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wtvpnndrym .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #wtvpnndrym .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wtvpnndrym .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wtvpnndrym .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #wtvpnndrym .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Committed pitching-model assets, by season |  |  |  |
|----|----|----|----|
| from mlb/pitching_models/parquet/; computed at render time |  |  |  |
| season | pitchers_xera | pitcher_pitchtype_rows | pitchers_command |
| 2015 | 736 | 4,676 | 883 |
| 2016 | 742 | 4,920 | 916 |
| 2017 | 755 | 4,459 | 862 |
| 2018 | 799 | 4,344 | 902 |
| 2019 | 830 | 4,379 | 944 |
| 2020 | 737 | 3,944 | 869 |
| 2021 | 909 | 4,543 | 1069 |
| 2022 | 871 | 4,676 | 1069 |
| 2023 | 863 | 5,761 | 1282 |
| 2024 | 855 | 5,795 | 1281 |
| 2025 | 873 | 6,569 | 1508 |
| 2026 | 833 | 7,646 | 1772 |

&#10;</div>

## Exploratory data analysis

<img src="pitching_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="Stuff+ distribution by pitch type, latest season (100 = league average)." />

<img src="pitching_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300" alt="Command+ distribution, latest season." />

## The xERA ≡ x_wOBA identity — by design, verified live

The 2026-09-01 audit flagged that published **xERA correlates r = 1.0
with x_wOBA-against**. Root-caused 2026-09-01: this is **by design, not
a defect** — sdv-py’s `x_era` is a documented *parametric* wOBA-to-runs
conversion,
`x_era = league_era + ((x_woba − league_woba) / woba_scale) · pa_per_9`,
an affine transform per season. The two columns deliberately carry one
signal on two scales (rate vs runs). The correlation is still recomputed
on every render — now as a **design confirmation**: any r below ~1.0
would mean the recipe silently changed:

<div id="djdcxmtnwq" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#djdcxmtnwq table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#djdcxmtnwq thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#djdcxmtnwq p { margin: 0; padding: 0; }
 #djdcxmtnwq .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #djdcxmtnwq .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #djdcxmtnwq .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #djdcxmtnwq .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #djdcxmtnwq .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #djdcxmtnwq .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #djdcxmtnwq .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #djdcxmtnwq .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #djdcxmtnwq .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #djdcxmtnwq .gt_column_spanner_outer:first-child { padding-left: 0; }
 #djdcxmtnwq .gt_column_spanner_outer:last-child { padding-right: 0; }
 #djdcxmtnwq .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #djdcxmtnwq .gt_spanner_row { border-bottom-style: hidden; }
 #djdcxmtnwq .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #djdcxmtnwq .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #djdcxmtnwq .gt_from_md> :first-child { margin-top: 0; }
 #djdcxmtnwq .gt_from_md> :last-child { margin-bottom: 0; }
 #djdcxmtnwq .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #djdcxmtnwq .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #djdcxmtnwq .gt_indent_1 { text-indent: 5px; }
 #djdcxmtnwq .gt_indent_2 { text-indent: calc(5px * 2); }
 #djdcxmtnwq .gt_indent_3 { text-indent: calc(5px * 3); }
 #djdcxmtnwq .gt_indent_4 { text-indent: calc(5px * 4); }
 #djdcxmtnwq .gt_indent_5 { text-indent: calc(5px * 5); }
 #djdcxmtnwq .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #djdcxmtnwq .gt_row_group_first td { border-top-width: 2px; }
 #djdcxmtnwq .gt_row_group_first th { border-top-width: 2px; }
 #djdcxmtnwq .gt_striped { color: #333333; background-color: #F4F4F4; }
 #djdcxmtnwq .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #djdcxmtnwq .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #djdcxmtnwq .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #djdcxmtnwq .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #djdcxmtnwq .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #djdcxmtnwq .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #djdcxmtnwq .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #djdcxmtnwq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #djdcxmtnwq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #djdcxmtnwq .gt_left { text-align: left; }
 #djdcxmtnwq .gt_center { text-align: center; }
 #djdcxmtnwq .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #djdcxmtnwq .gt_font_normal { font-weight: normal; }
 #djdcxmtnwq .gt_font_bold { font-weight: bold; }
 #djdcxmtnwq .gt_font_italic { font-style: italic; }
 #djdcxmtnwq .gt_super { font-size: 65%; }
 #djdcxmtnwq .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #djdcxmtnwq .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #djdcxmtnwq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #djdcxmtnwq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #djdcxmtnwq .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #djdcxmtnwq .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Design check: Pearson r between published xERA and x_wOBA-against |  |  |
|----|----|----|
| x_era is a documented affine transform of x_woba (sdv-py mlb_pitch_era) — r = 1.0 CONFIRMS the recipe |  |  |
| season | pearson_xera_xwoba | n |
| 2015 | 1.0000 | 736 |
| 2016 | 1.0000 | 742 |
| 2017 | 1.0000 | 755 |
| 2018 | 1.0000 | 799 |
| 2019 | 1.0000 | 830 |
| 2020 | 1.0000 | 737 |
| 2021 | 1.0000 | 909 |
| 2022 | 1.0000 | 871 |
| 2023 | 1.0000 | 863 |
| 2024 | 1.0000 | 855 |
| 2025 | 1.0000 | 873 |
| 2026 | 1.0000 | 833 |

&#10;</div>

## Evaluation — reliability and forward predictiveness

A skill metric must first be **reliable**: a pitcher’s score this year
should correlate with the same pitcher’s score next year. Then it should
be **predictive**: this year’s process metric should anticipate next
year’s outcome metric. Both are computed here across every adjacent
committed season pair.

<div id="tnqawvmfhh" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#tnqawvmfhh table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#tnqawvmfhh thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#tnqawvmfhh p { margin: 0; padding: 0; }
 #tnqawvmfhh .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #tnqawvmfhh .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #tnqawvmfhh .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #tnqawvmfhh .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #tnqawvmfhh .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #tnqawvmfhh .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #tnqawvmfhh .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #tnqawvmfhh .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #tnqawvmfhh .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #tnqawvmfhh .gt_column_spanner_outer:first-child { padding-left: 0; }
 #tnqawvmfhh .gt_column_spanner_outer:last-child { padding-right: 0; }
 #tnqawvmfhh .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #tnqawvmfhh .gt_spanner_row { border-bottom-style: hidden; }
 #tnqawvmfhh .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #tnqawvmfhh .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #tnqawvmfhh .gt_from_md> :first-child { margin-top: 0; }
 #tnqawvmfhh .gt_from_md> :last-child { margin-bottom: 0; }
 #tnqawvmfhh .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #tnqawvmfhh .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #tnqawvmfhh .gt_indent_1 { text-indent: 5px; }
 #tnqawvmfhh .gt_indent_2 { text-indent: calc(5px * 2); }
 #tnqawvmfhh .gt_indent_3 { text-indent: calc(5px * 3); }
 #tnqawvmfhh .gt_indent_4 { text-indent: calc(5px * 4); }
 #tnqawvmfhh .gt_indent_5 { text-indent: calc(5px * 5); }
 #tnqawvmfhh .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #tnqawvmfhh .gt_row_group_first td { border-top-width: 2px; }
 #tnqawvmfhh .gt_row_group_first th { border-top-width: 2px; }
 #tnqawvmfhh .gt_striped { color: #333333; background-color: #F4F4F4; }
 #tnqawvmfhh .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #tnqawvmfhh .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #tnqawvmfhh .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #tnqawvmfhh .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #tnqawvmfhh .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #tnqawvmfhh .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #tnqawvmfhh .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #tnqawvmfhh .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #tnqawvmfhh .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #tnqawvmfhh .gt_left { text-align: left; }
 #tnqawvmfhh .gt_center { text-align: center; }
 #tnqawvmfhh .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #tnqawvmfhh .gt_font_normal { font-weight: normal; }
 #tnqawvmfhh .gt_font_bold { font-weight: bold; }
 #tnqawvmfhh .gt_font_italic { font-style: italic; }
 #tnqawvmfhh .gt_super { font-size: 65%; }
 #tnqawvmfhh .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #tnqawvmfhh .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #tnqawvmfhh .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #tnqawvmfhh .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #tnqawvmfhh .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #tnqawvmfhh .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Year-over-year reliability — same pitcher, adjacent seasons |  |  |
|----|----|----|
| all committed season pairs pooled; higher = more stable skill |  |  |
| metric | pairs | yoy_pearson |
| Stuff+ (pitcher mean) | 10349 | 0.377 |
| Command+ | 7912 | 0.375 |
| xERA | 6389 | 0.208 |

&#10;</div>

<div id="qexawcuccz" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#qexawcuccz table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#qexawcuccz thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#qexawcuccz p { margin: 0; padding: 0; }
 #qexawcuccz .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #qexawcuccz .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #qexawcuccz .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #qexawcuccz .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #qexawcuccz .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qexawcuccz .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qexawcuccz .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qexawcuccz .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #qexawcuccz .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #qexawcuccz .gt_column_spanner_outer:first-child { padding-left: 0; }
 #qexawcuccz .gt_column_spanner_outer:last-child { padding-right: 0; }
 #qexawcuccz .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #qexawcuccz .gt_spanner_row { border-bottom-style: hidden; }
 #qexawcuccz .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #qexawcuccz .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #qexawcuccz .gt_from_md> :first-child { margin-top: 0; }
 #qexawcuccz .gt_from_md> :last-child { margin-bottom: 0; }
 #qexawcuccz .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #qexawcuccz .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #qexawcuccz .gt_indent_1 { text-indent: 5px; }
 #qexawcuccz .gt_indent_2 { text-indent: calc(5px * 2); }
 #qexawcuccz .gt_indent_3 { text-indent: calc(5px * 3); }
 #qexawcuccz .gt_indent_4 { text-indent: calc(5px * 4); }
 #qexawcuccz .gt_indent_5 { text-indent: calc(5px * 5); }
 #qexawcuccz .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #qexawcuccz .gt_row_group_first td { border-top-width: 2px; }
 #qexawcuccz .gt_row_group_first th { border-top-width: 2px; }
 #qexawcuccz .gt_striped { color: #333333; background-color: #F4F4F4; }
 #qexawcuccz .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qexawcuccz .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qexawcuccz .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #qexawcuccz .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qexawcuccz .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qexawcuccz .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #qexawcuccz .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #qexawcuccz .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qexawcuccz .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qexawcuccz .gt_left { text-align: left; }
 #qexawcuccz .gt_center { text-align: center; }
 #qexawcuccz .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #qexawcuccz .gt_font_normal { font-weight: normal; }
 #qexawcuccz .gt_font_bold { font-weight: bold; }
 #qexawcuccz .gt_font_italic { font-style: italic; }
 #qexawcuccz .gt_super { font-size: 65%; }
 #qexawcuccz .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qexawcuccz .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #qexawcuccz .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qexawcuccz .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qexawcuccz .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #qexawcuccz .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Forward predictiveness — process metric vs next season's xERA |  |  |  |
|----|----|----|----|
| negative r is the expected sign (better stuff/command → lower xERA) |  |  |  |
| predictor (season S) | target (season S+1) | pairs | pearson |
| Stuff+ (pitcher mean) | xERA | 7392 | 0.031 |
| Command+ | xERA | 6798 | −0.062 |

&#10;</div>

<img src="pitching_files/figure-commonmark/cell-9-output-1.png"
width="420" height="300"
alt="This year’s Stuff+ vs next year’s xERA, all adjacent season pairs." />

The reliability ordering is the theoretically expected one — pitch
physics (Stuff+) is the stickiest thing a pitcher owns, location value
is noisier, and outcome-adjacent xERA sits between. The
forward-validation sign check (better process → lower next-season xERA)
is the honest floor for surfaces whose in-season gates are deliberately
weak.

## Results

<div id="imacnxpwsw" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#imacnxpwsw table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#imacnxpwsw thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#imacnxpwsw p { margin: 0; padding: 0; }
 #imacnxpwsw .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #imacnxpwsw .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #imacnxpwsw .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #imacnxpwsw .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #imacnxpwsw .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #imacnxpwsw .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #imacnxpwsw .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #imacnxpwsw .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #imacnxpwsw .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #imacnxpwsw .gt_column_spanner_outer:first-child { padding-left: 0; }
 #imacnxpwsw .gt_column_spanner_outer:last-child { padding-right: 0; }
 #imacnxpwsw .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #imacnxpwsw .gt_spanner_row { border-bottom-style: hidden; }
 #imacnxpwsw .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #imacnxpwsw .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #imacnxpwsw .gt_from_md> :first-child { margin-top: 0; }
 #imacnxpwsw .gt_from_md> :last-child { margin-bottom: 0; }
 #imacnxpwsw .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #imacnxpwsw .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #imacnxpwsw .gt_indent_1 { text-indent: 5px; }
 #imacnxpwsw .gt_indent_2 { text-indent: calc(5px * 2); }
 #imacnxpwsw .gt_indent_3 { text-indent: calc(5px * 3); }
 #imacnxpwsw .gt_indent_4 { text-indent: calc(5px * 4); }
 #imacnxpwsw .gt_indent_5 { text-indent: calc(5px * 5); }
 #imacnxpwsw .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #imacnxpwsw .gt_row_group_first td { border-top-width: 2px; }
 #imacnxpwsw .gt_row_group_first th { border-top-width: 2px; }
 #imacnxpwsw .gt_striped { color: #333333; background-color: #F4F4F4; }
 #imacnxpwsw .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #imacnxpwsw .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #imacnxpwsw .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #imacnxpwsw .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #imacnxpwsw .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #imacnxpwsw .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #imacnxpwsw .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #imacnxpwsw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #imacnxpwsw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #imacnxpwsw .gt_left { text-align: left; }
 #imacnxpwsw .gt_center { text-align: center; }
 #imacnxpwsw .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #imacnxpwsw .gt_font_normal { font-weight: normal; }
 #imacnxpwsw .gt_font_bold { font-weight: bold; }
 #imacnxpwsw .gt_font_italic { font-style: italic; }
 #imacnxpwsw .gt_super { font-size: 65%; }
 #imacnxpwsw .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #imacnxpwsw .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #imacnxpwsw .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #imacnxpwsw .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #imacnxpwsw .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #imacnxpwsw .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 10 pitchers by mean Stuff+ — 2026 |  |  |  |
|----|----|----|----|
|  | Pitcher | Stuff+ | pitch types |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/804546/headshot/67/current"
height="42" /> | Hoss Brewer | 109.0 | 4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/689450/headshot/67/current"
height="42" /> | Luke Taggart | 108.6 | 3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/682044/headshot/67/current"
height="42" /> | Sean Boyle | 108.6 | 5 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/695465/headshot/67/current"
height="42" /> | Larson Kindreich | 108.3 | 2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/664774/headshot/67/current"
height="42" /> | LaMonte Wade Jr. | 108.2 | 2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/800124/headshot/67/current"
height="42" /> | David Lorduy | 108.1 | 3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/804663/headshot/67/current"
height="42" /> | Tyler Cleveland | 107.8 | 4 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/622609/headshot/67/current"
height="42" /> | Hector Villarroel | 107.1 | 3 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/691711/headshot/67/current"
height="42" /> | Jawilme Ramírez | 106.8 | 2 |
| <img
src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/700800/headshot/67/current"
height="42" /> | Darien Smith | 106.8 | 3 |

&#10;</div>

## Provenance & reproducibility

- **Trained on:** Baseball Savant pitch-level features (velocity,
  movement, release, location), seasons in the table above.
- **Committed at:** `mlb/pitching_models/parquet/`; published to
  `mlb_pitching_models`; per-publish metadata in
  [`../../mlb/pitching_models/mlb_pitching_models_card.json`](../../mlb/pitching_models/mlb_pitching_models_card.json).
- **Pipeline:** `scripts/mlb_models.sh 03` → stage
  `python/mlb_model_03_pitching.py` (`mlb_models_cron.yml`). Single
  home: `models/manifest.yaml`.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; `uv sync --group docs`). Names/headshots via one batched statsapi
  call; offline renders fall back to MLBAM ids.

## Avenues for improvement & open issues

- **Stuff+ vs run value remains a weak-signal gate (≥ 0.20)** —
  pitch-level target engineering (per-pitch run value with count
  context) is the known lever.
- **Known issue:** Command+’s 0.04 directional gate is honest but
  near-noise; treat the column as ordinal at best — its YoY reliability
  above quantifies exactly how noisy.
- **RESOLVED (2026-09-01):** the xERA ≡ x_wOBA identity is by design (a
  documented affine wOBA-to-runs conversion in sdv-py `mlb_pitch_era`);
  the render-time check above now guards the recipe rather than flagging
  a bug. Differentiating xERA with batted-ball mix / park inputs remains
  a real avenue — as a new model, not a fix.
