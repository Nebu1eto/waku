//! The collapsed form of a provider report card (choco-pi's `/status` and
//! `/context`). Pure selection over the report already in the message, so a
//! row builder can call it every frame; the web client mirrors it in
//! `apps/web/src/lib/context-report.ts`.

use crate::model::{ProviderReport, ProviderReportKind, ProviderReportRow};

/// Status rows a collapsed card shows, in this order, matched by label. They
/// answer "which model, how full, how much spent" without opening the card.
const STATUS_KEY_LABELS: [&str; 4] = ["Model", "Context usage", "Tokens", "Messages"];
/// Rows a collapsed status card shows when none of the key labels exist.
const STATUS_FALLBACK_ROWS: usize = 4;
/// Category swatches a collapsed context card names beside its bar.
const CONTEXT_TOP_CATEGORIES: usize = 3;

#[derive(Debug, PartialEq)]
pub(super) struct ReportSummary<'a> {
    /// Status rows shown collapsed, in display order.
    pub(super) rows: Vec<&'a ProviderReportRow>,
    /// Indices into the context block's categories, largest first.
    pub(super) top_categories: Vec<usize>,
    /// Sections the collapsed card does not show in full.
    pub(super) hidden_sections: usize,
}

/// The collapsed form of `report`, or `None` when it has none: a usage
/// report, a context report without its typed block, or a status report
/// whose rows all fit the summary anyway.
pub(super) fn report_summary(report: &ProviderReport) -> Option<ReportSummary<'_>> {
    match report.kind {
        ProviderReportKind::Status => status_summary(report),
        ProviderReportKind::Context => {
            let context = report.context.as_ref()?;
            let mut top_categories: Vec<usize> = (0..context.categories.len())
                .filter(|&index| context.categories[index].tokens > 0)
                .collect();
            // Stable, so equal categories keep the provider's order.
            top_categories
                .sort_by_key(|&index| std::cmp::Reverse(context.categories[index].tokens));
            top_categories.truncate(CONTEXT_TOP_CATEGORIES);
            Some(ReportSummary {
                rows: Vec::new(),
                top_categories,
                hidden_sections: report
                    .sections
                    .iter()
                    .filter(|section| !section.rows.is_empty())
                    .count(),
            })
        }
        ProviderReportKind::Usage => None,
    }
}

fn status_summary(report: &ProviderReport) -> Option<ReportSummary<'_>> {
    let all_rows = || report.sections.iter().flat_map(|section| &section.rows);
    let mut rows: Vec<&ProviderReportRow> = STATUS_KEY_LABELS
        .iter()
        .filter_map(|key| all_rows().find(|row| row.label.trim().eq_ignore_ascii_case(key)))
        .collect();
    if rows.is_empty() {
        rows = report
            .sections
            .iter()
            .find(|section| !section.rows.is_empty())
            .map(|section| section.rows.iter().take(STATUS_FALLBACK_ROWS).collect())
            .unwrap_or_default();
    }
    let shown = |row: &ProviderReportRow| rows.iter().any(|kept| std::ptr::eq(*kept, row));
    let hidden_sections = report
        .sections
        .iter()
        .filter(|section| section.rows.iter().any(|row| !shown(row)))
        .count();
    // Hiding a hint alone is not worth a disclosure.
    (hidden_sections > 0).then_some(ReportSummary {
        rows,
        top_categories: Vec::new(),
        hidden_sections,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> ProviderReport {
        let source = match name {
            "status" => {
                include_str!("../../crates/waku-protocol/src/fixtures/choco_pi_report_status.json")
            }
            "context" => {
                include_str!("../../crates/waku-protocol/src/fixtures/choco_pi_report_context.json")
            }
            "usage" => {
                include_str!("../../crates/waku-protocol/src/fixtures/choco_pi_report_usage.json")
            }
            _ => unreachable!(),
        };
        ProviderReport::parse(source).expect("fixture parses")
    }

    fn labels<'a>(summary: &ReportSummary<'a>) -> Vec<&'a str> {
        summary.rows.iter().map(|row| row.label.as_str()).collect()
    }

    #[test]
    fn collapsed_status_shows_its_key_rows_across_sections() {
        let report = fixture("status");
        let summary = report_summary(&report).expect("status collapses");
        assert_eq!(
            labels(&summary),
            ["Model", "Context usage", "Tokens", "Messages"]
        );
        assert_eq!(summary.rows[0].value, "Claude Fable 5.1 (claude-fable-5-1)");
        assert_eq!(summary.hidden_sections, 2);
        assert!(summary.top_categories.is_empty());
    }

    #[test]
    fn status_without_key_labels_falls_back_to_its_first_section() {
        let mut report = fixture("status");
        for section in &mut report.sections {
            for row in &mut section.rows {
                row.label = format!("x-{}", row.label);
            }
        }
        let summary = report_summary(&report).expect("status collapses");
        assert_eq!(
            labels(&summary),
            ["x-File", "x-ID", "x-Messages", "x-Tokens"]
        );
        assert_eq!(summary.hidden_sections, 2);
    }

    #[test]
    fn status_that_fits_the_summary_has_no_disclosure() {
        let mut report = fixture("status");
        report.sections.truncate(1);
        report.sections[0].rows.truncate(STATUS_FALLBACK_ROWS);
        report.sections[0]
            .rows
            .iter_mut()
            .for_each(|row| row.label = format!("x-{}", row.label));
        assert_eq!(report_summary(&report), None);
    }

    #[test]
    fn collapsed_context_keeps_the_bar_and_its_largest_categories() {
        let report = fixture("context");
        let summary = report_summary(&report).expect("context collapses");
        let context = report.context.as_ref().unwrap();
        let top: Vec<&str> = summary
            .top_categories
            .iter()
            .map(|&index| context.categories[index].label.as_str())
            .collect();
        assert_eq!(top, ["System tools", "Skills", "System prompt"]);
        assert!(summary.rows.is_empty());
        // Usage and Inventory both stay behind the disclosure.
        assert_eq!(summary.hidden_sections, 2);
    }

    #[test]
    fn usage_and_untyped_context_reports_never_collapse() {
        assert_eq!(report_summary(&fixture("usage")), None);
        let mut context = fixture("context");
        context.context = None;
        assert_eq!(report_summary(&context), None);
    }
}
