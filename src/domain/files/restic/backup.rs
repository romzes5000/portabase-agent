use super::excludes::backup_excludes;
use super::json::{BackupSummary, Snapshot, files_removed};
use super::repo::{ResticRepo, short};
use crate::domain::files::matcher::{exclude_patterns, one_file_system, source_root};
use crate::services::backup::logger::JobLogger;
use crate::services::config::DatabaseConfig;
use anyhow::{Context, Result, bail};

pub async fn snapshot(
    repo: &ResticRepo,
    cfg: &DatabaseConfig,
    backup_storage_id: &str,
    logger: &JobLogger,
) -> Result<BackupSummary> {
    let root = source_root(cfg)?;
    repo.ensure_initialized(logger).await?;

    let host = format!("portabase-{}", cfg.generated_id);
    let mut args: Vec<String> = vec![
        "backup".into(),
        root.to_string_lossy().into_owned(),
        "--json".into(),
        "--host".into(),
        host.clone(),
        "--tag".into(),
        "portabase".into(),
        "--tag".into(),
        format!("bs:{backup_storage_id}"),
    ];
    for pattern in backup_excludes(&root, &exclude_patterns(cfg))? {
        args.push("--exclude".into());
        args.push(pattern);
    }
    if one_file_system(cfg) {
        args.push("--one-file-system".into());
    }

    logger.log(
        "info",
        format!("Snapshotting {} into {}", root.display(), repo.repository()),
    );
    let run = repo.run(&args, logger).await?;
    for error in &run.errors {
        logger.log("warn", format!("Unreadable: {error}"));
    }
    let summary: Option<BackupSummary> = run
        .summary
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .context("unexpected restic backup summary")?;

    match (run.code, summary) {
        (0, Some(mut s)) => {
            s.files_removed = match removed(repo, &host, &s, logger).await {
                Ok(removed) => removed,
                Err(e) => {
                    logger.log("warn", format!("Could not count removed files: {e:#}"));
                    None
                }
            };
            let removed = s
                .files_removed
                .map(|n| format!(", {n} removed"))
                .unwrap_or_default();
            logger.log(
                "info",
                format!(
                    "Snapshot {}: {} new, {} changed, {} unmodified{removed} file(s); {} byte(s) scanned, {} byte(s) of new data",
                    short(&s.snapshot_id), s.files_new, s.files_changed, s.files_unmodified, s.total_bytes_processed, s.data_added_packed
                ),
            );
            Ok(s)
        }
        (0, None) => bail!("restic backup returned no summary"),
        (3, summary) => {
            let mut fate = "snapshot discarded";
            if let Some(s) = summary {
                if repo.is_append_only() {
                    logger.log(
                        "warn",
                        format!(
                            "Incomplete snapshot {} left for the dashboard to forget",
                            short(&s.snapshot_id)
                        ),
                    );
                    fate = "snapshot not restorable (the dashboard forgets it)";
                } else {
                    let forget = repo.run(["forget", s.snapshot_id.as_str()], logger).await?;
                    if forget.code != 0 {
                        logger.log(
                            "error",
                            format!(
                                "Could not forget incomplete snapshot {}: {}",
                                s.snapshot_id,
                                forget.error("forget")
                            ),
                        );
                    }
                }
            }
            bail!(
                "{} file(s) could not be read; {fate} so a mirror restore cannot delete them (fix permissions or add an exclude)",
                run.errors.len()
            )
        }
        _ => Err(run.error("backup")),
    }
}

async fn removed(
    repo: &ResticRepo,
    host: &str,
    s: &BackupSummary,
    logger: &JobLogger,
) -> Result<Option<u64>> {
    let listed = repo
        .run(["snapshots", "--json", "--host", host], logger)
        .await?;
    if listed.code != 0 {
        return Err(listed.error("snapshots"));
    }
    let snapshots: Vec<Snapshot> = serde_json::from_str(listed.stdout.trim())
        .context("unexpected `restic snapshots` output")?;
    Ok(files_removed(
        &snapshots,
        &s.snapshot_id,
        s.files_changed,
        s.files_unmodified,
    ))
}
