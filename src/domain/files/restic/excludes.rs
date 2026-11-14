use crate::domain::files::matcher::bracket_negation;
use anyhow::{Result, bail};
use std::path::Path;

pub fn glob_escape(literal: &str) -> String {
    let mut out = String::with_capacity(literal.len());
    for c in literal.chars() {
        if matches!(c, '\\' | '*' | '?' | '[' | ']') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn normalized(patterns: &[String]) -> Result<Vec<(bool, String)>> {
    let mut out = Vec::new();
    for raw in patterns {
        let pattern = raw.trim();
        if pattern.contains(['{', '}']) {
            bail!(
                "exclude pattern '{pattern}': brace patterns are not supported in Snapshots mode"
            );
        }
        let (anchored, rest) = match pattern.strip_prefix('/') {
            Some(rest) => (true, rest),
            None => (false, pattern),
        };
        let rest = rest.trim_end_matches('/');
        if !rest.is_empty() {
            out.push((anchored, bracket_negation(rest)));
        }
    }
    Ok(out)
}

pub fn backup_excludes(root: &Path, patterns: &[String]) -> Result<Vec<String>> {
    let root = glob_escape(root.to_string_lossy().trim_end_matches('/'));
    Ok(normalized(patterns)?
        .into_iter()
        .map(|(anchored, p)| {
            if anchored {
                format!("{root}/{p}")
            } else {
                format!("{root}/**/{p}")
            }
        })
        .collect())
}

pub fn restore_excludes(patterns: &[String]) -> Result<Vec<String>> {
    Ok(normalized(patterns)?
        .into_iter()
        .map(|(anchored, p)| {
            if anchored {
                format!("/{p}")
            } else {
                format!("/**/{p}")
            }
        })
        .collect())
}
