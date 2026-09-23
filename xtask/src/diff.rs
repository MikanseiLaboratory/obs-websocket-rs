//! Semantic diff between two vendored `protocol.json` files.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use crate::codegen::{Protocol, load_overrides, untyped_fields, workspace_root};

pub fn report(from: Option<&str>, to: Option<&str>) -> Result<String, String> {
    let root = workspace_root();
    let from_path = from
        .map(|path| root.join(path))
        .unwrap_or_else(|| root.join("protocol/protocol.json"));
    let to_path = match to {
        Some(path) => root.join(path),
        None => root.join("protocol/protocol.next.json"),
    };
    let previous = load(&from_path)?;
    let next = load(&to_path)?;
    let overrides = load_overrides()?;
    Ok(render(&previous, &next, &untyped_fields(&next, &overrides)))
}

fn load(path: &std::path::Path) -> Result<Protocol, String> {
    let text = fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))
}

fn render(previous: &Protocol, next: &Protocol, untyped: &[String]) -> String {
    let mut out = String::from("# obs-websocket protocol diff\n\n");
    let old_requests = index_requests(previous);
    let new_requests = index_requests(next);
    section(
        &mut out,
        "Added requests",
        &difference(new_requests.keys(), old_requests.keys()),
    );
    section(
        &mut out,
        "Removed requests",
        &difference(old_requests.keys(), new_requests.keys()),
    );
    let mut changed = Vec::new();
    for (name, request) in &new_requests {
        if let Some(old) = old_requests.get(name) {
            if old != request {
                changed.push(name.clone());
            }
        }
    }
    section(&mut out, "Changed requests", &changed);

    let old_events = index_events(previous);
    let new_events = index_events(next);
    section(
        &mut out,
        "Added events",
        &difference(new_events.keys(), old_events.keys()),
    );
    section(
        &mut out,
        "Removed events",
        &difference(old_events.keys(), new_events.keys()),
    );
    let mut changed_events = Vec::new();
    for (name, event) in &new_events {
        if let Some(old) = old_events.get(name) {
            if old != event {
                changed_events.push(name.clone());
            }
        }
    }
    section(&mut out, "Changed events", &changed_events);

    let old_enums = index_enums(previous);
    let new_enums = index_enums(next);
    let mut enum_changes = Vec::new();
    for (name, values) in &new_enums {
        match old_enums.get(name) {
            None => enum_changes.push(format!("{name} added")),
            Some(old) if old != values => enum_changes.push(format!("{name} changed")),
            _ => {}
        }
    }
    for name in old_enums.keys() {
        if !new_enums.contains_key(name) {
            enum_changes.push(format!("{name} removed"));
        }
    }
    section(&mut out, "Enums", &enum_changes);
    section(
        &mut out,
        "Object fields still typed as JSON values",
        untyped,
    );

    if changed.is_empty()
        && changed_events.is_empty()
        && enum_changes.is_empty()
        && old_requests.len() == new_requests.len()
        && old_events.len() == new_events.len()
    {
        out.push_str("\nNo protocol changes.\n");
    }
    out
}

fn section(out: &mut String, title: &str, items: &[String]) {
    out.push_str("## ");
    out.push_str(title);
    out.push_str("\n\n");
    if items.is_empty() {
        out.push_str("None.\n\n");
        return;
    }
    for item in items {
        out.push_str("- `");
        out.push_str(item);
        out.push_str("`\n");
    }
    out.push('\n');
}

fn difference<'a>(
    left: impl Iterator<Item = &'a String>,
    right: impl Iterator<Item = &'a String>,
) -> Vec<String> {
    let right: BTreeSet<&String> = right.collect();
    left.filter(|item| !right.contains(item)).cloned().collect()
}

fn index_requests(protocol: &Protocol) -> BTreeMap<String, String> {
    protocol
        .requests
        .iter()
        .map(|request| {
            let fields = field_summary(&request.request_fields, &request.response_fields, &[]);
            (
                request.request_type.clone(),
                format!(
                    "{}|{}|{}|{fields}",
                    request.deprecated, request.initial_version, request.rpc_version
                ),
            )
        })
        .collect()
}

fn index_events(protocol: &Protocol) -> BTreeMap<String, String> {
    protocol
        .events
        .iter()
        .map(|event| {
            (
                event.event_type.clone(),
                format!(
                    "{}|{}|{}|{}",
                    event.deprecated,
                    event.initial_version,
                    event.event_subscription,
                    field_summary(&[], &[], &event.data_fields)
                ),
            )
        })
        .collect()
}

fn index_enums(protocol: &Protocol) -> BTreeMap<String, String> {
    protocol
        .enums
        .iter()
        .map(|item| {
            let values = item
                .enum_identifiers
                .iter()
                .map(|identifier| {
                    format!(
                        "{}={}|dep={}",
                        identifier.enum_identifier, identifier.enum_value, identifier.deprecated
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            (item.enum_type.clone(), values)
        })
        .collect()
}

fn field_summary(
    request_fields: &[crate::codegen::ProtocolField],
    response_fields: &[crate::codegen::ProtocolField],
    data_fields: &[crate::codegen::ProtocolField],
) -> String {
    let mut parts = Vec::new();
    for (side, fields) in [
        ("request", request_fields),
        ("response", response_fields),
        ("data", data_fields),
    ] {
        for field in fields {
            parts.push(format!(
                "{side}:{}:{}:{}",
                field.value_name, field.value_type, field.value_optional
            ));
        }
    }
    parts.join(",")
}
