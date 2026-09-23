//! Download `protocol.json` from a tagged obs-websocket release.

use std::fs;

use serde::Deserialize;

use crate::codegen::workspace_root;

#[derive(Deserialize)]
struct Tag {
    name: String,
}

#[derive(Deserialize)]
struct GitRef {
    object: GitObject,
}

#[derive(Deserialize)]
struct GitObject {
    sha: String,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct TagObject {
    object: GitObject,
}

pub fn run(reference: Option<&str>) -> Result<(), String> {
    let root = workspace_root();
    let tag = match reference {
        Some(tag) => tag.to_string(),
        None => latest_v5_tag()?,
    };
    let commit = tag_commit(&tag)?;
    let url = format!(
        "https://raw.githubusercontent.com/obsproject/obs-websocket/{tag}/docs/generated/protocol.json"
    );
    let body = http_text(&url)?;
    serde_json::from_str::<serde_json::Value>(&body).map_err(|error| error.to_string())?;
    fs::write(root.join("protocol/protocol.json"), &body).map_err(|error| error.to_string())?;
    let upstream = format!(
        "repo = \"obsproject/obs-websocket\"\ntag = \"{tag}\"\ncommit = \"{commit}\"\nprotocol_path = \"docs/generated/protocol.json\"\n"
    );
    fs::write(root.join("protocol/UPSTREAM.toml"), upstream).map_err(|error| error.to_string())?;
    eprintln!("fetched obs-websocket {tag} ({commit})");
    Ok(())
}

fn latest_v5_tag() -> Result<String, String> {
    let body =
        http_text("https://api.github.com/repos/obsproject/obs-websocket/tags?per_page=100")?;
    let tags: Vec<Tag> = serde_json::from_str(&body).map_err(|error| error.to_string())?;
    tags.into_iter()
        .filter_map(|tag| version_key(&tag.name).map(|key| (key, tag.name)))
        .filter(|(key, _)| key.0 == 5)
        .max_by_key(|(key, _)| *key)
        .map(|(_, name)| name)
        .ok_or_else(|| "no obs-websocket 5.x tag found".into())
}

fn version_key(tag: &str) -> Option<(u32, u32, u32)> {
    let mut parts = tag.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn tag_commit(tag: &str) -> Result<String, String> {
    let url = format!("https://api.github.com/repos/obsproject/obs-websocket/git/ref/tags/{tag}");
    let body = http_text(&url)?;
    let git_ref: GitRef = serde_json::from_str(&body).map_err(|error| error.to_string())?;
    if git_ref.object.kind == "commit" {
        return Ok(git_ref.object.sha);
    }
    let url = format!(
        "https://api.github.com/repos/obsproject/obs-websocket/git/tags/{}",
        git_ref.object.sha
    );
    let body = http_text(&url)?;
    let tag: TagObject = serde_json::from_str(&body).map_err(|error| error.to_string())?;
    Ok(tag.object.sha)
}

fn http_text(url: &str) -> Result<String, String> {
    ureq::get(url)
        .set("User-Agent", "obs-websocket-rs")
        .set("Accept", "application/vnd.github+json")
        .call()
        .map_err(|error| error.to_string())?
        .into_string()
        .map_err(|error| error.to_string())
}
