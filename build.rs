use std::{process::Command, str::from_utf8};

fn main() {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .unwrap();

    let commit_hash = from_utf8(&output.stdout).unwrap().trim();

    println!("cargo:rustc-env=GIT_COMMIT={commit_hash}");
}
