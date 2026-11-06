use std::path::Path;
use std::process::Command;

fn main() {
    let web_dir = Path::new("../../agentd-web");
    for path in [
        "src",
        "public",
        "index.html",
        "package.json",
        "package-lock.json",
        "tsconfig.json",
        "vite.config.ts",
    ] {
        println!("cargo:rerun-if-changed={}", web_dir.join(path).display());
    }
    // The app imports this stylesheet by path, without depending on the package.
    println!("cargo:rerun-if-changed=../../infinity-ui/src/theme.css");

    // A default build never touches npm.
    if std::env::var_os("CARGO_FEATURE_BUNDLED_WEB").is_none() {
        return;
    }
    for args in [&["ci", "--prefix"][..], &["run", "build", "--prefix"][..]] {
        let status = Command::new("npm")
            .args(args)
            .arg(web_dir)
            .status()
            .unwrap_or_else(|error| panic!("the bundled-web feature needs npm on PATH: {error}"));
        assert!(status.success(), "npm {args:?} failed in agentd-web");
    }
}
