use infinity_provider_local::{BoxError, LocalProvider, config::Config};
use infinity_provider_protocol::remote::serve_provider;
use std::{io::Write, path::Path, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--config" {
        return Err("usage: infinity-provider-local --config <path>".into());
    }
    let provider = Arc::new(LocalProvider::new(Config::load(Path::new(&args[1]))?)?);
    let (path, server) = serve_provider(provider)?;
    println!("{}", path.display());
    std::io::stdout().flush()?;
    server.await;
    Ok(())
}
