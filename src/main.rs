mod args;
mod server;
mod adaptor;

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    use args::Config;
    use args::Mode;

    let config = Config::parse(&mut std::env::args());

    match config.mode {
        Some(Mode::Help) => {
            args::print_help();
            return ExitCode::SUCCESS;
        }
        Some(Mode::Version) => {
            args::print_version();
            return ExitCode::SUCCESS;
        }
        Some(Mode::Server) => {
            server::main_process(&config).await;
        }
        None => {
            return ExitCode::FAILURE;
        }
    }
    return ExitCode::SUCCESS;
}
