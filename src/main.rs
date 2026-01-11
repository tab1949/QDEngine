mod args;
mod server;
mod adaptor;

use std::process::ExitCode;
use std::sync::LazyLock;

static CONFIG: LazyLock<args::Config> = LazyLock::new(|| args::Config::parse(std::env::args()));

#[tokio::main]
async fn main() -> ExitCode {
    use args::Mode;

    match CONFIG.mode {
        Some(Mode::Help) => {
            args::print_help();
            return ExitCode::SUCCESS;
        }
        Some(Mode::Version) => {
            args::print_version();
            return ExitCode::SUCCESS;
        }
        Some(Mode::Server) => {
            let server = server::Server::new(&CONFIG);
            server.main_process().await;
        }
        None => {
            return ExitCode::FAILURE;
        }
    }
    return ExitCode::SUCCESS;
}
