//! `cargo xtask codegen`, `fetch-protocol`, and `protocol-diff`.

mod codegen;
mod diff;
mod fetch;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        usage();
        return ExitCode::from(2);
    };
    let result = match command.as_str() {
        "codegen" => {
            let check = args.any(|arg| arg == "--check");
            codegen::run(check)
        }
        "fetch-protocol" => {
            let mut reference = None;
            let rest: Vec<String> = args.collect();
            let mut index = 0;
            while index < rest.len() {
                if rest[index] == "--ref" {
                    reference = rest.get(index + 1).cloned();
                    index += 2;
                } else {
                    usage();
                    return ExitCode::from(2);
                }
            }
            fetch::run(reference.as_deref())
        }
        "protocol-diff" => {
            let from = args.next();
            let to = args.next();
            match diff::report(from.as_deref(), to.as_deref()) {
                Ok(report) => {
                    println!("{report}");
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
        _ => {
            usage();
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn usage() {
    eprintln!(
        "usage:\n  cargo xtask codegen [--check]\n  cargo xtask fetch-protocol [--ref <tag>]\n  cargo xtask protocol-diff [from.json] [to.json]"
    );
}
