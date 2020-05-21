use std::error;
use std::fmt;

use wasm3::Environment;

type Result<T> = std::result::Result<T, RuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    kind: RuntimeErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeErrorKind {
    AlreadyStarted,
    CannotCreateRuntime,
    CannotLinkWASI,
    CannotLoadModule,
    NoEntrypoint,
    RunFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeStatus {
    Running,
    Stopped,
}

impl RuntimeError {
    fn new(kind: RuntimeErrorKind) -> Self {
        Self { kind: kind }
    }

    fn __description(&self) -> &str {
        match self.kind {
            RuntimeErrorKind::AlreadyStarted => "runtime already started",
            RuntimeErrorKind::CannotCreateRuntime => "cannot create runtime",
            RuntimeErrorKind::CannotLinkWASI => "cannot link module to the WASI runtime",
            RuntimeErrorKind::CannotLoadModule => "cannot load module",
            RuntimeErrorKind::NoEntrypoint => "no entrypoint function called '_start' found",
            RuntimeErrorKind::RunFailure => "failure during function call",
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.__description())
    }
}

impl error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // source is not tracked
        None
    }
}

/// A runtime context for running a wasm module with wasm3
pub struct Runtime {
    module_bytes: Vec<u8>,
    stack_size: u32,
    current_status: RuntimeStatus,
}

impl Runtime {
    pub fn new(module_bytes: Vec<u8>, stack_size: u32) -> Self {
        Self {
            module_bytes: module_bytes,
            stack_size: stack_size,
            current_status: RuntimeStatus::Stopped,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        if self.current_status == RuntimeStatus::Running {
            return Err(RuntimeError::new(RuntimeErrorKind::AlreadyStarted));
        }
        let env = Environment::new()
            .map_err(|_| RuntimeError::new(RuntimeErrorKind::CannotCreateRuntime))?;
        let rt = env
            .create_runtime(self.stack_size)
            .map_err(|_| RuntimeError::new(RuntimeErrorKind::CannotCreateRuntime))?;
        let mut module = rt
            .parse_and_load_module(&self.module_bytes)
            .map_err(|_| RuntimeError::new(RuntimeErrorKind::CannotLoadModule))?;
        module.link_wasi().map_err(|_| RuntimeError::new(RuntimeErrorKind::CannotLinkWASI))?;
        let func = module
            .find_function::<(), ()>("_start")
            .map_err(|_| RuntimeError::new(RuntimeErrorKind::NoEntrypoint))?;
        self.current_status = RuntimeStatus::Running;
        // FIXME: run this in the background
        // for now, we block until the function is complete, then call .stop()
        func.call()
            .map_err(|_| RuntimeError::new(RuntimeErrorKind::RunFailure))?;
        self.stop()
    }

    pub fn stop(&mut self) -> Result<()> {
        // it is OK for the runtime to stop an already stopped module. Effectively a no-op
        self.current_status = RuntimeStatus::Stopped;
        Ok(())
    }
}
