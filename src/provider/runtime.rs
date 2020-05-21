use std::path::Path;
use std::sync::Arc;

use tempfile::NamedTempFile;
use tokio::sync::watch::{self, Sender};
use tokio::task::JoinHandle;
use wasm3::{Environment, Module};
use kubelet::handle::{RuntimeHandle, Stop};
use kubelet::status::ContainerStatus;

pub struct HandleStopper {
    handle: JoinHandle<anyhow::Result<()>>,
}

#[async_trait::async_trait]
impl Stop for HandleStopper {
    async fn stop(&mut self) -> anyhow::Result<()> {
        // no nothing
        Ok(())
    }

    async fn wait(&mut self) -> anyhow::Result<()> {
        (&mut self.handle).await??;
        Ok(())
    }
}

/// A runtime context for running a wasm module with wasm3
pub struct Runtime {
    module_bytes: Vec<u8>,
    stack_size: u32,
    output: Arc<NamedTempFile>,
}

impl Runtime {
    pub async fn new<L: AsRef<Path> + Send + Sync + 'static>(module_bytes: Vec<u8>, stack_size: u32, log_dir: L) -> anyhow::Result<Self> {
        let temp = tokio::task::spawn_blocking(move || -> anyhow::Result<NamedTempFile> {
            Ok(NamedTempFile::new_in(log_dir)?)
        })
        .await??;

        Ok(Self {
            module_bytes: module_bytes,
            stack_size: stack_size,
            output: Arc::new(temp),
        })
    }

    pub async fn start(&mut self) -> anyhow::Result<RuntimeHandle<HandleStopper, LogHandleFactory>> {
        let temp = self.output.clone();
        let output_write = tokio::task::spawn_blocking(move || -> anyhow::Result<std::fs::File> {
            Ok(temp.reopen()?)
        })
        .await??;

        let (status_sender, status_recv) = watch::channel(ContainerStatus::Waiting {
            timestamp: chrono::Utc::now(),
            message: "No status has been received from the process".into(),
        });
        let handle = spawn_wasm3(self.module_bytes.clone(), self.stack_size, status_sender, output_write).await?;


        let log_handle_factory = LogHandleFactory {
            temp: self.output.clone(),
        };

        Ok(RuntimeHandle::new(
            HandleStopper{handle},
            log_handle_factory,
            status_recv,
        ))
    }
}

/// Holds our tempfile handle.
pub struct LogHandleFactory {
    temp: Arc<NamedTempFile>,
}

impl kubelet::handle::LogHandleFactory<tokio::fs::File> for LogHandleFactory {
    /// Creates `tokio::fs::File` on demand for log reading.
    fn new_handle(&self) -> tokio::fs::File {
        tokio::fs::File::from_std(self.temp.reopen().unwrap())
    }
}

// Spawns a running wasmtime instance with the given context and status
// channel. Due to the Instance type not being Send safe, all of the logic
// needs to be done within the spawned task
async fn spawn_wasm3(
    module_bytes: Vec<u8>,
    stack_size: u32,
    status_sender: Sender<ContainerStatus>,
    _output_write: std::fs::File, //TODO: hook this up such that log output will be written to the file
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let env = Environment::new().expect("cannot create environment");
        let rt = env.create_runtime(stack_size).expect("cannot create runtime");
        let module = Module::parse(&env, &module_bytes).expect("cannot parse module");
        let mut module = rt.load_module(module).expect("cannot load module");
        module.link_wasi().expect("cannot link WASI");
        let func = module.find_function::<(), ()>("_start").expect("cannot find function '_start' in module");
        func.call().expect("cannot call '_start' in module");
        status_sender
        .broadcast(ContainerStatus::Terminated {
            failed: false,
            message: "Module run completed".into(),
            timestamp: chrono::Utc::now(),
        })
        .expect("status should be able to send");
        Ok(())
    });

    Ok(handle)
}
