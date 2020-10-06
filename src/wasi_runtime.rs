use futures::task;
use log::{error, info, trace};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};

use tempfile::NamedTempFile;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use wasm3::{Environment, Module};

use kubelet::container::Handle as ContainerHandle;
use kubelet::container::Status;
use kubelet::handle::StopHandler;

pub struct Runtime {
    handle: JoinHandle<anyhow::Result<()>>,
}

#[async_trait::async_trait]
impl StopHandler for Runtime {
    async fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn wait(&mut self) -> anyhow::Result<()> {
        (&mut self.handle).await??;
        Ok(())
    }
}

/// WasiRuntime provides a WASI compatible runtime. A runtime should be used for
/// each "instance" of a process and can be passed to a thread pool for running
pub struct WasiRuntime {
    // name of the process
    name: String,
    /// Data needed for the runtime
    data: Arc<Data>,
    /// The tempfile that output from the wasmtime process writes to
    output: Arc<NamedTempFile>,
    /// A channel to send status updates on the runtime
    status_sender: Sender<(String, Status)>,
    /// The stack size to be used with the wasm3 runtime.
    stack_size: u32,
}

struct Data {
    /// binary module data to be run as a wasm module
    module_data: Vec<u8>,
    /// key/value environment variables made available to the wasm process
    env: HashMap<String, String>,
    /// the arguments passed as the command-line arguments list
    args: Vec<String>,
    /// a hash map of local file system paths to optional path names in the runtime
    /// (e.g. /tmp/foo/myfile -> /app/config). If the optional value is not given,
    /// the same path will be allowed in the runtime
    dirs: HashMap<PathBuf, Option<PathBuf>>,
}

/// Holds our tempfile handle.
pub struct HandleFactory {
    temp: Arc<NamedTempFile>,
}

impl kubelet::log::HandleFactory<tokio::fs::File> for HandleFactory {
    /// Creates `tokio::fs::File` on demand for log reading.
    fn new_handle(&self) -> tokio::fs::File {
        tokio::fs::File::from_std(self.temp.reopen().unwrap())
    }
}

impl WasiRuntime {
    /// Creates a new WasiRuntime
    ///
    /// # Arguments
    ///
    /// * `module_path` - the path to the WebAssembly binary
    /// * `env` - a collection of key/value pairs containing the environment variables
    /// * `args` - the arguments passed as the command-line arguments list
    /// * `dirs` - a map of local file system paths to optional path names in the runtime
    ///     (e.g. /tmp/foo/myfile -> /app/config). If the optional value is not given,
    ///     the same path will be allowed in the runtime
    /// * `log_dir` - location for storing logs
    pub async fn new<L: AsRef<Path> + Send + Sync + 'static>(
        name: String,
        module_data: Vec<u8>,
        env: HashMap<String, String>,
        args: Vec<String>,
        dirs: HashMap<PathBuf, Option<PathBuf>>,
        log_dir: L,
        status_sender: Sender<(String, Status)>,
    ) -> anyhow::Result<Self> {
        let temp = tokio::task::spawn_blocking(move || -> anyhow::Result<NamedTempFile> {
            Ok(NamedTempFile::new_in(log_dir)?)
        })
        .await??;

        // We need to use named temp file because we need multiple file handles
        // and if we are running in the temp dir, we run the possibility of the
        // temp file getting cleaned out from underneath us while running. If we
        // think it necessary, we can make these permanent files with a cleanup
        // loop that runs elsewhere. These will get deleted when the reference
        // is dropped
        Ok(WasiRuntime {
            name,
            data: Arc::new(Data {
                module_data,
                env,
                args,
                dirs,
            }),
            output: Arc::new(temp),
            status_sender,
            stack_size: 1,
        })
    }

    pub async fn start(&self) -> anyhow::Result<ContainerHandle<Runtime, HandleFactory>> {
        let temp = self.output.clone();
        // Because a reopen is blocking, run in a blocking task to get new
        // handles to the tempfile
        let output_write = tokio::task::spawn_blocking(move || -> anyhow::Result<std::fs::File> {
            Ok(temp.reopen()?)
        })
        .await??;

        let handle = self.spawn_wasm3(output_write).await?;

        let log_handle_factory = HandleFactory {
            temp: self.output.clone(),
        };

        Ok(ContainerHandle::new(
            Runtime {
                handle,
            },
            log_handle_factory,
        ))
    }

    // Spawns a running wasmtime instance with the given context and status
    // channel. Due to the Instance type not being Send safe, all of the logic
    // needs to be done within the spawned task
    async fn spawn_wasm3(
        &self,
        _output_write: std::fs::File,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        // Clone the module data Arc so it can be moved
        let data = self.data.clone();
        let name = self.name.clone();
        let stack_size = self.stack_size.clone();
        let status_sender = self.status_sender.clone();

        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let waker = task::noop_waker();
            let mut cx = Context::from_waker(&waker);

            let env = match Environment::new() {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(e) => e,
                Err(e) => {
                    let message = "cannot create environment";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name,
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            let rt = match env.create_runtime(stack_size) {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(rt) => rt,
                Err(e) => {
                    let message = "cannot create runtime";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name.clone(),
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            let module = match Module::parse(&env, &data.module_data) {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(m) => m,
                Err(e) => {
                    let message = "cannot parse module";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name.clone(),
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            let mut module = match rt.load_module(module) {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(m) => m,
                Err(e) => {
                    let message = "cannot load module";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name.clone(),
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            match module.link_wasi() {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(_) => {}
                Err(e) => {
                    let message = "cannot link WASI";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name.clone(),
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            let func = match module.find_function::<(), ()>("_start") {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(f) => f,
                Err(e) => {
                    let message = "cannot find function '_start' in module";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name.clone(),
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            match func.call() {
                // We can't map errors here or it moves the send channel, so we
                // do it in a match
                Ok(_) => {}
                Err(e) => {
                    let message = "unable to run module";
                    error!("{}: {:?}", message, e);
                    send(
                        status_sender.clone(),
                        name.clone(),
                        Status::Terminated {
                            failed: true,
                            message: message.into(),
                            timestamp: chrono::Utc::now(),
                        },
                        &mut cx,
                    );
                    return Err(anyhow::anyhow!("{}: {}", message, e));
                }
            };

            info!("module run complete");
            send(
                status_sender.clone(),
                name,
                Status::Terminated {
                    failed: false,
                    message: "Module run complete".into(),
                    timestamp: chrono::Utc::now(),
                },
                &mut cx,
            );

            Ok(())
        });

        Ok(handle)
    }
}

fn send(mut sender: Sender<(String, Status)>, name: String, status: Status, cx: &mut Context<'_>) {
    loop {
        if let Poll::Ready(r) = sender.poll_ready(cx) {
            if r.is_ok() {
                sender
                    .try_send((name, status))
                    .expect("Possible deadlock, exiting");
                return;
            }
            trace!("Receiver for status showing as closed: {:?}", r);
        }
        trace!(
            "Channel for container {} not ready for send. Attempting again",
            name
        );
    }
}
