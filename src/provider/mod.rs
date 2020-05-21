//! A custom kubelet backend that can run [WASI](https://wasi.dev/) based workloads using wasm3
//!
//! The crate provides the [`Provider`] type which can be used
//! as a provider with [`kubelet`].
//!
//! # Example
//! ```rust,no_run
//! use kubelet::{Kubelet, config::Config};
//! use kubelet::module_store::FileModuleStore;
//! use wasm3_provider::Provider;
//!
//! async {
//!     // Get a configuration for the Kubelet
//!     let kubelet_config = Config::default();
//!     let client = oci_distribution::Client::default();
//!     let store = FileModuleStore::new(client, &std::path::PathBuf::from(""));
//!
//!     // Load a kubernetes configuration
//!     let kubeconfig = kube::Config::infer().await.unwrap();
//!
//!     // Instantiate the provider type
//!     let provider = Provider::new(store, &kubelet_config, kubeconfig.clone()).await.unwrap();
//!
//!     // Instantiate the Kubelet
//!     let kubelet = Kubelet::new(provider, kubeconfig, kubelet_config);
//!     // Start the Kubelet and block on it
//!     kubelet.start().await.unwrap();
//! };
//! ```
use std::path::PathBuf;

use k8s_openapi::api::core::v1::Pod as KubePod;
use kube::api::DeleteParams;
use kube::Api;
use kube::Config as KubeConfig;
use kubelet::config::Config as KubeletConfig;
use kubelet::handle::{key_from_pod, pod_key, PodHandle};
use kubelet::module_store::ModuleStore;
use kubelet::provider::ProviderError;
use kubelet::Pod;
use log::{debug, error, info, trace};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

mod runtime;
use runtime::{HandleStopper, LogHandleFactory, Runtime};

const TARGET_WASM32_WASI: &str = "wasm32-wasi";
const LOG_DIR_NAME: &str = "wasm3-logs";

/// Provider provides a Kubelet runtime implementation that executes WASM
/// binaries conforming to the WASI spec
pub struct Provider<S> {
    handles: Arc<RwLock<HashMap<String, PodHandle<HandleStopper, LogHandleFactory>>>>,
    store: S,
    log_path: PathBuf,
    kubeconfig: KubeConfig,
}

impl<S: ModuleStore + Send + Sync> Provider<S> {
    /// Create a new wasi provider from a module store and a kubelet config
    pub async fn new(store: S, config: &KubeletConfig, kubeconfig: KubeConfig) -> anyhow::Result<Self> {
        let log_path = config.data_dir.join(LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_path).await?;
        Ok(Self {
            handles: Default::default(),
            store,
            log_path,
            kubeconfig,
        })
    }
}

#[async_trait::async_trait]
impl<S: ModuleStore + Send + Sync> kubelet::Provider for Provider<S> {
    const ARCH: &'static str = TARGET_WASM32_WASI;

    async fn add(&self, pod: Pod) -> anyhow::Result<()> {
        // To run an Add event, we load the WASM, update the pod status to Running,
        // and then execute the WASM, passing in the relevant data.
        // When the pod finishes, we update the status to Succeeded unless it
        // produces an error, in which case we mark it Failed.

        let pod_name = pod.name();
        let mut containers = HashMap::new();
        let client = kube::Client::new(self.kubeconfig.clone());

        let mut modules = self.store.fetch_pod_modules(&pod).await?;
        info!("Starting containers for pod {:?}", pod_name);
        for container in pod.containers() {
            let module_data = modules
                .remove(&container.name)
                .expect("FATAL ERROR: module map not properly populated");

            // TODO: expose this as a feature flag (--stack-size)
            let mut runtime = Runtime::new(module_data, (1024 * 60) as u32, self.log_path.clone()).await?;

            debug!("Starting container {} on thread", container.name);
            let handle = runtime.start().await?;
            containers.insert(container.name.clone(), handle);
        }
        info!(
            "All containers started for pod {:?}. Updating status",
            pod_name
        );

        // Wrap this in a block so the write lock goes out of scope when we are done
        {
            // Grab the entry while we are creating things
            let mut handles = self.handles.write().await;
            handles.insert(
                key_from_pod(&pod),
                PodHandle::new(containers, pod, client, None)?,
            );
        }

        Ok(())
    }

    async fn modify(&self, pod: Pod) -> anyhow::Result<()> {
        // The only things we care about are:
        // 1. metadata.deletionTimestamp => signal all containers to stop and then mark them
        //    as terminated
        // 2. spec.containers[*].image, spec.initContainers[*].image => stop the currently
        //    running containers and start new ones?
        // 3. spec.activeDeadlineSeconds => Leaving unimplemented for now
        // TODO: Determine what the proper behavior should be if labels change
        debug!(
            "Got pod modified event for {} in namespace {}",
            pod.name(),
            pod.namespace()
        );
        trace!("Modified pod spec: {:#?}", pod.as_kube_pod());
        if let Some(_timestamp) = pod.deletion_timestamp() {
            let mut handles = self.handles.write().await;
            match handles.get_mut(&key_from_pod(&pod)) {
                Some(h) => {
                    h.stop().await?;
                    // Follow up with a delete when everything is stopped
                    let dp = DeleteParams {
                        grace_period_seconds: Some(0),
                        ..Default::default()
                    };
                    let pod_client: Api<KubePod> = Api::namespaced(
                        kube::client::Client::new(self.kubeconfig.clone()),
                        pod.namespace(),
                    );
                    match pod_client.delete(pod.name(), &dp).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e.into()),
                    }
                }
                None => {
                    // This isn't an error with the pod, so don't return an error (otherwise it will
                    // get updated in its status). This is an unlikely case to get into and means
                    // that something is likely out of sync, so just log the error
                    error!(
                        "Unable to find pod {} in namespace {} when trying to stop all containers",
                        pod.name(),
                        pod.namespace()
                    );
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
        // TODO: Implement behavior for stopping old containers and restarting when the container
        // image changes
    }

    async fn delete(&self, pod: Pod) -> anyhow::Result<()> {
        let mut handles = self.handles.write().await;
        match handles.remove(&key_from_pod(&pod)) {
            Some(_) => debug!(
                "Pod {} in namespace {} removed",
                pod.name(),
                pod.namespace()
            ),
            None => info!(
                "unable to find pod {} in namespace {}, it was likely already deleted",
                pod.name(),
                pod.namespace()
            ),
        }
        Ok(())
    }

    async fn logs(
        &self,
        namespace: String,
        pod_name: String,
        _container_name: String,
        _sender: kubelet::LogSender,
    ) -> anyhow::Result<()> {
        let mut handles = self.handles.write().await;
        let _containers = handles
            .get_mut(&pod_key(&namespace, &pod_name))
            .ok_or_else(|| ProviderError::PodNotFound {
                pod_name: pod_name.clone(),
            })?;
        // pod.output(&container_name, sender).await
        unimplemented!()
    }
}
