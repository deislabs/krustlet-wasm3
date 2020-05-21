use kubelet::config::Config;
use kubelet::module_store::FileModuleStore;
use kubelet::Kubelet;

mod provider;
use provider::Provider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::new_from_flags(env!("CARGO_PKG_VERSION"));
    let kubeconfig = kube::Config::infer().await?;

    env_logger::init();

    let client = oci_distribution::Client::default();
    let mut module_store_path = config.data_dir.join(".oci");
    module_store_path.push("modules");
    let store = FileModuleStore::new(client, &module_store_path);

    let provider = Provider::new(store, &config, kubeconfig.clone()).await?;
    let kubelet = Kubelet::new(provider, kubeconfig, config);
    kubelet.start().await
}
