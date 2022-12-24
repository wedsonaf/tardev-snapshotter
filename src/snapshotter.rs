use containerd_snapshots::{api, Info, Kind, Snapshotter, Usage};
use oci_distribution::{secrets::RegistryAuth, Client, Reference, RegistryOperation};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fs, fs::OpenOptions, future::Future, path::Path};
use tonic::Status;

const SNAPSHOT_REF_LABEL: &str = "containerd.io/snapshot.ref";
const TARGET_LAYER_DIGEST_LABEL: &str = "containerd.io/snapshot/cri.layer-digest";
const TARGET_REF_LABEL: &str = "containerd.io/snapshot/cri.image-ref";

/// The snapshotter that creates tar devices.
pub(crate) struct TarDevSnapshotter {
    root: String,
}

impl TarDevSnapshotter {
    /// Creates a new instance of the snapshotter.
    ///
    /// `root` is the root directory where the snapshotter state is to be stored.
    pub(crate) fn new(root: &str) -> Self {
        Self {
            root: root.to_string(),
        }
    }

    /// Creates the snapshot file path from its name.
    ///
    /// If `write` is `true`, it also ensures that the directory exists.
    fn snapshot_path(&self, name: &str, write: bool) -> Result<String, Status> {
        let mut hasher = Sha256::new();
        hasher.update(name);
        let mut path = format!("{}/snapshots/", self.root);
        for b in hasher.finalize() {
            path += &format!("{:02x}", b);
        }

        if write {
            if let Some(parent) = Path::new(&path).parent() {
                fs::create_dir_all(parent)?;
            }
        }

        Ok(path)
    }

    /// Reads the information from storage for the given snapshot name.
    fn read_snapshot(&self, name: &str) -> Result<Info, Status> {
        let path = self.snapshot_path(name, false)?;
        let file = fs::File::open(path)?;
        serde_json::from_reader(file).map_err(|_| Status::unknown("unable to read snapshot"))
    }

    /// Writes to storage the given snapshot information.
    ///
    /// It fails if a snapshot with the given name already exists.
    fn write_snapshot(
        &self,
        kind: Kind,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<(), Status> {
        let info = Info {
            kind,
            name: key,
            parent,
            labels,
            ..Info::default()
        };
        let name = self.snapshot_path(&info.name, true)?;
        // TODO: How to specify the file mode (e.g., 0600)?
        let file = OpenOptions::new().write(true).create_new(true).open(name)?;
        serde_json::to_writer_pretty(file, &info)
            .map_err(|_| Status::internal("unable to write snapshot"))
    }

    /// Creates a new snapshot for use.
    ///
    /// It checks that the parent chain exists and that all ancestors are committed and consist of
    /// layers before writing the new snapshot.
    fn prepare_snapshot_for_use(
        &self,
        kind: Kind,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Status> {
        // Get chain of parents.
        let mut next_parent = Some(parent.clone());
        let mut parents = Vec::new();
        while let Some(p) = next_parent {
            println!("About to try to read {}", &p);
            let info = self.read_snapshot(&p)?;
            if info.kind != Kind::Committed {
                return Err(Status::failed_precondition(
                    "parent snapshot is not committed",
                ));
            }

            match info.labels.get(TARGET_LAYER_DIGEST_LABEL) {
                Some(digest) => parents.push(digest.clone()),
                None => {
                    return Err(Status::failed_precondition(
                        "parent snapshot missing digest",
                    ))
                }
            }

            next_parent = (!info.parent.is_empty()).then_some(info.parent);
        }

        println!("Parents are: {:?}", parents);

        // Write the new snapshot.
        self.write_snapshot(kind, key, parent, labels)?;

        // TODO: Get the details on how to mount it.
        Ok(Vec::new())
    }

    /// Creates a new snapshot for an image layer.
    ///
    /// It downloads and decompresses the layer before writing the new snapshot.
    async fn prepare_image_layer(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Status> {
        let reference: Reference = {
            let image_ref = if let Some(r) = labels.get(TARGET_REF_LABEL) {
                r
            } else {
                return Err(Status::invalid_argument("missing target ref label"));
            };
            image_ref
                .parse()
                .map_err(|_| Status::invalid_argument("bad target ref"))?
        };

        {
            let digest_str = if let Some(d) = labels.get(TARGET_LAYER_DIGEST_LABEL) {
                d
            } else {
                return Err(Status::invalid_argument(
                    "missing target layer digest label",
                ));
            };

            let mut client = Client::new(Default::default());

            client
                .auth(
                    &reference,
                    &RegistryAuth::Anonymous,
                    RegistryOperation::Pull,
                )
                .await
                .map_err(|_| Status::internal("unable to authenticate"))?;

            let name = format!("{}/layers/{}.gz", self.root, digest_str);
            if let Some(parent) = Path::new(&name).parent() {
                fs::create_dir_all(parent)?;
            }

            println!("Downloading to {}", &name);
            let mut file = tokio::fs::File::create(&name).await?;
            if let Err(err) = client.pull_blob(&reference, digest_str, &mut file).await {
                drop(file);
                println!("Download failed: {:?}", err);
                let _ = fs::remove_file(&name);
                return Err(Status::unknown("unable to pull blob"));
            }

            // TODO: Decompress in stream instead of doing this.
            // Decompress data.
            if !tokio::process::Command::new("gunzip")
                .arg(&name[..name.len() - 3])
                .spawn()?
                .wait()
                .await?
                .success()
            {
                let _ = fs::remove_file(&name);
                return Err(Status::unknown("unable to decompress layer"));
            }
        }

        // TODO: Check existence of parent.
        self.write_snapshot(Kind::Committed, key, parent, labels)?;

        Err(Status::already_exists(""))
    }
}

#[tonic::async_trait]
impl Snapshotter for TarDevSnapshotter {
    type Error = Status;

    async fn stat(&self, key: String) -> Result<Info, Self::Error> {
        self.read_snapshot(&key)
    }

    async fn update(
        &self,
        _info: Info,
        _fieldpaths: Option<Vec<String>>,
    ) -> Result<Info, Self::Error> {
        Err(Status::unimplemented("no support for updating snapshots"))
    }

    async fn usage(&self, key: String) -> Result<Usage, Self::Error> {
        // TODO: Implement this.
        println!("Usage: {}", key);
        Ok(Usage::default())
    }

    async fn mounts(&self, key: String) -> Result<Vec<api::types::Mount>, Self::Error> {
        // TODO: Implement this.
        println!("Mounts: {}", key);
        Ok(Vec::new())
        /*
        Ok(vec![api::types::Mount {
            r#type: "gz-overlay".to_string(),
            source: "/home/wedsonaf/src/cloud-hypervisor/vhost_user_block/pause.sock".to_string(),
            target: String::new(),
            options: vec!["ro".to_string()],
        }])
        */
    }

    async fn prepare(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Status> {
        println!(
            "Prepare: key={}, parent={}, labels={:?}",
            key, parent, labels
        );

        // There are two reasons for preparing a snapshot: to build an image and to actually use it
        // as a container image. We determine the reason by the presence of the snapshot-ref label.
        if let Some(snapshot) = labels.get(SNAPSHOT_REF_LABEL) {
            self.prepare_image_layer(snapshot.to_string(), parent, labels)
                .await
        } else {
            self.prepare_snapshot_for_use(Kind::Active, key, parent, labels)
        }
    }

    async fn view(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        println!("View: key={}, parent={}, labels={:?}", key, parent, labels);
        self.prepare_snapshot_for_use(Kind::View, key, parent, labels)
    }

    async fn commit(
        &self,
        _name: String,
        _key: String,
        _labels: HashMap<String, String>,
    ) -> Result<(), Self::Error> {
        Err(Status::unimplemented("no support for commiting snapshots"))
    }

    async fn remove(&self, key: String) -> Result<(), Self::Error> {
        println!("Remove: {}", key);
        let name = self.snapshot_path(&key, false)?;
        fs::remove_file(name)?;
        Ok(())
    }

    async fn walk<F, R>(&self, mut cb: F) -> Result<(), Status>
    where
        F: Send + FnMut(Info) -> R,
        R: Send + Future<Output = Result<(), Status>>,
    {
        let snapshots_dir = format!("{}/snapshots/", self.root);
        let mut files = tokio::fs::read_dir(snapshots_dir).await?;
        while let Some(p) = files.next_entry().await? {
            if let Ok(f) = fs::File::open(p.path()) {
                if let Ok(i) = serde_json::from_reader(f) {
                    cb(i).await?;
                }
            }
        }
        Ok(())
    }
}
