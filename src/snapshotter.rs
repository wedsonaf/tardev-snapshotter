use containerd_snapshots::{api, Info, Kind, Snapshotter, Usage};
use oci_distribution::{secrets::RegistryAuth, Client, Reference, RegistryOperation};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fs, fs::OpenOptions, io, io::Seek, path::Path};
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

    /// Creates the snapshot image path from its name.
    ///
    /// If `write` is `true`, it also ensures that the directory exists.
    fn image_path(&self, name: &str, write: bool) -> Result<String, Status> {
        let path = format!("{}/images/{}", self.root, &name_to_hash(name));
        if write {
            if let Some(parent) = Path::new(&path).parent() {
                fs::create_dir_all(parent)?;
            }
        }
        Ok(path)
    }

    /// Creates the snapshot socket path from its name.
    fn socket_path(&self, name: &str) -> Result<String, Status> {
        let path = format!("{}/sockets/{}", self.root, &name_to_hash(name));
        if let Some(parent) = Path::new(&path).parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(path)
    }

    /// Creates the snapshot file path from its name.
    ///
    /// If `write` is `true`, it also ensures that the directory exists.
    fn snapshot_path(&self, name: &str, write: bool) -> Result<String, Status> {
        let path = format!("{}/snapshots/{}", self.root, &name_to_hash(name));
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

    /// Creates a merged image with two partitions.
    ///
    /// The first one contains all layers followed by an index, while the second one is empty.
    fn create_image(&self, files: &[String], path: &str) -> Result<(), Status> {
        let mut layers = Vec::new();
        for name in files {
            let f = fs::File::open(&format!("{}/layers/{}", self.root, name))?;
            layers.push(io::BufReader::new(f));
        }
        // TODO: BufWriter?
        let mut out = fs::File::create(path)?;

        // Leave room for the partition table at the front.
        let prefix_size_in_blocks = part::table_size_in_blocks(2)? + 1;
        out.seek(io::SeekFrom::Start(prefix_size_in_blocks * part::BLOCK_SIZE))?;

        // Write all the layers out to the merged image.
        for layer in &mut layers {
            io::copy(layer, &mut out)?;
            layer.rewind()?;
        }

        // TODO: Align to 512?
        tarindex::build_index(&mut layers, &mut out)?;

        let image_size = out.stream_position()? - prefix_size_in_blocks * part::BLOCK_SIZE;
        let image_size_in_blocks = (image_size + part::BLOCK_SIZE - 1) / part::BLOCK_SIZE;

        part::write_table(
            &mut out,
            &[
                part::Descriptor {
                    start_lba: prefix_size_in_blocks,
                    size_in_blocks: image_size_in_blocks,
                },
                part::Descriptor {
                    start_lba: prefix_size_in_blocks + image_size_in_blocks,
                    size_in_blocks: 10 * 1024 * 1024 / part::BLOCK_SIZE,
                },
            ],
        )?;

        Ok(())
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

        parents.reverse();
        println!("Parents are: {:?}", parents);

        let image_path = self.image_path(&key, true)?;
        self.create_image(&parents, &image_path)?;
        // TODO: Use scope guard to delete image if anything goes wrong.

        println!("Image was created");

        let socket = self.socket_path(&key)?;

        let ret = crate::block::start_block_backend(&crate::block::VhostUserBlkBackendConfig {
            path: &image_path,
            socket: &socket,
            num_queues: None,
            queue_size: None,
            readonly: None,
            direct: None,
            poll_queue: None,
        });
        if let Err(e) = ret {
            println!("start_block_backend failed: {:?}", e);
            return Err(e.into());
        }

        println!("Block backend is ready at {}", &socket);

        let mounts = self.mounts_from_socket(socket)?;

        // Write the new snapshot.
        self.write_snapshot(kind, key, parent, labels)?;

        Ok(mounts)
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

    fn mounts_from_socket(&self, socket_path: String) -> Result<Vec<api::types::Mount>, Status> {
        Ok(vec![
           api::types::Mount {
               // TODO: Rename this.
               r#type: "gz-overlay".to_string(),
               source: fs::canonicalize(&socket_path)?.to_string_lossy().into(),
               target: String::new(),
               options: Vec::new(),
           },
        ])
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
        println!("Mounts: {}", key);
        self.mounts_from_socket(self.socket_path(&key)?)
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

    type InfoStream = impl tokio_stream::Stream<Item = Result<Info, Self::Error>> + Send + 'static;
    fn walk(&self) -> Result<Self::InfoStream, Self::Error> {
        let snapshots_dir = format!("{}/snapshots/", self.root);
        Ok(async_stream::try_stream! {
            let mut files = tokio::fs::read_dir(snapshots_dir).await?;
            while let Some(p) = files.next_entry().await? {
                if let Ok(f) = fs::File::open(p.path()) {
                    if let Ok(i) = serde_json::from_reader(f) {
                        yield i;
                    }
                }
            }
        })
    }
}

/// Converts the given name to a string representation of its sha256 hash.
fn name_to_hash(name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name);
    let mut res = String::new();
    for b in hasher.finalize() {
        res += &format!("{:02x}", b);
    }
    res
}
