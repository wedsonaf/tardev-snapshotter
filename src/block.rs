// Copyright 2019 Red Hat, Inc. All Rights Reserved.
//
// Portions Copyright 2019 Intel Corporation. All Rights Reserved.
//
// Portions Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Portions Copyright 2017 The Chromium OS Authors. All rights reserved.
//
// SPDX-License-Identifier: (Apache-2.0 AND BSD-3-Clause)

#![allow(clippy::significant_drop_in_scrutinee)]

use block_util::{build_disk_image_id, Request, VirtioBlockConfig};
use libc::EFD_NONBLOCK;
use log::*;
use qcow::{self, ImageType, QcowFile};
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use vhost::vhost_user::message::*;
use vhost::vhost_user::Listener;
use vhost_user_backend::{VhostUserBackendMut, VhostUserDaemon, VringRwLock, VringState, VringT};
use virtio_bindings::bindings::virtio_blk::*;
use virtio_bindings::bindings::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use virtio_queue::QueueT;
use vm_memory::{bitmap::AtomicBitmap, ByteValued, Bytes, GuestAddressSpace, GuestMemoryAtomic};
use vmm_sys_util::{epoll::EventSet, eventfd::EventFd};

type GuestMemoryMmap = vm_memory::GuestMemoryMmap<AtomicBitmap>;

const SECTOR_SHIFT: u8 = 9;
const SECTOR_SIZE: u64 = 0x01 << SECTOR_SHIFT;
const BLK_SIZE: u32 = 512;
// Current (2020) enterprise SSDs have a latency lower than 30us.
// Polling for 50us should be enough to cover for the device latency
// and the overhead of the emulation layer.
const POLL_QUEUE_US: u128 = 50;

trait DiskFile: Read + Seek + Write + Send {}
impl<D: Read + Seek + Write + Send> DiskFile for D {}

type Result<T = (), E = io::Error> = std::result::Result<T, E>;

struct Thread {
    disk_image: Arc<Mutex<dyn DiskFile>>,
    disk_image_id: Vec<u8>,
    disk_nsectors: u64,
    event_idx: bool,
    kill_evt: EventFd,
    writeback: Arc<AtomicBool>,
    mem: GuestMemoryAtomic<GuestMemoryMmap>,
}

impl Thread {
    fn new(
        disk_image: Arc<Mutex<dyn DiskFile>>,
        disk_image_id: Vec<u8>,
        disk_nsectors: u64,
        writeback: Arc<AtomicBool>,
        mem: GuestMemoryAtomic<GuestMemoryMmap>,
    ) -> Result<Self> {
        Ok(Self {
            disk_image,
            disk_image_id,
            disk_nsectors,
            event_idx: false,
            kill_evt: EventFd::new(EFD_NONBLOCK)?,
            writeback,
            mem,
        })
    }

    fn process_queue(
        &mut self,
        vring: &mut VringState<GuestMemoryAtomic<GuestMemoryMmap>>,
    ) -> Result<bool> {
        let mut used_descs = false;

        while let Some(mut desc_chain) = vring
            .get_queue_mut()
            .pop_descriptor_chain(self.mem.memory())
        {
            debug!("got an element in the queue");
            let len;
            match Request::parse(&mut desc_chain, None) {
                Ok(mut request) => {
                    debug!("element is a valid request");
                    request.set_writeback(self.writeback.load(Ordering::Acquire));
                    let status = match request.execute(
                        &mut self.disk_image.lock().unwrap().deref_mut(),
                        self.disk_nsectors,
                        desc_chain.memory(),
                        &self.disk_image_id,
                    ) {
                        Ok(l) => {
                            len = l;
                            VIRTIO_BLK_S_OK
                        }
                        Err(e) => {
                            len = 1;
                            e.status()
                        }
                    };
                    desc_chain
                        .memory()
                        .write_obj(status, request.status_addr)
                        .map_err(to_other)?;
                }
                Err(err) => {
                    error!("failed to parse available descriptor chain: {:?}", err);
                    len = 0;
                }
            }

            vring
                .get_queue_mut()
                .add_used(desc_chain.memory(), desc_chain.head_index(), len)
                .map_err(to_other)?;
            used_descs = true;
        }

        let needs_signalling = if self.event_idx {
            vring
                .get_queue_mut()
                .needs_notification(self.mem.memory().deref())
                .map_err(to_other)?
        } else {
            debug!("signalling queue");
            true
        };

        if needs_signalling {
            let _ = vring.signal_used_queue();
        }

        Ok(used_descs)
    }
}

struct Backend {
    threads: Vec<Mutex<Thread>>,
    config: VirtioBlockConfig,
    rdonly: bool,
    poll_queue: bool,
    queues_per_thread: Vec<u64>,
    queue_size: usize,
    acked_features: u64,
    writeback: Arc<AtomicBool>,
    mem: GuestMemoryAtomic<GuestMemoryMmap>,
}

impl Backend {
    fn new(
        image_path: &str,
        num_queues: usize,
        rdonly: bool,
        direct: bool,
        poll_queue: bool,
        queue_size: usize,
        mem: GuestMemoryAtomic<GuestMemoryMmap>,
    ) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true);
        options.write(!rdonly);
        if direct {
            options.custom_flags(libc::O_DIRECT);
        }
        let image = options.open(image_path)?;
        let mut raw_img = qcow::RawFile::new(image, direct);

        // TODO: image_id should be the hash of the compressed layer.
        let image_id = build_disk_image_id(&PathBuf::from(image_path));
        let image_type = qcow::detect_image_type(&mut raw_img).map_err(to_other)?;
        let image: Arc<Mutex<dyn DiskFile>> = match image_type {
            ImageType::Raw => Arc::new(Mutex::new(raw_img)),
            ImageType::Qcow2 => Arc::new(Mutex::new(QcowFile::from(raw_img).map_err(to_other)?)),
        };

        let nsectors = (image.lock().unwrap().seek(SeekFrom::End(0))?) / SECTOR_SIZE;
        let config = VirtioBlockConfig {
            capacity: nsectors,
            blk_size: BLK_SIZE,
            size_max: 65535,
            seg_max: 128 - 2,
            min_io_size: 1,
            opt_io_size: 1,
            num_queues: num_queues as u16,
            writeback: 1,
            ..Default::default()
        };

        let mut queues_per_thread = Vec::new();
        let mut threads = Vec::new();
        let writeback = Arc::new(AtomicBool::new(true));
        for i in 0..num_queues {
            let thread = Mutex::new(Thread::new(
                image.clone(),
                image_id.clone(),
                nsectors,
                writeback.clone(),
                mem.clone(),
            )?);
            threads.push(thread);
            queues_per_thread.push(0b1 << i);
        }

        Ok(Self {
            threads,
            config,
            rdonly,
            poll_queue,
            queues_per_thread,
            queue_size,
            acked_features: 0,
            writeback,
            mem,
        })
    }

    fn update_writeback(&mut self) {
        // Use writeback from config if VIRTIO_BLK_F_CONFIG_WCE
        let writeback = if self.acked_features & 1 << VIRTIO_BLK_F_CONFIG_WCE != 0 {
            self.config.writeback == 1
        } else {
            // Else check if VIRTIO_BLK_F_FLUSH negotiated
            self.acked_features & 1 << VIRTIO_BLK_F_FLUSH != 0
        };

        info!(
            "Changing cache mode to {}",
            if writeback {
                "writeback"
            } else {
                "writethrough"
            }
        );
        self.writeback.store(writeback, Ordering::Release);
    }
}

impl VhostUserBackendMut<VringRwLock<GuestMemoryAtomic<GuestMemoryMmap>>, AtomicBitmap>
    for Backend
{
    fn num_queues(&self) -> usize {
        self.config.num_queues as usize
    }

    fn max_queue_size(&self) -> usize {
        self.queue_size
    }

    fn features(&self) -> u64 {
        1 << VIRTIO_BLK_F_SEG_MAX
            | 1 << VIRTIO_BLK_F_BLK_SIZE
            | 1 << VIRTIO_BLK_F_FLUSH
            | 1 << VIRTIO_BLK_F_TOPOLOGY
            | 1 << VIRTIO_BLK_F_MQ
            | 1 << VIRTIO_BLK_F_CONFIG_WCE
            | 1 << VIRTIO_RING_F_EVENT_IDX
            | 1 << VIRTIO_F_VERSION_1
            | VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits()
            | if self.rdonly { 1 << VIRTIO_BLK_F_RO } else { 0 }
    }

    fn acked_features(&mut self, features: u64) {
        self.acked_features = features;
        self.update_writeback();
    }

    fn protocol_features(&self) -> VhostUserProtocolFeatures {
        VhostUserProtocolFeatures::CONFIG
            | VhostUserProtocolFeatures::MQ
            | VhostUserProtocolFeatures::CONFIGURE_MEM_SLOTS
    }

    fn set_event_idx(&mut self, enabled: bool) {
        for thread in self.threads.iter() {
            thread.lock().unwrap().event_idx = enabled;
        }
    }

    fn handle_event(
        &mut self,
        device_event: u16,
        evset: EventSet,
        vrings: &[VringRwLock<GuestMemoryAtomic<GuestMemoryMmap>>],
        thread_id: usize,
    ) -> Result<bool> {
        if evset != EventSet::IN {
            return Err(other("not EPOLLIN"));
        }

        debug!("event received: {:?}", device_event);

        let mut thread = self.threads[thread_id].lock().unwrap();
        if device_event != 0 {
            return Err(other("unknown event"));
        }

        let mut vring = vrings[0].get_mut();

        if self.poll_queue {
            // Actively poll the queue until POLL_QUEUE_US has passed
            // without seeing a new request.
            let mut now = Instant::now();
            loop {
                if thread.process_queue(&mut vring)? {
                    now = Instant::now();
                } else if now.elapsed().as_micros() > POLL_QUEUE_US {
                    break;
                }
            }
        }

        if thread.event_idx {
            // vm-virtio's Queue implementation only checks avail_index
            // once, so to properly support EVENT_IDX we need to keep
            // calling process_queue() until it stops finding new
            // requests on the queue.
            loop {
                vring
                    .get_queue_mut()
                    .enable_notification(self.mem.memory().deref())
                    .map_err(to_other)?;
                if !thread.process_queue(&mut vring)? {
                    break;
                }
            }
        } else {
            // Without EVENT_IDX, a single call is enough.
            thread.process_queue(&mut vring)?;
        }

        Ok(false)
    }

    fn get_config(&self, _offset: u32, _size: u32) -> Vec<u8> {
        self.config.as_slice().to_vec()
    }

    fn set_config(&mut self, offset: u32, data: &[u8]) -> Result {
        let config_slice = self.config.as_mut_slice();
        let data_len = data.len() as u32;
        let config_len = config_slice.len() as u32;
        if offset + data_len > config_len {
            error!("Failed to write config space");
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        let (_, right) = config_slice.split_at_mut(offset as usize);
        right.copy_from_slice(data);
        self.update_writeback();
        Ok(())
    }

    fn exit_event(&self, thread_index: usize) -> Option<EventFd> {
        Some(
            self.threads[thread_index]
                .lock()
                .unwrap()
                .kill_evt
                .try_clone()
                .unwrap(),
        )
    }

    fn queues_per_thread(&self) -> Vec<u64> {
        self.queues_per_thread.clone()
    }

    fn update_memory(&mut self, _mem: GuestMemoryAtomic<GuestMemoryMmap>) -> Result {
        Ok(())
    }
}

pub struct VhostUserBlkBackendConfig<'a> {
    pub path: &'a str,
    pub socket: &'a str,
    pub num_queues: Option<usize>,
    pub queue_size: Option<usize>,
    pub readonly: Option<bool>,
    pub direct: Option<bool>,
    pub poll_queue: Option<bool>,
}

fn other(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn to_other(err: impl std::fmt::Display) -> io::Error {
    other(&format!("{}", err))
}

pub fn start_block_backend(backend_config: &VhostUserBlkBackendConfig) -> Result {
    let mem = GuestMemoryAtomic::new(GuestMemoryMmap::new());
    let blk_backend = Arc::new(RwLock::new(Backend::new(
        backend_config.path,
        backend_config.num_queues.unwrap_or(1),
        backend_config.readonly.unwrap_or(false),
        backend_config.direct.unwrap_or(false),
        backend_config.poll_queue.unwrap_or(true),
        backend_config.queue_size.unwrap_or(1024),
        mem.clone(),
    )?));

    println!("blk_backend is created!\n");

    let listener = Listener::new(backend_config.socket, true).map_err(to_other)?;

    let name = "vhost-user-blk-backend";
    let mut blk_daemon =
        VhostUserDaemon::new(name.to_string(), blk_backend.clone(), mem).map_err(to_other)?;

    println!("blk_daemon is created!\n");

    std::thread::spawn(move || {
        // TODO: We need to be able to be informed when the snapshot is deleted so that we can
        // cleanup in the cases when the client never connects.
        if let Err(e) = blk_daemon.start(listener).map_err(to_other) {
            println!("failed to accept: {:?}", e);
        }

        let _ = blk_daemon.wait();

        println!("Disconnected");

        for thread in blk_backend.read().unwrap().threads.iter() {
            if let Err(e) = thread.lock().unwrap().kill_evt.write(1) {
                error!("Error shutting down worker thread: {:?}", e)
            }
        }
    });

    Ok(())
}
