use std::{io, mem};
use zerocopy::byteorder::{LE, U16, U32, U64};
use zerocopy::AsBytes;

/// The size of a block for the partition table.
pub const BLOCK_SIZE: u64 = 512;

#[derive(zerocopy::AsBytes, zerocopy::Unaligned)]
#[repr(C)]
struct Mbr {
    bootcode: [u8; 440],
    disk_signature: U32<LE>,
    unknown: U16<LE>,
    partitions: [MbrEntry; 4],
    signature: [u8; 2],
}

#[derive(Default, zerocopy::AsBytes, zerocopy::Unaligned)]
#[repr(C)]
struct MbrEntry {
    boot_indicator: u8, /* unused by EFI, set to 0x80 for bootable */
    start_head: u8,     /* unused by EFI, pt start in CHS */
    start_sector: u8,   /* unused by EFI, pt start in CHS */
    start_track: u8,
    os_type: u8,       /* EFI and legacy non-EFI OS types */
    end_head: u8,      /* unused by EFI, pt end in CHS */
    end_sector: u8,    /* unused by EFI, pt end in CHS */
    end_track: u8,     /* unused by EFI, pt end in CHS */
    starting_lba: U32<LE>, /* used by EFI - start addr of the on disk pt */
    size_in_lba: U32<LE>,  /* used by EFI - size of pt in LBA */
}

const GPT_HEADER_SIGNATURE: u64 = 0x5452415020494645;
const GPT_HEADER_REVISION_V1: u32 = 0x00010000;

#[derive(Default, zerocopy::AsBytes, zerocopy::Unaligned)]
#[repr(C)]
struct GptHeader {
    signature: U64<LE>,
    revision: U32<LE>,
    header_size: U32<LE>,
    header_crc32: U32<LE>,
    reserved1: U32<LE>,
    my_lba: U64<LE>,
    alternate_lba: U64<LE>,
    first_usable_lba: U64<LE>,
    last_usable_lba: U64<LE>,
    disk_guid: [u8; 16],
    partition_entry_lba: U64<LE>,
    num_partition_entries: U32<LE>,
    sizeof_partition_entry: U32<LE>,
    partition_entry_array_crc32: U32<LE>,
}

#[derive(zerocopy::AsBytes, zerocopy::Unaligned)]
#[repr(C)]
struct GptEntry {
    partition_type_guid: [u8; 16],
    unique_partition_guid: [u8; 16],
    starting_lba: U64<LE>,
    ending_lba: U64<LE>,
    attributes: U64<LE>,
    partition_name: [U16<LE>; 36],
}

fn calculate_crc32(data: &[u8]) -> u32 {
    let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
    let mut digest = crc.digest();
    digest.update(data);
    digest.finalize()
}

pub struct Descriptor {
    pub start_lba: u64,
    pub size_in_blocks: u64,
}

/// Finds the begin and end LBAs for the data in the given descriptors.
fn find_data_lbas(parts: &[Descriptor]) -> io::Result<(u64, u64)> {
    let mut end_lba = 0u64;
    let mut begin_lba = u64::MAX;

    for (i, part) in parts.iter().enumerate() {
        let end = part
            .start_lba
            .checked_add(part.size_in_blocks)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("overflow in calculating end of partition {i}"),
                )
            })?;
        end_lba = std::cmp::max(end, end_lba);
        begin_lba = std::cmp::min(part.start_lba, begin_lba);
    }

    Ok((begin_lba, end_lba))
}

const ENTRY_SIZE: u64 = mem::size_of::<GptEntry>() as u64;

/// Calculates the size of the table, given the number of partitions.
pub fn table_size_in_blocks(part_count: usize) -> io::Result<u64> {
    fn option_based(part_count: usize) -> Option<u64> {
        Some(
            ENTRY_SIZE
                .checked_mul(part_count as u64)?
                .checked_add(BLOCK_SIZE - 1)?
                / BLOCK_SIZE
                + 1,
        )
    }
    option_based(part_count).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "overflow calculating table size",
        )
    })
}

/// Writes the master boot record (MBR) to the given output stream.
fn write_mbr(output: &mut (impl io::Write + io::Seek), size: u64) -> io::Result<()> {
    output.seek(io::SeekFrom::Start(0))?;

    let mbr = Mbr {
        bootcode: [0; 440],
        disk_signature: 0.into(),
        unknown: 0.into(),
        partitions: [
            MbrEntry {
                start_sector: 2,
                os_type: 0xee,
                end_head: 0,
                end_sector: 0,
                end_track: 0,
                starting_lba: 1u32.into(),
                size_in_lba: u32::try_from(size).unwrap_or(u32::MAX).into(),
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            Default::default(),
        ],
        signature: [0x55, 0xaa],
    };

    output.write_all(mbr.as_bytes())
}

pub fn write_table(
    output: &mut (impl io::Write + io::Seek),
    parts: &[Descriptor],
) -> io::Result<()> {
    let (begin_data_lba, end_data_lba) = find_data_lbas(parts)?;
    let table_size_in_blocks = table_size_in_blocks(parts.len())?;
    let total_size_in_blocks = 2 * table_size_in_blocks + end_data_lba;

    // TODO: Check that the table doesn't overlap the first lba.
    write_mbr(output, total_size_in_blocks)?;

    // Create the entries.
    let mut entries = Vec::new();
    for p in parts {
        entries.push(GptEntry {
            partition_type_guid: [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            unique_partition_guid: [0; 16],
            starting_lba: p.start_lba.into(),
            ending_lba: (p.start_lba + p.size_in_blocks - 1).into(),
            attributes: 0.into(),
            partition_name: [0.into(); 36],
        });
    }

    // Create the header.
    let mut header = GptHeader {
        signature: GPT_HEADER_SIGNATURE.into(),
        revision: GPT_HEADER_REVISION_V1.into(),
        header_size: (mem::size_of::<GptHeader>() as u32).into(),
        my_lba: 1u64.into(),
        alternate_lba: (1 + total_size_in_blocks - 1).into(),
        first_usable_lba: begin_data_lba.into(),
        last_usable_lba: (end_data_lba - 1).into(),
        partition_entry_lba: 2u64.into(),
        num_partition_entries: (entries.len() as u32).into(),
        sizeof_partition_entry: (mem::size_of::<GptEntry>() as u32).into(),
        ..Default::default()
    };

    header.partition_entry_array_crc32 = calculate_crc32(entries.as_bytes()).into();
    header.header_crc32 = calculate_crc32(header.as_bytes()).into();

    // Write the GPT header.
    output.seek(io::SeekFrom::Start(BLOCK_SIZE))?;
    output.write_all(header.as_bytes())?;

    // Write entries.
    output.seek(io::SeekFrom::Start(2 * BLOCK_SIZE))?;
    output.write_all(entries.as_bytes())?;

    // Write alternate LBA.
    output.seek(io::SeekFrom::Start(end_data_lba * BLOCK_SIZE))?;
    output.write_all(entries.as_bytes())?;

    header.header_crc32 = 0.into();
    header.my_lba = header.alternate_lba;
    header.alternate_lba = 1u64.into();
    header.partition_entry_lba = end_data_lba.into();
    header.header_crc32 = calculate_crc32(header.as_bytes()).into();
    output.seek(io::SeekFrom::Start(
        (1 + total_size_in_blocks - 1) * BLOCK_SIZE,
    ))?;
    output.write_all(header.as_bytes())?;

    // Write last byte to get the right size.
    output.seek(io::SeekFrom::Start(
        (1 + total_size_in_blocks - 1) * BLOCK_SIZE - 1,
    ))?;
    output.write_all(&[0u8; 1])?;
    Ok(())
}
