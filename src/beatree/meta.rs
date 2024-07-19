/// This data structure describes the state of the btree.
pub struct Meta {
    /// The sequence number of the last sync.
    /// 
    /// The sync sequence number is incremented by 1 for every sync. The very first sync will have
    /// the sequence number of 1.
    pub sync_seqn: u32,
    /// The next allocated BBN will have this sequence number.
    /// 
    /// The next allocated BBN will have this sequence number. The very first BBN will have the
    /// sequence number of 1.
    pub next_bbn_seqn: u32,
    /// The page number of the head of the freelist of the leaf storage file. 0 means the freelist
    /// is empty.
    pub ln_freelist_pn: u32,
    /// The next page available for allocation in the LN storage file. 
    /// 
    /// Since the first page is reserved, this is always more than 1.
    pub ln_bump: u32,
    /// The next page available for allocation in the LN storage file.
    /// 
    /// Since the first page is reserved, this is always more than 1.
    pub bbn_bump: u32,
}

impl Meta {
    pub fn encode_to(&self, buf: &mut [u8; 20]) {
        buf[..4].copy_from_slice(&self.sync_seqn.to_le_bytes());
        buf[4..8].copy_from_slice(&self.next_bbn_seqn.to_le_bytes());
        buf[8..12].copy_from_slice(&self.ln_freelist_pn.to_le_bytes());
        buf[12..16].copy_from_slice(&self.ln_bump.to_le_bytes());
        buf[16..20].copy_from_slice(&self.bbn_bump.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Self {
        let sync_seqn = u32::from_le_bytes(buf[..4].try_into().unwrap());
        let next_bbn_seqn = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let ln_freelist_pn = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let ln_bump = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let bbn_bump = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        Self {
            sync_seqn,
            next_bbn_seqn,
            ln_freelist_pn,
            ln_bump,
            bbn_bump,
        }
    }
}
