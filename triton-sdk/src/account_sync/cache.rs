use std::collections::HashMap;

use solana_account::Account;
use solana_pubkey::Pubkey;

#[derive(Clone, Debug)]
pub(crate) struct CachedAccount {
    pub account: Option<Account>,
    pub slot: u64,
    pub version: u64,
    pub stream: bool,
}

#[derive(Default)]
pub(super) struct Cache {
    entries: HashMap<Pubkey, CachedAccount>,
}

impl Cache {
    pub fn remove(&mut self, key: &Pubkey) {
        self.entries.remove(key);
    }

    pub fn get(&self, key: &Pubkey) -> Option<&CachedAccount> {
        self.entries.get(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &Pubkey> {
        self.entries.keys()
    }

    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }

    pub fn write(&mut self, key: Pubkey, entry: CachedAccount) -> bool {
        if self
            .entries
            .get(&key)
            .is_some_and(|old| !is_newer(&entry, old))
        {
            return false;
        }
        self.entries.insert(key, entry);
        true
    }
}

fn is_newer(new: &CachedAccount, old: &CachedAccount) -> bool {
    if new.slot != old.slot {
        return new.slot > old.slot;
    }
    if new.stream && old.stream {
        return new.version > old.version;
    }
    new.stream && !old.stream
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(slot: u64, version: u64, stream: bool) -> CachedAccount {
        CachedAccount {
            account: None,
            slot,
            version,
            stream,
        }
    }

    #[test]
    fn orders_by_slot_then_stream_source_then_write_version() {
        let mut cache = Cache::default();
        let key = Pubkey::new_unique();
        assert!(cache.write(key, entry(10, 0, false)));
        assert!(!cache.write(key, entry(10, 0, false)));
        assert!(cache.write(key, entry(10, 2, true)));
        assert!(!cache.write(key, entry(10, 1, true)));
        assert!(!cache.write(key, entry(9, 4, true)));
        assert!(!cache.write(key, entry(10, 2, true)));
        assert!(!cache.write(key, entry(10, 0, false)));
        assert!(cache.write(key, entry(10, 3, true)));
        assert!(cache.write(key, entry(11, 0, false)));
    }

    #[test]
    fn missing_account_is_a_hit_until_removed() {
        let mut cache = Cache::default();
        let key = Pubkey::new_unique();
        assert!(cache.get(&key).is_none());
        assert!(cache.write(key, entry(11, 0, false)));
        assert!(cache.get(&key).is_some_and(|entry| entry.account.is_none()));
        cache.remove(&key);
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.keys().count(), 0);
    }
}
