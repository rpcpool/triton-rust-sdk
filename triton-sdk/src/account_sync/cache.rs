use std::collections::HashMap;

use solana_account::Account;
use solana_pubkey::Pubkey;

#[derive(Clone, Debug)]
pub(crate) struct CachedAccount {
    pub account: Option<Account>,
    pub slot: u64,
    pub version: u64,
    pub generation: u64,
    pub stream: bool,
}

#[derive(Default)]
pub(super) struct Cache {
    entries: HashMap<Pubkey, CachedAccount>,
    generations: HashMap<Pubkey, u64>,
    next_generation: u64,
}

impl Cache {
    pub fn generation(&mut self, key: Pubkey) -> u64 {
        *self.generations.entry(key).or_insert_with(|| {
            self.next_generation += 1;
            self.next_generation
        })
    }

    pub fn remove(&mut self, key: &Pubkey) {
        self.generations.remove(key);
        self.entries.remove(key);
    }

    pub fn get(&self, key: &Pubkey) -> Option<&CachedAccount> {
        self.entries.get(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &Pubkey> {
        self.generations.keys()
    }

    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }

    pub fn write(&mut self, key: Pubkey, entry: CachedAccount) -> bool {
        if self.generations.get(&key) != Some(&entry.generation) {
            return false;
        }
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

    fn entry(slot: u64, version: u64, generation: u64, stream: bool) -> CachedAccount {
        CachedAccount {
            account: None,
            slot,
            version,
            generation,
            stream,
        }
    }

    #[test]
    fn rejects_old_order_and_generation() {
        let mut cache = Cache::default();
        let key = Pubkey::new_unique();
        let first = cache.generation(key);
        assert!(cache.write(key, entry(10, 2, first, true)));
        assert!(!cache.write(key, entry(10, 1, first, true)));
        assert!(!cache.write(key, entry(9, 4, first, true)));
        assert!(!cache.write(key, entry(10, 2, first, true)));
        cache.remove(&key);
        let second = cache.generation(key);
        assert_ne!(first, second);
        assert!(!cache.write(key, entry(11, 0, first, false)));
        assert!(cache.write(key, entry(11, 0, second, false)));
        assert!(cache.get(&key).is_some_and(|entry| entry.account.is_none()));
    }
}
