use alloc::rc::Rc;
use alloc::vec::Vec;
use core::hash::Hash;

use hashbrown::HashSet;

use crate::utils::SeqExt;

#[derive(Clone, Debug)]
pub struct HashVec<T: Eq + Hash> {
    pub hash: HashSet<Rc<T>>,
    pub vector: Vec<Rc<T>>,
    pub length: usize,
}

impl<T: Eq + Hash> HashVec<T> {
    pub fn new() -> Self {
        Self {
            hash: HashSet::new(),
            vector: Vec::new(),
            length: 0,
        }
    }

    pub fn remove(&mut self, rc: &Rc<T>) {
        if let Some(pos) = self.vector.index_of(rc) {
            self.vector.remove(pos);
            self.hash.remove(rc);
            self.length -= 1;
        }
    }

    pub fn add(&mut self, rc_value: Rc<T>) {
        let cloned_value = Rc::clone(&rc_value);
        self.hash.insert(cloned_value);
        self.vector.push(rc_value);
        self.length += 1;
    }

    pub fn add_if_not_exists(&mut self, value: T) {
        if !self.hash.contains(&value) {
            let rc = Rc::new(value);
            self.add(rc);
        }
    }

    pub fn merge_with_hashvec(&mut self, other_hashvec: &HashVec<T>) {
        for value in &other_hashvec.vector {
            if !self.hash.contains(value) {
                let rc = Rc::clone(value);
                self.add(rc);
            }
        }
    }

    pub fn from_others(items: Vec<&HashVec<T>>) -> HashVec<T> {
        items.into_iter().fold(HashVec::new(), |mut out, other| {
            out.merge_with_hashvec(other);
            out
        })
    }
}

#[macro_export]
macro_rules! hashvec {
    ($($x:expr),*) => {{
        let mut temp_vec = HashVec::new();
        $(temp_vec.add(Rc::new($x));)*
        temp_vec
    }};
}