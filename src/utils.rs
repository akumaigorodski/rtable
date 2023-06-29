use alloc::vec::Vec;

pub trait SeqExt<T: PartialEq> {
    fn has_value(&self, to_check: &T) -> bool;
    fn index_of(&self, to_check: &T) -> Option<usize>;
}

impl<T: PartialEq> SeqExt<T> for Vec<T> {
    fn has_value(&self, value_to_check: &T) -> bool {
        self.index_of(value_to_check).is_some()
    }

    fn index_of(&self, value_to_check: &T) -> Option<usize> {
        self.iter().position(|value| value == value_to_check)
    }
}
